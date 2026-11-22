package com.example.matching.service;

import akka.actor.typed.ActorSystem;
import com.example.matching.actor.EngineRootActor;
import com.example.matching.controller.OrderController;
import com.example.matching.grpc.OrderGrpcService;
import com.example.matching.persistence.OrderEventStore;
import com.example.order.grpc.OrderServiceGrpc;
import com.example.order.grpc.PlaceOrderRequest;
import com.example.order.grpc.PlaceOrderResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;

class OrderIngressIdempotencyTest {
    @TempDir Path data;

    @Test void queryBookSupportsSlashedSymbolAndRejectsInvalidSymbol() throws Exception {
        ActorSystem<EngineRootActor.Command> root = ActorSystem.create(
                EngineRootActor.create(true, data, 1000, 64), "ingress-book-query");
        try {
            MockMvc mvc = MockMvcBuilders.standaloneSetup(
                    new OrderController(new OrderApplicationService(root))).build();
            MvcResult started = mvc.perform(get("/orders/book").queryParam("symbol", "BTC/USDT"))
                    .andExpect(request().asyncStarted()).andReturn();
            MockHttpServletResponse response = mvc.perform(asyncDispatch(started)).andReturn().getResponse();
            assertEquals(200, response.getStatus());
            assertTrue(response.getContentAsString().contains("SELL BOOK"));

            MvcResult invalid = mvc.perform(get("/orders/book").queryParam("symbol", "../bad"))
                    .andExpect(request().asyncStarted()).andReturn();
            assertEquals(400, mvc.perform(asyncDispatch(invalid)).andReturn().getResponse().getStatus());
            assertEquals(0, new OrderEventStore(data, "BTC/USDT").loadRecoveryState().lastSequence());
        } finally {
            root.terminate();
            root.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test void httpAndGrpcCarryClientOrderIdAndReportConflicts() throws Exception {
        ActorSystem<EngineRootActor.Command> root = ActorSystem.create(
                EngineRootActor.create(true, data, 1000, 64), "ingress-idempotency");
        try {
            OrderApplicationService service = new OrderApplicationService(root);
            MockMvc mvc = MockMvcBuilders.standaloneSetup(new OrderController(service)).build();
            String http = "{\"userId\":\"http-user\",\"clientOrderId\":\"http-key\",\"symbol\":\"BTCUSDT\",\"side\":\"BUY\",\"price\":10000,\"quantity\":5}";
            MockHttpServletResponse original = httpPlace(mvc, http);
            MockHttpServletResponse repeated = httpPlace(mvc, http);
            assertEquals(200, original.getStatus());
            assertEquals(200, repeated.getStatus());
            String originalId = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readTree(original.getContentAsString()).at("/data/finalOrder/orderId").asText();
            assertEquals(originalId, new com.fasterxml.jackson.databind.ObjectMapper()
                    .readTree(repeated.getContentAsString()).at("/data/finalOrder/orderId").asText());
            assertEquals(409, httpPlace(mvc, http.replace("\"price\":10000", "\"price\":10100")).getStatus());
            assertEquals(400, httpPlace(mvc, http.replace("\"quantity\":5", "\"quantity\":0")).getStatus());
            assertEquals(400, httpPlace(mvc, http.replace("\"clientOrderId\":\"http-key\",", "")).getStatus());
            assertEquals(400, httpPlace(mvc, http.replace("\"symbol\":\"BTCUSDT\"", "\"symbol\":\"bad symbol\"")).getStatus());
            MvcResult invalidCancel = mvc.perform(post("/orders/cancel")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content("{\"symbol\":\"BTCUSDT\",\"orderId\":\"\"}"))
                    .andExpect(request().asyncStarted()).andReturn();
            assertEquals(400, mvc.perform(asyncDispatch(invalidCancel)).andReturn().getResponse().getStatus());

            OrderGrpcService grpc = new OrderGrpcService(service);
            PlaceOrderRequest grpcRequest = PlaceOrderRequest.newBuilder().setUserId(7).setClientOrderId("grpc-key")
                    .setSymbol("BTCUSDT").setSide(com.example.order.grpc.Side.SIDE_SELL)
                    .setPrice(11000).setQuantity(2).build();
            PlaceOrderResponse firstGrpc = grpcPlace(grpc, grpcRequest);
            PlaceOrderResponse retryGrpc = grpcPlace(grpc, grpcRequest);
            assertEquals(firstGrpc.getOrderId(), retryGrpc.getOrderId());
            assertTrue(retryGrpc.getMessage().startsWith("already accepted"));
            ExecutionException conflict = assertThrows(ExecutionException.class,
                    () -> grpcPlace(grpc, grpcRequest.toBuilder().setQuantity(3).build()));
            assertEquals(Status.Code.ALREADY_EXISTS,
                    Status.fromThrowable(conflict.getCause()).getCode());
            ExecutionException invalid = assertThrows(ExecutionException.class,
                    () -> grpcPlace(grpc, grpcRequest.toBuilder().setQuantity(0).build()));
            assertEquals(Status.Code.INVALID_ARGUMENT,
                    Status.fromThrowable(invalid.getCause()).getCode());
            ExecutionException missingKey = assertThrows(ExecutionException.class,
                    () -> grpcPlace(grpc, grpcRequest.toBuilder().clearClientOrderId().build()));
            assertEquals(Status.Code.INVALID_ARGUMENT,
                    Status.fromThrowable(missingKey.getCause()).getCode());
            var cancelFuture = new CompletableFuture<com.example.order.grpc.CancelOrderResponse>();
            grpc.cancelOrder(com.example.order.grpc.CancelOrderRequest.newBuilder()
                    .setSymbol("BTCUSDT").setOrderId("").build(), new StreamObserver<>() {
                        @Override public void onNext(com.example.order.grpc.CancelOrderResponse value) { cancelFuture.complete(value); }
                        @Override public void onError(Throwable error) { cancelFuture.completeExceptionally(error); }
                        @Override public void onCompleted() {}
                    });
            ExecutionException invalidCancelGrpc = assertThrows(ExecutionException.class,
                    () -> cancelFuture.get(5, TimeUnit.SECONDS));
            assertEquals(Status.Code.INVALID_ARGUMENT,
                    Status.fromThrowable(invalidCancelGrpc.getCause()).getCode());
            var bookFuture = new CompletableFuture<com.example.order.grpc.GetBookResponse>();
            grpc.getBook(com.example.order.grpc.GetBookRequest.newBuilder()
                    .setSymbol("bad symbol").build(), new StreamObserver<>() {
                        @Override public void onNext(com.example.order.grpc.GetBookResponse value) { bookFuture.complete(value); }
                        @Override public void onError(Throwable error) { bookFuture.completeExceptionally(error); }
                        @Override public void onCompleted() {}
                    });
            ExecutionException invalidBookGrpc = assertThrows(ExecutionException.class,
                    () -> bookFuture.get(5, TimeUnit.SECONDS));
            assertEquals(Status.Code.INVALID_ARGUMENT,
                    Status.fromThrowable(invalidBookGrpc.getCause()).getCode());
            assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        } finally {
            root.terminate();
            root.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private MockHttpServletResponse httpPlace(MockMvc mvc, String body) throws Exception {
        MvcResult started = mvc.perform(post("/orders/place").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(request().asyncStarted()).andReturn();
        return mvc.perform(asyncDispatch(started)).andReturn().getResponse();
    }

    private PlaceOrderResponse grpcPlace(OrderGrpcService grpc, PlaceOrderRequest request) throws Exception {
        CompletableFuture<PlaceOrderResponse> future = new CompletableFuture<>();
        grpc.placeOrder(request, new StreamObserver<>() {
            @Override public void onNext(PlaceOrderResponse value) { future.complete(value); }
            @Override public void onError(Throwable error) { future.completeExceptionally(error); }
            @Override public void onCompleted() {}
        });
        return future.get(5, TimeUnit.SECONDS);
    }
}
