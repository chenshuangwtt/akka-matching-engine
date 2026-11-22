package com.example.matching.grpc;

import com.example.matching.domain.Side;
import com.example.matching.engine.MatchResult;
import com.example.matching.service.OrderApplicationService;
import com.example.matching.service.OrderConflictException;
import com.example.matching.util.MdcUtil;
import com.example.order.grpc.CancelOrderRequest;
import com.example.order.grpc.CancelOrderResponse;
import com.example.order.grpc.GetBookRequest;
import com.example.order.grpc.GetBookResponse;
import com.example.order.grpc.OrderServiceGrpc;
import com.example.order.grpc.PlaceOrderRequest;
import com.example.order.grpc.PlaceOrderResponse;
import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase implements BindableService {

    private static final Logger log = LoggerFactory.getLogger(OrderGrpcService.class);

    private final OrderApplicationService orderApplicationService;

    public OrderGrpcService(OrderApplicationService orderApplicationService) {
        this.orderApplicationService = orderApplicationService;
    }

    // gRPC 下单直接桥接到现有 Actor 撮合链路。
    @Override
    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        String requestId = newRequestId();
        String clientOrderId = request.getClientOrderId();
        Side side;
        try {
            side = mapSide(request.getSide());
        } catch (IllegalArgumentException ex) {
            try (MdcUtil.Scope ignored = MdcUtil.with(requestId, clientOrderId)) {
                log.warn("gRPC placeOrder rejected: symbol={}, reason={}", request.getSymbol(), ex.getMessage());
            }
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(ex.getMessage()).asRuntimeException());
            return;
        }

        String userId = request.getUsername().isBlank()
                ? (request.getUserId() > 0 ? String.valueOf(request.getUserId()) : "")
                : request.getUsername();

        try (MdcUtil.Scope ignored = MdcUtil.with(requestId, clientOrderId)) {
            log.info(
                    "gRPC placeOrder received: userId={}, symbol={}, side={}, price={}, quantity={}",
                    userId,
                    request.getSymbol(),
                    side,
                    request.getPrice(),
                    request.getQuantity()
            );
        }

        orderApplicationService.placeOrder(
                        requestId,
                        clientOrderId,
                        userId,
                        request.getSymbol(),
                        side,
                        request.getPrice(),
                        request.getQuantity()
                )
                .whenComplete((result, throwable) -> {
                    try (MdcUtil.Scope ignored = MdcUtil.with(requestId, clientOrderId)) {
                        if (throwable != null) {
                            log.error(
                                    "gRPC placeOrder failed: userId={}, symbol={}, side={}, error={}",
                                    userId,
                                    request.getSymbol(),
                                    side,
                                    throwable.getMessage()
                            );
                            Throwable cause = throwable;
                            while (cause.getCause() != null) cause = cause.getCause();
                            Status status = cause instanceof OrderConflictException ? Status.ALREADY_EXISTS
                                    : cause instanceof IllegalArgumentException ? Status.INVALID_ARGUMENT : Status.UNAVAILABLE;
                            responseObserver.onError(status.withDescription(cause.getMessage()).asRuntimeException());
                            return;
                        }

                        log.info(
                                "gRPC placeOrder completed: userId={}, symbol={}, orderId={}, message={}",
                                userId,
                                request.getSymbol(),
                                result.getFinalOrder() == null ? "" : result.getFinalOrder().getOrderId(),
                                result.getMessage() == null ? "ok" : result.getMessage()
                        );
                    }
                    responseObserver.onNext(toResponse(result));
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void cancelOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> responseObserver) {
        String requestId = newRequestId();
        try (MdcUtil.Scope ignored = MdcUtil.withRequest(requestId)) {
            log.info("gRPC cancelOrder received: symbol={}, orderId={}", request.getSymbol(), request.getOrderId());
        }
        orderApplicationService.cancelOrder(requestId, request.getSymbol(), request.getOrderId())
                .whenComplete((success, throwable) -> {
                    try (MdcUtil.Scope ignored = MdcUtil.withRequest(requestId)) {
                        if (throwable != null) {
                            log.error(
                                    "gRPC cancelOrder failed: symbol={}, orderId={}, error={}",
                                    request.getSymbol(),
                                    request.getOrderId(),
                                    throwable.getMessage()
                            );
                            responseObserver.onError(statusFor(throwable).asRuntimeException());
                            return;
                        }

                        log.info(
                                "gRPC cancelOrder completed: symbol={}, orderId={}, success={}",
                                request.getSymbol(),
                                request.getOrderId(),
                                success
                        );
                    }
                    responseObserver.onNext(
                            CancelOrderResponse.newBuilder()
                                    .setSuccess(success)
                                    .setMessage(success ? "ok" : "order not found")
                                    .build()
                    );
                    responseObserver.onCompleted();
                });
    }

    @Override
    public void getBook(GetBookRequest request, StreamObserver<GetBookResponse> responseObserver) {
        String requestId = newRequestId();
        try (MdcUtil.Scope ignored = MdcUtil.withRequest(requestId)) {
            log.info("gRPC getBook received: symbol={}", request.getSymbol());
        }
        orderApplicationService.getBook(requestId, request.getSymbol())
                .whenComplete((lines, throwable) -> {
                    try (MdcUtil.Scope ignored = MdcUtil.withRequest(requestId)) {
                        if (throwable != null) {
                            log.error(
                                    "gRPC getBook failed: symbol={}, error={}",
                                    request.getSymbol(),
                                    throwable.getMessage()
                            );
                            responseObserver.onError(statusFor(throwable).asRuntimeException());
                            return;
                        }

                        log.info(
                                "gRPC getBook completed: symbol={}, lines={}",
                                request.getSymbol(),
                                lines.size()
                        );
                    }
                    responseObserver.onNext(
                            GetBookResponse.newBuilder()
                                    .setSuccess(true)
                                    .setMessage("ok")
                                    .addAllLines(lines)
                                    .build()
                    );
                    responseObserver.onCompleted();
                });
    }

    private static Side mapSide(com.example.order.grpc.Side side) {
        return switch (side) {
            case SIDE_BUY -> Side.BUY;
            case SIDE_SELL -> Side.SELL;
            case SIDE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("side is required");
        };
    }

    private static Status statusFor(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) cause = cause.getCause();
        return (cause instanceof IllegalArgumentException ? Status.INVALID_ARGUMENT : Status.UNAVAILABLE)
                .withDescription(cause.getMessage());
    }

    private static PlaceOrderResponse toResponse(MatchResult result) {
        return PlaceOrderResponse.newBuilder()
                .setSuccess(true)
                .setMessage(result.getMessage() == null ? "ok" : result.getMessage())
                .setOrderId(result.getFinalOrder() == null ? "" : result.getFinalOrder().getOrderId())
                .build();
    }

    private static String newRequestId() {
        return UUID.randomUUID().toString();
    }
}
