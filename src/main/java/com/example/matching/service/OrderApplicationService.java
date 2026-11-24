package com.example.matching.service;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;

import com.example.matching.actor.EngineRootActor;
import com.example.matching.actor.SymbolActor;
import com.example.matching.domain.Order;
import com.example.matching.domain.OrderRules;
import com.example.matching.domain.Side;
import com.example.matching.engine.MatchResult;
import com.example.matching.util.MdcUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

@Service
public class OrderApplicationService {

    private static final Logger log = LoggerFactory.getLogger(OrderApplicationService.class);

    private final ActorSystem<EngineRootActor.Command> actorSystem;
    private final Duration timeout = Duration.ofSeconds(65);
    private final Semaphore inFlight;
    private final int maxInFlight;

    public OrderApplicationService(ActorSystem<EngineRootActor.Command> actorSystem) {
        this(actorSystem, 1_000);
    }

    @Autowired
    public OrderApplicationService(
            ActorSystem<EngineRootActor.Command> actorSystem,
            @Value("${matching.max-in-flight-requests:1000}") int maxInFlight) {
        if (maxInFlight <= 0 || maxInFlight > 5_000) {
            throw new IllegalArgumentException(
                    "matching.max-in-flight-requests must be between 1 and 5000");
        }
        this.actorSystem = actorSystem;
        this.maxInFlight = maxInFlight;
        this.inFlight = new Semaphore(maxInFlight);
    }

    private <T> CompletionStage<T> admit(String operation, Supplier<CompletionStage<T>> action) {
        if (!inFlight.tryAcquire()) {
            log.warn(
                    "Request overload: operation={}, inFlight={}, limit={}",
                    operation,
                    maxInFlight - inFlight.availablePermits(),
                    maxInFlight);
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "SERVICE_UNAVAILABLE: overloaded; request not admitted"));
        }
        try {
            return action.get().whenComplete((ignored, error) -> inFlight.release());
        } catch (RuntimeException ex) {
            inFlight.release();
            return CompletableFuture.failedFuture(ex);
        }
    }

    // HTTP / gRPC 共用的下单入口。
    public CompletionStage<MatchResult> placeOrder(
            String requestId,
            String clientOrderId,
            String userId,
            String symbol,
            Side side,
            long price,
            long quantity) {
        Order order =
                new Order(
                        UUID.randomUUID().toString(),
                        userId,
                        symbol,
                        side,
                        price,
                        quantity,
                        System.currentTimeMillis(),
                        clientOrderId);
        try {
            OrderRules.validateNewOrder(order);
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }

        try (MdcUtil.Scope ignored = MdcUtil.withRequest(requestId)) {
            log.info(
                    "OrderApplicationService placeOrder: symbol={}, orderId={}",
                    symbol,
                    order.getOrderId());
        }
        return admit(
                "place",
                () ->
                        AskPattern.<EngineRootActor.Command, MatchResult>ask(
                                        actorSystem,
                                        (ActorRef<MatchResult> replyTo) ->
                                                new EngineRootActor.RoutePlaceOrder(
                                                        order, requestId, replyTo),
                                        timeout,
                                        actorSystem.scheduler())
                                .exceptionallyCompose(
                                        ex -> {
                                            log.warn(
                                                    "Place outcome unknown after actor failure or"
                                                        + " timeout: symbol={}, clientOrderId={}",
                                                    symbol,
                                                    clientOrderId,
                                                    ex);
                                            return CompletableFuture.failedFuture(
                                                    new IllegalStateException(
                                                            "SERVICE_UNAVAILABLE: request outcome"
                                                                + " unknown after actor failure or"
                                                                + " timeout",
                                                            ex));
                                        })
                                .thenCompose(
                                        result -> {
                                            if (result.getMessage() != null
                                                    && result.getMessage()
                                                            .startsWith("SERVICE_UNAVAILABLE:")) {
                                                return CompletableFuture.failedFuture(
                                                        new IllegalStateException(
                                                                result.getMessage()));
                                            }
                                            if (result.getMessage() != null
                                                    && (result.getMessage()
                                                                    .startsWith(
                                                                            "IDEMPOTENCY_CONFLICT:")
                                                            || result.getMessage()
                                                                    .startsWith(
                                                                            "ORDER_ID_CONFLICT:"))) {
                                                return CompletableFuture.failedFuture(
                                                        new OrderConflictException(
                                                                result.getMessage()));
                                            }
                                            if (result.getMessage() != null
                                                    && result.getMessage()
                                                            .startsWith("INVALID_ORDER:")) {
                                                return CompletableFuture.failedFuture(
                                                        new IllegalArgumentException(
                                                                result.getMessage()));
                                            }
                                            return CompletableFuture.completedFuture(result);
                                        }));
    }

    // HTTP / gRPC 共用的撤单入口。
    public CompletionStage<Boolean> cancelOrder(String symbol, String orderId) {
        return cancelOrder("", symbol, orderId);
    }

    public CompletionStage<Boolean> cancelOrder(String requestId, String symbol, String orderId) {
        try {
            OrderRules.validateSymbol(symbol);
            OrderRules.validateId(orderId, "orderId");
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        try (MdcUtil.Scope ignored = MdcUtil.withRequest(requestId)) {
            log.info("OrderApplicationService cancelOrder: symbol={}, orderId={}", symbol, orderId);
        }
        return admit(
                "cancel",
                () ->
                        AskPattern.<EngineRootActor.Command, SymbolActor.CancelResult>ask(
                                        actorSystem,
                                        (ActorRef<SymbolActor.CancelResult> replyTo) ->
                                                new EngineRootActor.RouteCancelOrder(
                                                        symbol, orderId, requestId, replyTo),
                                        timeout,
                                        actorSystem.scheduler())
                                .exceptionallyCompose(
                                        ex -> {
                                            log.warn(
                                                    "Cancel outcome unknown after actor failure or"
                                                            + " timeout: symbol={}, orderId={}",
                                                    symbol,
                                                    orderId,
                                                    ex);
                                            return CompletableFuture.failedFuture(
                                                    new IllegalStateException(
                                                            "SERVICE_UNAVAILABLE: cancel outcome"
                                                                + " unknown after actor failure or"
                                                                + " timeout",
                                                            ex));
                                        })
                                .thenCompose(
                                        result ->
                                                result.message().startsWith("SERVICE_UNAVAILABLE:")
                                                        ? CompletableFuture.failedFuture(
                                                                new IllegalStateException(
                                                                        result.message()))
                                                        : CompletableFuture.completedFuture(
                                                                result.success())));
    }

    // HTTP / gRPC 共用的订单簿查询入口。
    public CompletionStage<List<String>> getBook(String symbol) {
        return getBook("", symbol);
    }

    public CompletionStage<List<String>> getBook(String requestId, String symbol) {
        try {
            OrderRules.validateSymbol(symbol);
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        try (MdcUtil.Scope ignored = MdcUtil.withRequest(requestId)) {
            log.info("OrderApplicationService getBook: symbol={}", symbol);
        }
        return admit(
                "book",
                () ->
                        AskPattern.<EngineRootActor.Command, List<String>>ask(
                                        actorSystem,
                                        (ActorRef<List<String>> replyTo) ->
                                                new EngineRootActor.RouteGetBook(
                                                        symbol, requestId, replyTo),
                                        timeout,
                                        actorSystem.scheduler())
                                .exceptionallyCompose(
                                        ex -> {
                                            log.warn(
                                                    "Book request failed or timed out: symbol={}",
                                                    symbol,
                                                    ex);
                                            return CompletableFuture.failedFuture(
                                                    new IllegalStateException(
                                                            "SERVICE_UNAVAILABLE: book request"
                                                                    + " failed or timed out",
                                                            ex));
                                        })
                                .thenCompose(
                                        lines ->
                                                !lines.isEmpty()
                                                                && lines.get(0)
                                                                        .startsWith(
                                                                                "SERVICE_UNAVAILABLE:")
                                                        ? CompletableFuture.failedFuture(
                                                                new IllegalStateException(
                                                                        lines.get(0)))
                                                        : CompletableFuture.completedFuture(
                                                                lines)));
    }
}
