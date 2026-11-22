package com.example.matching.persistence;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import com.example.matching.actor.SymbolActor;
import com.example.matching.domain.Order;
import com.example.matching.domain.Side;
import com.example.matching.engine.MatchResult;
import com.example.matching.engine.OrderBook;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Entry point for the separate JVM used by ProcessCrashAcceptanceTest. */
public final class CrashProcessWorker {
    private CrashProcessWorker() {}

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        Path data = Path.of(args[1]);
        Path signals = Path.of(args[2]);
        String point = args[3];
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "crash-worker");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, data, point.startsWith("snapshot") || point.startsWith("journal-cleanup") ? 2 : 1000),
                "symbol-btcusdt", Props.empty());
        if (mode.equals("crash")) {
            place(system, actor, new Order("BASE", "U1", "BTCUSDT", Side.BUY, 10000, 5, 1, "BASE"));
            Files.writeString(signals.resolve("confirmed-1"), "1");
            System.setProperty("matching.test.signalDir", signals.toString());
            System.setProperty("matching.test.faultPoint", point);
            MatchResult fill = place(system, actor, new Order("CRASH", "U2", "BTCUSDT", Side.SELL, 10000, 3, 2, "crash-key"));
            Files.writeString(signals.resolve("trade-2"), fill.getTrades().isEmpty() ? "0"
                    : Long.toString(fill.getTrades().get(0).getQuantity()));
            Files.writeString(signals.resolve("confirmed-2"), "2");
            new CountDownLatch(1).await();
        } else {
            List<String> book = AskPattern.<SymbolActor.Command, List<String>>ask(actor,
                    SymbolActor.GetBook::new, Duration.ofSeconds(10), system.scheduler())
                    .toCompletableFuture().get(10, TimeUnit.SECONDS);
            Properties result = new Properties();
            result.setProperty("book", String.join("|", book));
            result.setProperty("available", Boolean.toString(book.stream().noneMatch(s -> s.startsWith("SERVICE_UNAVAILABLE:"))));
            try {
                OrderEventStore.RecoveryState state = new OrderEventStore(data, "BTCUSDT").loadRecoveryState();
                result.setProperty("sequence", Long.toString(state.lastSequence()));
                result.setProperty("snapshotSequence", state.snapshot() == null ? "0" : state.snapshot().getLastAppliedSequence().toString());
                OrderBook rebuilt = new OrderBook();
                if (state.snapshot() != null) rebuilt.restoreSnapshot(
                        state.snapshot().getOpenOrders(), state.snapshot().getAllOrders());
                long replayedTradeQuantity = 0;
                String crashStatus = "NOT_IN_REPLAY_WINDOW";
                for (OrderEvent event : state.events()) {
                    if (event.getType() == OrderEvent.Type.PLACE) {
                        MatchResult replayed = rebuilt.place(event.toOrder(), event.getSequence());
                        if ("CRASH".equals(event.getOrderId())) {
                            replayedTradeQuantity = replayed.getTrades().stream().mapToLong(t -> t.getQuantity()).sum();
                            crashStatus = replayed.getFinalOrder().getStatus().name();
                        }
                    } else {
                        rebuilt.cancel(event.getOrderId());
                    }
                }
                result.setProperty("openCount", Integer.toString(rebuilt.openOrderCount()));
                result.setProperty("replayedTradeQuantity", Long.toString(replayedTradeQuantity));
                result.setProperty("crashStatus", crashStatus);
                for (Order open : rebuilt.exportOpenOrders()) {
                    if ("BASE".equals(open.getOrderId())) {
                        result.setProperty("baseRemaining", Long.toString(open.getRemainingQuantity()));
                        result.setProperty("baseStatus", open.getStatus().name());
                    }
                }
                if (Boolean.parseBoolean(result.getProperty("available"))) {
                    MatchResult retried = place(system, actor,
                            new Order("RETRY", "U2", "BTCUSDT", Side.SELL, 10000, 3, 3, "crash-key"));
                    result.setProperty("retryOrderId", retried.getFinalOrder().getOrderId());
                    result.setProperty("retryMessage", retried.getMessage());
                    result.setProperty("retryTradeQuantity", Long.toString(
                            retried.getTrades().stream().mapToLong(t -> t.getQuantity()).sum()));
                    result.setProperty("postRetrySequence", Long.toString(
                            new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence()));
                }
            } catch (RuntimeException ex) {
                result.setProperty("sequence", "UNKNOWN");
                result.setProperty("error", ex.getMessage());
            }
            try (var output = Files.newOutputStream(signals.resolve("recovered.properties"))) {
                result.store(output, "separate JVM recovery result");
            }
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static MatchResult place(ActorSystem<Void> system, ActorRef<SymbolActor.Command> actor, Order order) throws Exception {
        MatchResult result = AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                replyTo -> new SymbolActor.PlaceOrder(order, replyTo), Duration.ofSeconds(10), system.scheduler())
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        if (!"ok".equals(result.getMessage()) && !result.getMessage().startsWith("already accepted")) {
            throw new IllegalStateException(result.getMessage());
        }
        return result;
    }
}
