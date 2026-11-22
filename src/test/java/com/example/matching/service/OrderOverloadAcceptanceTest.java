package com.example.matching.service;

import akka.actor.typed.ActorSystem;
import com.example.matching.actor.EngineRootActor;
import com.example.matching.domain.Side;
import com.example.matching.engine.MatchResult;
import com.example.matching.persistence.FaultInjectionBarrier;
import com.example.matching.persistence.OrderEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class OrderOverloadAcceptanceTest {
    @TempDir Path data;

    @Test void stalledJournalBoundsInFlightAndRejectsBeforeRoutingWithoutLosingAcceptedOrders() throws Exception {
        ActorSystem<EngineRootActor.Command> root = ActorSystem.create(
                EngineRootActor.create(true, data, 1000, 64), "overload-acceptance");
        OrderApplicationService service = new OrderApplicationService(root, 2);
        Path signals = Files.createDirectory(data.resolve("signals"));
        System.setProperty("matching.test.signalDir", signals.toString());
        System.setProperty("matching.test.faultPoint", "journal-before-force");
        System.setProperty("matching.test.releasable", "true");
        try {
            CompletionStage<MatchResult> first = place(service, "first");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(signals.resolve("journal-before-force.reached"))
                    && System.nanoTime() < deadline) Thread.sleep(10);
            assertTrue(Files.exists(signals.resolve("journal-before-force.reached")));

            CompletionStage<MatchResult> second = place(service, "second");
            var overloaded = place(service, "third").toCompletableFuture();
            ExecutionException rejected = assertThrows(ExecutionException.class,
                    () -> overloaded.get(1, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, rejected.getCause());
            assertTrue(rejected.getCause().getMessage().contains("request not admitted"));
            assertFalse(first.toCompletableFuture().isDone());
            assertFalse(second.toCompletableFuture().isDone());

            System.clearProperty("matching.test.faultPoint");
            FaultInjectionBarrier.release("journal-before-force");
            assertEquals("first", first.toCompletableFuture().get(10, TimeUnit.SECONDS)
                    .getFinalOrder().getClientOrderId());
            assertEquals("second", second.toCompletableFuture().get(10, TimeUnit.SECONDS)
                    .getFinalOrder().getClientOrderId());
            assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
            MatchResult retry = place(service, "first").toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals("first", retry.getFinalOrder().getClientOrderId());
            assertTrue(retry.getMessage().startsWith("already accepted"));
            assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        } finally {
            FaultInjectionBarrier.release("journal-before-force");
            System.clearProperty("matching.test.signalDir");
            System.clearProperty("matching.test.faultPoint");
            System.clearProperty("matching.test.releasable");
            root.terminate();
            root.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private CompletionStage<MatchResult> place(OrderApplicationService service, String key) {
        return service.placeOrder("", key, "user", "BTCUSDT", Side.BUY, 10_000, 1);
    }
}
