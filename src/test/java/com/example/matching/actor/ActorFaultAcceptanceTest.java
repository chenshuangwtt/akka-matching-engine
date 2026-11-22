package com.example.matching.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import com.example.matching.domain.Order;
import com.example.matching.domain.Side;
import com.example.matching.engine.MatchResult;
import com.example.matching.util.SymbolKeyUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class ActorFaultAcceptanceTest {
    @TempDir Path data;

    @Test
    void symbolActorRestartKeepsRootRouteAndReplaysBeforeServingNextOrder() throws Exception {
        ActorSystem<EngineRootActor.Command> root = ActorSystem.create(
                EngineRootActor.create(true, data, 1000, 64), "root-restart-test");
        MatchResult first = place(root, "BEFORE", 5);
        assertEquals("ok", first.getMessage());
        // A null reply actor causes GetBook to throw inside SymbolActor. The root's
        // cached child reference must remain usable through its supervisor restart.
        root.tell(new EngineRootActor.RouteGetBook("BTCUSDT", "", null));
        List<String> recovered = book(root);
        assertTrue(recovered.stream().anyMatch(line -> line.contains("id=BEFORE") && line.contains("remain=5")), recovered.toString());
        MatchResult next = place(root, "AFTER", 2);
        assertEquals("ok", next.getMessage());
        assertTrue(book(root).stream().anyMatch(line -> line.contains("id=AFTER")));
        assertEquals(2, new com.example.matching.persistence.OrderEventStore(data, "BTCUSDT")
                .loadRecoveryState().lastSequence());
        root.terminate();
        root.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void persistenceActorStopsAfterCompactionFailureAndSymbolRejectsNewOrders() throws Exception {
        ActorSystem<EngineRootActor.Command> root = ActorSystem.create(
                EngineRootActor.create(true, data, 2, 64), "persistence-stop-test");
        assertEquals("ok", place(root, "BASE", 5).getMessage());
        Path tempSnapshot = data.resolve(SymbolKeyUtil.toPersistenceStem("BTCUSDT") + ".snapshot.json.tmp");
        Files.createDirectory(tempSnapshot); // deterministic failure when compaction opens its temporary file
        assertEquals("ok", sell(root, "FILL", 3).getMessage()); // journal commit preceded compaction
        String unavailable = awaitUnavailable(root);
        assertTrue(unavailable.startsWith("SERVICE_UNAVAILABLE:"), unavailable);
        assertTrue(place(root, "REJECTED", 1).getMessage().startsWith("SERVICE_UNAVAILABLE:"));
        root.terminate();
        root.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);

        Files.delete(tempSnapshot);
        ActorSystem<EngineRootActor.Command> restarted = ActorSystem.create(
                EngineRootActor.create(true, data, 2, 64), "persistence-stop-restart");
        assertTrue(book(restarted).stream().anyMatch(line -> line.contains("id=BASE") && line.contains("remain=2")));
        assertFalse(book(restarted).stream().anyMatch(line -> line.contains("id=REJECTED")));
        assertEquals(2, new com.example.matching.persistence.OrderEventStore(data, "BTCUSDT")
                .loadRecoveryState().lastSequence());
        restarted.terminate();
        restarted.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void replyInterruptedBySymbolActorRestartIsUnknownAndRecoveredFromJournal() throws Exception {
        ActorSystem<EngineRootActor.Command> root = ActorSystem.create(
                EngineRootActor.create(true, data, 1000, 64), "pending-restart-test");
        assertEquals("ok", place(root, "BASE", 5).getMessage());
        Path signals = Files.createDirectory(data.resolve("signals"));
        System.setProperty("matching.test.signalDir", signals.toString());
        System.setProperty("matching.test.faultPoint", "actor-before-reply");
        try {
            var pending = AskPattern.<EngineRootActor.Command, MatchResult>ask(root,
                    replyTo -> new EngineRootActor.RoutePlaceOrder(
                            new Order("UNCONFIRMED", "U", "BTCUSDT", Side.SELL, 10000, 3, 2, "retry-key"), replyTo),
                    Duration.ofMillis(750), root.scheduler()).toCompletableFuture();
            ExecutionException error = assertThrows(ExecutionException.class,
                    () -> pending.get(3, TimeUnit.SECONDS));
            assertTrue(error.getCause().getMessage().toLowerCase().contains("timeout"));
            assertEquals("2", Files.readString(signals.resolve("actor-before-reply.reached")));
        } finally {
            System.clearProperty("matching.test.signalDir");
            System.clearProperty("matching.test.faultPoint");
        }
        assertTrue(book(root).stream().anyMatch(line -> line.contains("id=BASE") && line.contains("remain=2")));
        MatchResult retried = askPlace(root,
                new Order("RETRY", "U", "BTCUSDT", Side.SELL, 10000, 3, 3, "retry-key"));
        assertEquals("UNCONFIRMED", retried.getFinalOrder().getOrderId());
        assertTrue(retried.getTrades().isEmpty());
        assertEquals(2, new com.example.matching.persistence.OrderEventStore(data, "BTCUSDT")
                .loadRecoveryState().lastSequence());
        root.terminate();
        root.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void permanentlyStoppedSymbolIsRemovedFromRootCacheAndRecreated() throws Exception {
        ActorSystem<EngineRootActor.Command> root = ActorSystem.create(
                EngineRootActor.create(true, data, 1000, 64), "root-permanent-stop-test");
        assertEquals("ok", place(root, "BASE", 5).getMessage());
        Path signals = Files.createDirectory(data.resolve("signals"));
        System.setProperty("matching.test.signalDir", signals.toString());
        System.setProperty("matching.test.faultPoint", "actor-fatal-before-reply");
        try {
            var pending = AskPattern.<EngineRootActor.Command, MatchResult>ask(root,
                    replyTo -> new EngineRootActor.RoutePlaceOrder(
                            new Order("UNCONFIRMED", "U", "BTCUSDT", Side.SELL, 10000, 3, 2, "retry-key"), replyTo),
                    Duration.ofMillis(750), root.scheduler()).toCompletableFuture();
            assertThrows(ExecutionException.class, () -> pending.get(3, TimeUnit.SECONDS));
            assertEquals("2", Files.readString(signals.resolve("actor-fatal-before-reply.reached")));
        } finally {
            System.clearProperty("matching.test.signalDir");
            System.clearProperty("matching.test.faultPoint");
        }
        assertTrue(book(root).stream().anyMatch(line -> line.contains("id=BASE") && line.contains("remain=2")));
        MatchResult retried = askPlace(root,
                new Order("RETRY", "U", "BTCUSDT", Side.SELL, 10000, 3, 3, "retry-key"));
        assertEquals("UNCONFIRMED", retried.getFinalOrder().getOrderId());
        assertTrue(retried.getTrades().isEmpty());
        assertEquals("ok", place(root, "AFTER", 1).getMessage());
        assertEquals(3, new com.example.matching.persistence.OrderEventStore(data, "BTCUSDT")
                .loadRecoveryState().lastSequence());
        root.terminate();
        root.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private MatchResult place(ActorSystem<EngineRootActor.Command> root, String id, long quantity) throws Exception {
        return askPlace(root, new Order(id, "U", "BTCUSDT", Side.BUY, 10000, quantity, 1, id));
    }

    private MatchResult sell(ActorSystem<EngineRootActor.Command> root, String id, long quantity) throws Exception {
        return askPlace(root, new Order(id, "U", "BTCUSDT", Side.SELL, 10000, quantity, 2, id));
    }

    private MatchResult askPlace(ActorSystem<EngineRootActor.Command> root, Order order) throws Exception {
        return AskPattern.<EngineRootActor.Command, MatchResult>ask(root,
                (ActorRef<MatchResult> replyTo) -> new EngineRootActor.RoutePlaceOrder(order, replyTo),
                Duration.ofSeconds(10), root.scheduler()).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private List<String> book(ActorSystem<EngineRootActor.Command> root) throws Exception {
        return AskPattern.<EngineRootActor.Command, List<String>>ask(root,
                (ActorRef<List<String>> replyTo) -> new EngineRootActor.RouteGetBook("BTCUSDT", replyTo),
                Duration.ofSeconds(10), root.scheduler()).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private String awaitUnavailable(ActorSystem<EngineRootActor.Command> root) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            List<String> lines = book(root);
            if (lines.get(0).startsWith("SERVICE_UNAVAILABLE:")) return lines.get(0);
            Thread.sleep(10);
        }
        fail("symbol did not become unavailable after persistence actor stopped");
        return "";
    }
}
