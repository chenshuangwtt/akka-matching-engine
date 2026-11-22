package com.example.matching.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import com.example.matching.domain.Order;
import com.example.matching.domain.Side;
import com.example.matching.engine.MatchResult;
import com.example.matching.util.SymbolKeyUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolActorPersistenceTest {

    @Test void emptyBookDoesNotCompactUntilEventThresholdAndThenRecovers() throws Exception {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "compact-threshold-test");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, 4), "symbol-btcusdt", Props.empty());
        try {
            askPlace(system, actor, new Order("B1", "U1", "BTCUSDT", Side.BUY, 10000, 1, 1, "B1"));
            askPlace(system, actor, new Order("S1", "U2", "BTCUSDT", Side.SELL, 10000, 1, 2, "S1"));
            assertFalse(Files.exists(persistenceFile("BTCUSDT", ".snapshot.json")));
            askPlace(system, actor, new Order("B2", "U1", "BTCUSDT", Side.BUY, 10000, 1, 3, "B2"));
            askPlace(system, actor, new Order("S2", "U2", "BTCUSDT", Side.SELL, 10000, 1, 4, "S2"));
            awaitUntil(() -> Files.exists(persistenceFile("BTCUSDT", ".snapshot.json")));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        var recovered = new com.example.matching.persistence.OrderEventStore(tempDir, "BTCUSDT")
                .loadRecoveryState();
        assertTrue(recovered.snapshot() != null && recovered.snapshot().getLastAppliedSequence() == 4L);
        assertTrue(recovered.events().isEmpty());
        assertTrue(recovered.snapshot().getOpenOrders().isEmpty());
        assertTrue(recovered.snapshot().getAllOrders().size() == 4);
    }

    @Test void busyBookForcesCheckpointAfterBoundedEventMultiple() throws Exception {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "busy-compact-test");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, 2, 0), "symbol-btcusdt", Props.empty());
        try {
            for (int i = 1; i <= 2; i++) {
                askPlace(system, actor, new Order("B" + i, "U", "BTCUSDT", Side.BUY,
                        10000, 1, i, "B" + i));
            }
            assertFalse(Files.exists(persistenceFile("BTCUSDT", ".snapshot.json")));
            for (int i = 3; i <= 8; i++) {
                askPlace(system, actor, new Order("B" + i, "U", "BTCUSDT", Side.BUY,
                        10000, 1, i, "B" + i));
            }
            awaitUntil(() -> Files.exists(persistenceFile("BTCUSDT", ".snapshot.json")));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        var recovered = new com.example.matching.persistence.OrderEventStore(tempDir, "BTCUSDT")
                .loadRecoveryState();
        assertTrue(recovered.snapshot() != null && recovered.snapshot().getLastAppliedSequence() == 8L);
        assertTrue(recovered.snapshot().getOpenOrders().size() == 8);
    }

    private static final int SNAPSHOT_THRESHOLD = 2;
    private static final int NO_COMPACT_THRESHOLD = 1_000;

    @TempDir
    Path tempDir;

    @Test
    void shouldReplayEventsAfterRestart() throws Exception {
        ActorSystem<Void> firstSystem = ActorSystem.create(Behaviors.empty(), "persist-test-1");
        ActorRef<SymbolActor.Command> firstActor = firstSystem.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, SNAPSHOT_THRESHOLD),
                "symbol-btcusdt",
                Props.empty()
        );

        Order buy = new Order("O1", "U1", "BTCUSDT", Side.BUY, 10_000, 5, 1L, "O1");
        Order sell = new Order("O2", "U2", "BTCUSDT", Side.SELL, 10_100, 3, 2L, "O2");

        askPlace(firstSystem, firstActor, buy);
        askPlace(firstSystem, firstActor, sell);

        List<String> beforeRestart = askBook(firstSystem, firstActor);
        assertTrue(beforeRestart.stream().anyMatch(line -> line.contains("BUY price=10000")));
        assertTrue(beforeRestart.stream().anyMatch(line -> line.contains("SELL price=10100")));
        Path snapshotFile = persistenceFile("BTCUSDT", ".snapshot.json");
        Path eventFile = persistenceFile("BTCUSDT", ".events.jsonl");
        awaitUntil(() -> Files.exists(snapshotFile));
        awaitUntil(() -> Files.exists(eventFile));

        firstSystem.terminate();
        firstSystem.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        var checkpoint = new com.example.matching.persistence.OrderEventStore(tempDir, "BTCUSDT")
                .loadRecoveryState();
        assertTrue(checkpoint.snapshot() != null
                && checkpoint.snapshot().getLastAppliedSequence() == 2L);
        assertTrue(checkpoint.lastSequence() == 2L);

        ActorSystem<Void> secondSystem = ActorSystem.create(Behaviors.empty(), "persist-test-2");
        ActorRef<SymbolActor.Command> secondActor = secondSystem.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, SNAPSHOT_THRESHOLD),
                "symbol-btcusdt",
                Props.empty()
        );

        List<String> afterRestart = askBook(secondSystem, secondActor);
        assertTrue(afterRestart.stream().anyMatch(line -> line.contains("BUY price=10000")));
        assertTrue(afterRestart.stream().anyMatch(line -> line.contains("SELL price=10100")));

        boolean canceled = askCancel(secondSystem, secondActor, "O1");
        assertTrue(canceled);
        assertFalse(askBook(secondSystem, secondActor).stream().anyMatch(line -> line.contains("id=O1")));

        secondSystem.terminate();
        secondSystem.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);

        ActorSystem<Void> thirdSystem = ActorSystem.create(Behaviors.empty(), "persist-test-3");
        ActorRef<SymbolActor.Command> thirdActor = thirdSystem.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, SNAPSHOT_THRESHOLD),
                "symbol-btcusdt",
                Props.empty()
        );

        List<String> afterCancelReplay = askBook(thirdSystem, thirdActor);
        assertFalse(afterCancelReplay.stream().anyMatch(line -> line.contains("id=O1")));
        assertTrue(afterCancelReplay.stream().anyMatch(line -> line.contains("id=O2")));
        awaitUntil(() -> Files.readString(eventFile, StandardCharsets.UTF_8).contains("\"type\":\"CANCEL\""));

        thirdSystem.terminate();
        thirdSystem.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldRejectOrdersWhenSnapshotIsCorrupted() throws Exception {
        Path eventFile = persistenceFile("BTCUSDT", ".events.jsonl");
        new com.example.matching.persistence.OrderEventStore(tempDir, "BTCUSDT")
                .appendPlace(new Order("HISTORICAL", "U", "BTCUSDT", Side.BUY, 10000, 1, 1, "HISTORICAL"));
        Files.writeString(persistenceFile("BTCUSDT", ".snapshot.json"), "{broken-json", StandardCharsets.UTF_8);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "persist-test-corrupted");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, SNAPSHOT_THRESHOLD),
                "symbol-btcusdt",
                Props.empty()
        );

        List<String> book = askBook(system, actor);
        assertTrue(book.stream().anyMatch(line -> line.contains("SERVICE_UNAVAILABLE: recovery failed")));
        MatchResult rejected = askPlaceResult(system, actor,
                new Order("O300", "U300", "BTCUSDT", Side.BUY, 10_000, 1, 30L, "O300"));
        assertTrue(rejected.getMessage().startsWith("SERVICE_UNAVAILABLE:"));
        assertTrue(Files.readString(eventFile).contains("HISTORICAL"));

        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldFailClosedWhenJournalAppendFails() throws Exception {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "persist-test-write-failure");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        askBook(system, actor); // wait for recovery
        Files.createDirectory(persistenceFile("BTCUSDT", ".events.jsonl"));
        MatchResult rejected = askPlaceResult(system, actor,
                new Order("FAILED", "U", "BTCUSDT", Side.BUY, 10000, 2, 1, "FAILED"));
        assertTrue(rejected.getMessage().startsWith("SERVICE_UNAVAILABLE:"));
        assertTrue(askBook(system, actor).stream().anyMatch(line -> line.contains("SERVICE_UNAVAILABLE:")));
        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Files.delete(persistenceFile("BTCUSDT", ".events.jsonl"));
        ActorSystem<Void> restarted = ActorSystem.create(Behaviors.empty(), "persist-test-write-failure-restart");
        ActorRef<SymbolActor.Command> recovered = restarted.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        assertFalse(askBook(restarted, recovered).stream().anyMatch(line -> line.contains("id=FAILED")));
        restarted.terminate();
        restarted.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldKeepOrderWhenCancelAppendFails() throws Exception {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "persist-test-cancel-failure");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        askPlace(system, actor, new Order("LIVE", "U", "BTCUSDT", Side.BUY, 10000, 2, 1, "LIVE"));
        Path eventFile = persistenceFile("BTCUSDT", ".events.jsonl");
        Files.move(eventFile, tempDir.resolve("saved-journal"));
        Files.createDirectory(eventFile);
        SymbolActor.CancelResult rejected = askCancelResult(system, actor, "LIVE");
        assertTrue(rejected.message().startsWith("SERVICE_UNAVAILABLE:"));
        // The failed cancel has not changed the in-memory order.
        // The actor is unavailable, so inspect its unchanged journal and then restore on restart.
        assertTrue(Files.readString(tempDir.resolve("saved-journal")).contains("LIVE"));
        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Files.delete(eventFile);
        Files.move(tempDir.resolve("saved-journal"), eventFile);
        ActorSystem<Void> restarted = ActorSystem.create(Behaviors.empty(), "persist-test-cancel-restart");
        ActorRef<SymbolActor.Command> recovered = restarted.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        assertTrue(askBook(restarted, recovered).stream().anyMatch(line -> line.contains("id=LIVE")));
        restarted.terminate();
        restarted.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldPreserveSameSymbolOrderThroughAcknowledgedWrites() throws Exception {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "persist-test-ordering");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        var first = AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                replyTo -> new SymbolActor.PlaceOrder(new Order("FIRST", "U", "BTCUSDT", Side.BUY, 10000, 2, 1, "FIRST"), replyTo),
                Duration.ofSeconds(3), system.scheduler()).toCompletableFuture();
        var second = AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                replyTo -> new SymbolActor.PlaceOrder(new Order("SECOND", "U", "BTCUSDT", Side.BUY, 10000, 3, 2, "SECOND"), replyTo),
                Duration.ofSeconds(3), system.scheduler()).toCompletableFuture();
        assertTrue(first.get(3, TimeUnit.SECONDS).getMessage().equals("ok"));
        assertTrue(second.get(3, TimeUnit.SECONDS).getMessage().equals("ok"));
        String journal = Files.readString(persistenceFile("BTCUSDT", ".events.jsonl"));
        assertTrue(journal.indexOf("FIRST") < journal.indexOf("SECOND"));
        assertTrue(journal.contains("\"sequence\":1"));
        assertTrue(journal.contains("\"sequence\":2"));
        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldRecoverPartialFillAndCancelAcrossRestarts() throws Exception {
        ActorSystem<Void> first = ActorSystem.create(Behaviors.empty(), "partial-first");
        ActorRef<SymbolActor.Command> firstActor = first.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        askPlace(first, firstActor, new Order("BUY", "U1", "BTCUSDT", Side.BUY, 10000, 5, 1, "BUY"));
        MatchResult fill = askPlaceResult(first, firstActor, new Order("SELL", "U2", "BTCUSDT", Side.SELL, 10000, 3, 2, "SELL"));
        assertTrue(fill.getTrades().size() == 1 && fill.getTrades().get(0).getQuantity() == 3);
        assertTrue(askBook(first, firstActor).stream().anyMatch(line -> line.contains("id=BUY") && line.contains("remain=2")));
        first.terminate();
        first.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);

        ActorSystem<Void> second = ActorSystem.create(Behaviors.empty(), "partial-second");
        ActorRef<SymbolActor.Command> secondActor = second.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        assertTrue(askBook(second, secondActor).stream().anyMatch(line -> line.contains("id=BUY") && line.contains("remain=2")));
        assertTrue(askCancel(second, secondActor, "BUY"));
        second.terminate();
        second.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);

        ActorSystem<Void> third = ActorSystem.create(Behaviors.empty(), "partial-third");
        ActorRef<SymbolActor.Command> thirdActor = third.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        assertFalse(askBook(third, thirdActor).stream().anyMatch(line -> line.contains("id=BUY")));
        third.terminate();
        third.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldRejectNewOrdersWhenJournalIsCorrupted() throws Exception {
        Files.writeString(persistenceFile("BTCUSDT", ".events.jsonl"), "{\"type\":\"PLACE\"", StandardCharsets.UTF_8);
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "corrupted-journal");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD), "symbol-btcusdt", Props.empty());
        MatchResult result = askPlaceResult(system, actor, orderFor("NEW"));
        assertTrue(result.getMessage().startsWith("SERVICE_UNAVAILABLE: recovery failed"));
        assertTrue(Files.readString(persistenceFile("BTCUSDT", ".events.jsonl")).contains("PLACE"));
        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static Order orderFor(String id) {
        return new Order(id, "U", "BTCUSDT", Side.BUY, 10000, 1, 1, id);
    }

    @Test
    void shouldPersistPlaceOrderThroughAsyncWritePath() throws Exception {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "persist-test-write-path");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create("BTCUSDT", true, tempDir, NO_COMPACT_THRESHOLD),
                "symbol-btcusdt",
                Props.empty()
        );

        Order buy = new Order("O100", "U100", "BTCUSDT", Side.BUY, 10_000, 2, 10L, "O100");
        askPlace(system, actor, buy);

        List<String> book = askBook(system, actor);
        assertTrue(book.stream().anyMatch(line -> line.contains("id=O100")));

        Path eventFile = persistenceFile("BTCUSDT", ".events.jsonl");
        awaitUntil(() -> Files.exists(eventFile)
                && Files.readString(eventFile, StandardCharsets.UTF_8).contains("\"type\":\"PLACE\"")
                && Files.readString(eventFile, StandardCharsets.UTF_8).contains("\"orderId\":\"O100\""));

        assertFalse(Files.exists(persistenceFile("BTCUSDT", ".snapshot.json")));

        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldPersistSymbolWithSlashUsingSafeFileNames() throws Exception {
        String symbol = "BTC/USDT";
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "persist-test-slash-symbol");
        ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                SymbolActor.create(symbol, true, tempDir, NO_COMPACT_THRESHOLD),
                "symbol-btc-usdt-test",
                Props.empty()
        );

        Order buy = new Order("O200", "U200", symbol, Side.BUY, 20_000, 1, 20L, "O200");
        askPlace(system, actor, buy);

        Path eventFile = persistenceFile(symbol, ".events.jsonl");
        awaitUntil(() -> Files.exists(eventFile)
                && Files.readString(eventFile, StandardCharsets.UTF_8).contains("\"symbol\":\"BTC/USDT\""));

        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private Path persistenceFile(String symbol, String suffix) {
        return tempDir.resolve(SymbolKeyUtil.toPersistenceStem(symbol) + suffix);
    }

    private static void askPlace(ActorSystem<Void> system,
                                 ActorRef<SymbolActor.Command> actor,
                                 Order order) throws Exception {
        MatchResult ignored = askPlaceResult(system, actor, order);
        assertTrue(ignored != null);
    }

    private static MatchResult askPlaceResult(ActorSystem<Void> system,
                                  ActorRef<SymbolActor.Command> actor,
                                  Order order) throws Exception {
        return AskPattern.ask(
                actor,
                (ActorRef<MatchResult> replyTo) -> new SymbolActor.PlaceOrder(order, replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        ).toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    private static boolean askCancel(ActorSystem<Void> system,
                                     ActorRef<SymbolActor.Command> actor,
                                     String orderId) throws Exception {
        return askCancelResult(system, actor, orderId).success();
    }

    private static SymbolActor.CancelResult askCancelResult(ActorSystem<Void> system,
                                      ActorRef<SymbolActor.Command> actor,
                                      String orderId) throws Exception {
        return AskPattern.ask(
                actor,
                (ActorRef<SymbolActor.CancelResult> replyTo) -> new SymbolActor.CancelOrder(orderId, replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        ).toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    private static List<String> askBook(ActorSystem<Void> system,
                                        ActorRef<SymbolActor.Command> actor) throws Exception {
        return AskPattern.ask(
                actor,
                (ActorRef<List<String>> replyTo) -> new SymbolActor.GetBook(replyTo),
                Duration.ofSeconds(3),
                system.scheduler()
        ).toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    private static void awaitUntil(CheckedBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
