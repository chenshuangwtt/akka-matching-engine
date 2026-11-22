package com.example.matching.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import com.example.matching.domain.Order;
import com.example.matching.domain.OrderStatus;
import com.example.matching.domain.Side;
import com.example.matching.engine.MatchResult;
import com.example.matching.persistence.OrderEventStore;
import com.example.matching.persistence.FaultInjectionBarrier;
import com.example.matching.util.SymbolKeyUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class SymbolActorIdempotencyTest {
    @TempDir Path data;

    private ActorSystem<Void> system(String name) { return ActorSystem.create(Behaviors.empty(), name); }
    private ActorRef<SymbolActor.Command> actor(ActorSystem<Void> system, int threshold) {
        return system.systemActorOf(SymbolActor.create("BTCUSDT", true, data, threshold), "symbol-btcusdt", Props.empty());
    }
    private Order order(String orderId, String clientId, String userId, Side side, long price, long quantity) {
        return new Order(orderId, userId, "BTCUSDT", side, price, quantity, 1L, clientId);
    }
    private MatchResult place(ActorSystem<Void> system, ActorRef<SymbolActor.Command> actor, Order order) throws Exception {
        return AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                replyTo -> new SymbolActor.PlaceOrder(order, replyTo), Duration.ofSeconds(5), system.scheduler())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    private SymbolActor.CancelResult cancel(ActorSystem<Void> system, ActorRef<SymbolActor.Command> actor, String id) throws Exception {
        return AskPattern.<SymbolActor.Command, SymbolActor.CancelResult>ask(actor,
                replyTo -> new SymbolActor.CancelOrder(id, replyTo), Duration.ofSeconds(5), system.scheduler())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    private List<String> book(ActorSystem<Void> system, ActorRef<SymbolActor.Command> actor) throws Exception {
        return AskPattern.<SymbolActor.Command, List<String>>ask(actor,
                SymbolActor.GetBook::new, Duration.ofSeconds(5), system.scheduler())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    private void stop(ActorSystem<Void> system) throws Exception {
        system.terminate(); system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test void sameClientOrderAndSamePayloadReturnOriginalIdWithoutSecondEvent() throws Exception {
        var system = system("idem-same"); var actor = actor(system, 1000);
        assertEquals("A", place(system, actor, order("A", "key", "user", Side.BUY, 10000, 5)).getFinalOrder().getOrderId());
        MatchResult retry = place(system, actor, order("B", "key", "user", Side.BUY, 10000, 5));
        assertEquals("A", retry.getFinalOrder().getOrderId());
        assertTrue(retry.getMessage().startsWith("already accepted"));
        assertTrue(retry.getTrades().isEmpty());
        assertEquals(1, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        assertEquals(1, book(system, actor).stream().filter(s -> s.contains("id=A")).count());
        stop(system);
    }

    @Test void sameClientOrderWithChangedPayloadConflictsAndDifferentUserIsSeparate() throws Exception {
        var system = system("idem-conflict"); var actor = actor(system, 1000);
        place(system, actor, order("A", "key", "user", Side.BUY, 10000, 5));
        MatchResult conflict = place(system, actor, order("B", "key", "user", Side.BUY, 10100, 5));
        assertTrue(conflict.getMessage().startsWith("IDEMPOTENCY_CONFLICT:"));
        assertEquals(1, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        assertEquals("C", place(system, actor, order("C", "key", "other-user", Side.BUY, 10000, 5)).getFinalOrder().getOrderId());
        stop(system);
    }

    @Test void concurrentSameKeySubmissionsHaveOneAcceptedOrder() throws Exception {
        var system = system("idem-concurrent"); var actor = actor(system, 1000);
        var first = AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                replyTo -> new SymbolActor.PlaceOrder(order("A", "key", "user", Side.BUY, 10000, 5), replyTo),
                Duration.ofSeconds(5), system.scheduler()).toCompletableFuture();
        var second = AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                replyTo -> new SymbolActor.PlaceOrder(order("B", "key", "user", Side.BUY, 10000, 5), replyTo),
                Duration.ofSeconds(5), system.scheduler()).toCompletableFuture();
        assertEquals("A", first.get(5, TimeUnit.SECONDS).getFinalOrder().getOrderId());
        assertEquals("A", second.get(5, TimeUnit.SECONDS).getFinalOrder().getOrderId());
        assertEquals(1, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(system);
    }

    @Test void retryArrivingWhileFirstWriteIsUnconfirmedWaitsAndReturnsOriginalOrder() throws Exception {
        var system = system("idem-pending"); var actor = actor(system, 1000);
        Path signals = Files.createDirectory(data.resolve("signals"));
        System.setProperty("matching.test.signalDir", signals.toString());
        System.setProperty("matching.test.faultPoint", "journal-before-force");
        System.setProperty("matching.test.releasable", "true");
        try {
            var first = AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                    replyTo -> new SymbolActor.PlaceOrder(order("A", "key", "user", Side.BUY, 10000, 5), replyTo),
                    Duration.ofSeconds(5), system.scheduler()).toCompletableFuture();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(signals.resolve("journal-before-force.reached")) && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertTrue(Files.exists(signals.resolve("journal-before-force.reached")));
            assertFalse(first.isDone());
            var retry = AskPattern.<SymbolActor.Command, MatchResult>ask(actor,
                    replyTo -> new SymbolActor.PlaceOrder(order("B", "key", "user", Side.BUY, 10000, 5), replyTo),
                    Duration.ofSeconds(5), system.scheduler()).toCompletableFuture();
            FaultInjectionBarrier.release("journal-before-force");
            assertEquals("A", first.get(5, TimeUnit.SECONDS).getFinalOrder().getOrderId());
            assertEquals("A", retry.get(5, TimeUnit.SECONDS).getFinalOrder().getOrderId());
            assertEquals(1, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        } finally {
            FaultInjectionBarrier.release("journal-before-force");
            System.clearProperty("matching.test.signalDir");
            System.clearProperty("matching.test.faultPoint");
            System.clearProperty("matching.test.releasable");
            stop(system);
        }
    }

    @Test void filledAndCancelledOrdersRemainIdempotentAfterSnapshotAndRestart() throws Exception {
        var first = system("idem-snapshot-first"); var actor = actor(first, 2);
        place(first, actor, order("BUY", "buy-key", "u1", Side.BUY, 10000, 5));
        place(first, actor, order("SELL", "sell-key", "u2", Side.SELL, 10000, 3));
        assertTrue(book(first, actor).stream().anyMatch(s -> s.contains("id=BUY") && s.contains("remain=2")));
        assertEquals(OrderStatus.FILLED, place(first, actor,
                order("SELL-RETRY", "sell-key", "u2", Side.SELL, 10000, 3)).getFinalOrder().getStatus());
        assertTrue(cancel(first, actor, "BUY").success());
        assertFalse(cancel(first, actor, "BUY").success());
        stop(first);

        var second = system("idem-snapshot-second"); var recovered = actor(second, 2);
        MatchResult buyRetry = place(second, recovered, order("BUY-RETRY", "buy-key", "u1", Side.BUY, 10000, 5));
        assertEquals("BUY", buyRetry.getFinalOrder().getOrderId());
        assertEquals(OrderStatus.CANCELED, buyRetry.getFinalOrder().getStatus());
        assertEquals(2, buyRetry.getFinalOrder().getRemainingQuantity());
        assertEquals("SELL", place(second, recovered,
                order("SELL-AGAIN", "sell-key", "u2", Side.SELL, 10000, 3)).getFinalOrder().getOrderId());
        assertFalse(book(second, recovered).stream().anyMatch(s -> s.contains("id=BUY") || s.contains("id=SELL")));
        assertEquals(3, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(second);
    }

    @Test void duplicateServerOrderIdCannotOverwriteOriginalIndex() throws Exception {
        var system = system("idem-server-id"); var actor = actor(system, 1000);
        place(system, actor, order("FIXED", "one", "user", Side.BUY, 10000, 5));
        MatchResult rejected = place(system, actor, order("FIXED", "two", "user", Side.BUY, 10000, 7));
        assertTrue(rejected.getMessage().startsWith("ORDER_ID_CONFLICT:"));
        assertTrue(cancel(system, actor, "FIXED").success());
        assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(system);
    }

    @Test void duplicateServerOrderIdAfterFullFillAndCompactionIsRejected() throws Exception {
        var first = system("server-id-filled-first"); var actor = actor(first, 2);
        place(first, actor, order("FIXED", "fixed-key", "u1", Side.BUY, 10000, 2));
        place(first, actor, order("TAKER", "taker-key", "u2", Side.SELL, 10000, 2));
        Path snapshot = data.resolve(SymbolKeyUtil.toPersistenceStem("BTCUSDT") + ".snapshot.json");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!Files.exists(snapshot) && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(Files.exists(snapshot));
        stop(first);
        var second = system("server-id-filled-second"); var recovered = actor(second, 2);
        MatchResult rejected = place(second, recovered,
                order("FIXED", "other-key", "u3", Side.BUY, 10000, 7));
        assertTrue(rejected.getMessage().startsWith("ORDER_ID_CONFLICT:"));
        assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        assertEquals(0, book(second, recovered).stream().filter(s -> s.contains("id=FIXED")).count());
        stop(second);
    }

    @Test void partialFillRetryCancelAndRecoveryPreserveRemainingAndIdentity() throws Exception {
        var first = system("partial-cancel-first"); var actor = actor(first, 1000);
        assertEquals("BUY", place(first, actor,
                order("BUY", "buy-key", "buyer", Side.BUY, 10000, 5)).getFinalOrder().getOrderId());
        MatchResult sell = place(first, actor,
                order("SELL", "sell-key", "seller", Side.SELL, 10000, 3));
        assertEquals(1, sell.getTrades().size());
        assertEquals(3, sell.getTrades().get(0).getQuantity());
        assertEquals(OrderStatus.FILLED, sell.getFinalOrder().getStatus());
        assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(first);

        var second = system("partial-cancel-second"); var recovered = actor(second, 1000);
        MatchResult retry = place(second, recovered,
                order("BUY-RETRY", "buy-key", "buyer", Side.BUY, 10000, 5));
        assertEquals("BUY", retry.getFinalOrder().getOrderId());
        assertEquals(OrderStatus.PARTIALLY_FILLED, retry.getFinalOrder().getStatus());
        assertEquals(2, retry.getFinalOrder().getRemainingQuantity());
        assertTrue(retry.getTrades().isEmpty());
        assertTrue(cancel(second, recovered, "BUY").success());
        assertFalse(cancel(second, recovered, "BUY").success());
        assertEquals(3, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(second);

        var third = system("partial-cancel-third"); var again = actor(third, 1000);
        MatchResult afterCancel = place(third, again,
                order("BUY-AGAIN", "buy-key", "buyer", Side.BUY, 10000, 5));
        assertEquals("BUY", afterCancel.getFinalOrder().getOrderId());
        assertEquals(OrderStatus.CANCELED, afterCancel.getFinalOrder().getStatus());
        assertEquals(2, afterCancel.getFinalOrder().getRemainingQuantity());
        assertTrue(afterCancel.getTrades().isEmpty());
        assertEquals(0, book(third, again).stream().filter(s -> s.contains("id=BUY")).count());
        assertEquals(3, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(third);
    }

    @Test void invalidOrderIsRejectedBeforeJournalAppend() throws Exception {
        var system = system("invalid-order"); var actor = actor(system, 1000);
        MatchResult invalid = place(system, actor,
                order("INVALID", "invalid-key", "buyer", Side.BUY, 10000, 0));
        assertTrue(invalid.getMessage().startsWith("INVALID_ORDER:"));
        assertEquals(0, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        assertTrue(book(system, actor).stream().noneMatch(s -> s.contains("id=INVALID")));
        stop(system);
    }

    @Test void missingClientIdentityAndOutOfRangeValuesNeverAdvanceJournal() throws Exception {
        var system = system("required-identity"); var actor = actor(system, 1000);
        assertTrue(place(system, actor, order("NO-KEY", null, "buyer", Side.BUY, 10000, 1))
                .getMessage().startsWith("INVALID_ORDER:"));
        assertTrue(place(system, actor, order("NO-USER", "key", " ", Side.BUY, 10000, 1))
                .getMessage().startsWith("INVALID_ORDER:"));
        assertTrue(place(system, actor, order("PRICE", "price-key", "buyer", Side.BUY, 1_000_000_001L, 1))
                .getMessage().startsWith("INVALID_ORDER:"));
        assertTrue(place(system, actor, order("QUANTITY", "qty-key", "buyer", Side.BUY, 10000, 1_000_000_001L))
                .getMessage().startsWith("INVALID_ORDER:"));
        assertEquals(0, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        assertTrue(book(system, actor).stream().noneMatch(s -> s.contains("id=")));
        stop(system);
    }

    @Test void tradeIdsUseSymbolSequenceAndMatchOrdinalAcrossReplayAndRestart() throws Exception {
        var first = system("trade-ids-first"); var actor = actor(first, 1000);
        place(first, actor, order("B1", "b1", "buyer", Side.BUY, 10000, 2));
        place(first, actor, order("B2", "b2", "buyer", Side.BUY, 10000, 2));
        MatchResult matched = place(first, actor, order("S1", "s1", "seller", Side.SELL, 10000, 3));
        assertEquals(2, matched.getTrades().size());
        assertEquals("T:QlRDVVNEVA:3:1", matched.getTrades().get(0).getTradeId());
        assertEquals("T:QlRDVVNEVA:3:2", matched.getTrades().get(1).getTradeId());
        String firstTradeId = matched.getTrades().get(0).getTradeId();
        stop(first);

        var recoveredData = new OrderEventStore(data, "BTCUSDT").loadRecoveryState();
        var replayed = new com.example.matching.engine.OrderBook();
        MatchResult replayResult = null;
        for (var event : recoveredData.events()) {
            if (event.getType() == com.example.matching.persistence.OrderEvent.Type.PLACE) {
                replayResult = replayed.place(event.toOrder(), event.getSequence());
            }
        }
        assertEquals(firstTradeId, replayResult.getTrades().get(0).getTradeId());
        assertEquals(1, replayed.openOrderCount());

        var second = system("trade-ids-second"); var restored = actor(second, 1000);
        MatchResult next = place(second, restored, order("S2", "s2", "seller", Side.SELL, 10000, 1));
        assertEquals("T:QlRDVVNEVA:4:1", next.getTrades().get(0).getTradeId());
        assertNotEquals(firstTradeId, next.getTrades().get(0).getTradeId());
        stop(second);
        assertEquals(4, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
    }

    @Test void capacityRejectsNewOrdersBeforeJournalButPreservesRetriesAfterRestart() throws Exception {
        var first = system("capacity-first");
        var actor = first.systemActorOf(SymbolActor.create("BTCUSDT", true, data, 2, 64, 2),
                "capacity-symbol", Props.empty());
        assertEquals("A", place(first, actor, order("A", "a", "u", Side.BUY, 10000, 1)).getFinalOrder().getOrderId());
        assertEquals("B", place(first, actor, order("B", "b", "u", Side.BUY, 9900, 1)).getFinalOrder().getOrderId());
        assertTrue(place(first, actor, order("C", "c", "u", Side.BUY, 9800, 1))
                .getMessage().startsWith("SERVICE_UNAVAILABLE: historical order capacity exceeded"));
        assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(first);

        var second = system("capacity-second");
        var restored = second.systemActorOf(SymbolActor.create("BTCUSDT", true, data, 2, 64, 2),
                "capacity-symbol", Props.empty());
        assertEquals("A", place(second, restored, order("RETRY", "a", "u", Side.BUY, 10000, 1))
                .getFinalOrder().getOrderId());
        assertTrue(place(second, restored, order("D", "d", "u", Side.BUY, 9700, 1))
                .getMessage().startsWith("SERVICE_UNAVAILABLE: historical order capacity exceeded"));
        assertEquals(2, new OrderEventStore(data, "BTCUSDT").loadRecoveryState().lastSequence());
        stop(second);
    }
}
