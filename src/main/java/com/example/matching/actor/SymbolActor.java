package com.example.matching.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.StashBuffer;
import akka.actor.typed.javadsl.TimerScheduler;
import com.example.matching.domain.Order;
import com.example.matching.domain.OrderRules;
import com.example.matching.engine.MatchResult;
import com.example.matching.engine.OrderBook;
import com.example.matching.persistence.OrderBookSnapshot;
import com.example.matching.persistence.FaultInjectionBarrier;
import com.example.matching.util.MdcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class SymbolActor {

    private static final Logger log = LoggerFactory.getLogger(SymbolActor.class);
    private static final AtomicLong nextWriteRequestId = new AtomicLong();

    public sealed interface Command permits PlaceOrder, CancelOrder, GetBook, PlaceOrderNoReply, GetProcessedCount, RecoveryLoaded, RecoveryFailed, WriteSucceeded, WriteFailed, WriteTimedOut, RecoveryTimedOut, PersistenceStopped {
    }

    public record PlaceOrder(Order order, String requestId, ActorRef<MatchResult> replyTo) implements Command {
        public PlaceOrder(Order order, ActorRef<MatchResult> replyTo) {
            this(order, "", replyTo);
        }
    }

    public record CancelOrder(String orderId, String requestId, ActorRef<CancelResult> replyTo) implements Command {
        public CancelOrder(String orderId, ActorRef<CancelResult> replyTo) {
            this(orderId, "", replyTo);
        }
    }

    public record CancelResult(boolean success, String message) {}

    public record GetBook(String requestId, ActorRef<List<String>> replyTo) implements Command {
        public GetBook(ActorRef<List<String>> replyTo) {
            this("", replyTo);
        }
    }

    public record PlaceOrderNoReply(Order order) implements Command {
    }

    public record GetProcessedCount(ActorRef<Long> replyTo) implements Command {
    }

    private record RecoveryLoaded(PersistenceActor.RecoveryLoaded loaded) implements Command {
    }

    private record RecoveryFailed(PersistenceActor.RecoveryFailed failed) implements Command {
    }

    private record WriteSucceeded(PersistenceActor.WriteSucceeded result) implements Command {}
    private record WriteFailed(PersistenceActor.WriteFailed result) implements Command {}
    private record WriteTimedOut(long requestId) implements Command {}
    private record RecoveryTimedOut() implements Command {}
    private record PersistenceStopped() implements Command {}
    private record Pending(long requestId, Command command) {}

    private record State(String symbol,
                         OrderBook orderBook,
                         ActorRef<PersistenceActor.Command> persistenceActor,
                         ActorRef<PersistenceActor.Response> persistenceReplyTo,
                         int snapshotThreshold,
                         int compactLowLoadOpenOrders,
                         long processedCount,
                         int pendingEventCount,
                         long lastSequence) {

        private State withProcessedCount(long value) {
            return new State(symbol, orderBook, persistenceActor, persistenceReplyTo, snapshotThreshold, compactLowLoadOpenOrders, value, pendingEventCount, lastSequence);
        }

        private State withPendingEventCount(int value) {
            return new State(symbol, orderBook, persistenceActor, persistenceReplyTo, snapshotThreshold, compactLowLoadOpenOrders, processedCount, value, lastSequence);
        }

        private State withLastSequence(long value) {
            return new State(symbol, orderBook, persistenceActor, persistenceReplyTo, snapshotThreshold, compactLowLoadOpenOrders, processedCount, pendingEventCount, value);
        }

        private State incrementProcessedCount() {
            return withProcessedCount(processedCount + 1);
        }

        private State incrementPendingEvents() {
            return withPendingEventCount(pendingEventCount + 1);
        }
    }

    public static Behavior<Command> create(String symbol) {
        return create(symbol, false, null, 1_000, 64);
    }

    public static Behavior<Command> create(String symbol, boolean persistenceEnabled, Path persistenceDir) {
        return create(symbol, persistenceEnabled, persistenceDir, 1_000, 64);
    }

    public static Behavior<Command> create(String symbol,
                                           boolean persistenceEnabled,
                                           Path persistenceDir,
                                           int snapshotThreshold) {
        return create(symbol, persistenceEnabled, persistenceDir, snapshotThreshold, 64);
    }

    public static Behavior<Command> create(String symbol,
                                           boolean persistenceEnabled,
                                           Path persistenceDir,
                                           int snapshotThreshold,
                                           int compactLowLoadOpenOrders) {
        return create(symbol, persistenceEnabled, persistenceDir, snapshotThreshold,
                compactLowLoadOpenOrders, OrderBook.DEFAULT_MAX_HISTORICAL_ORDERS);
    }

    public static Behavior<Command> create(String symbol,
                                           boolean persistenceEnabled,
                                           Path persistenceDir,
                                           int snapshotThreshold,
                                           int compactLowLoadOpenOrders,
                                           int maxHistoricalOrders) {
        if (snapshotThreshold <= 0 || compactLowLoadOpenOrders < 0) {
            throw new IllegalArgumentException("snapshot threshold must be positive and low-load order count nonnegative");
        }
        return Behaviors.withStash(10_000, stash -> Behaviors.withTimers(timers ->
                Behaviors.setup(context -> {
                    OrderBook orderBook = new OrderBook(maxHistoricalOrders);
                    ActorRef<PersistenceActor.Command> persistenceActor = persistenceEnabled && persistenceDir != null
                            ? context.spawn(
                            PersistenceActor.create(persistenceDir, symbol, snapshotThreshold),
                            "persistence",
                            // 文件 I/O 和撮合线程隔离，避免持久化拖慢撮合路径。
                            DispatcherSelector.fromConfig("akka.actor.persistence-dispatcher")
                    )
                            : null;
                    if (persistenceActor != null) context.watchWith(persistenceActor, new PersistenceStopped());

                    ActorRef<PersistenceActor.Response> adapter = context.messageAdapter(PersistenceActor.Response.class, response -> {
                        if (response instanceof PersistenceActor.RecoveryLoaded loaded) return new RecoveryLoaded(loaded);
                        if (response instanceof PersistenceActor.RecoveryFailed failed) return new RecoveryFailed(failed);
                        if (response instanceof PersistenceActor.WriteSucceeded written) return new WriteSucceeded(written);
                        return new WriteFailed((PersistenceActor.WriteFailed) response);
                    });
                    State state = new State(
                            symbol,
                            orderBook,
                            persistenceActor,
                            adapter,
                            snapshotThreshold,
                            compactLowLoadOpenOrders,
                            0L,
                            0,
                            0L
                    );

                    if (persistenceActor == null) {
                        return running(stash, timers, state);
                    }

                    // 恢复流程异步执行；恢复完成前收到的新消息先暂存。
                    persistenceActor.tell(new PersistenceActor.LoadRecovery(adapter));
                    timers.startSingleTimer(new RecoveryTimedOut(), Duration.ofSeconds(60));
                    return loading(stash, timers, state);
                })
        ));
    }

    private static Behavior<Command> loading(StashBuffer<Command> stash, TimerScheduler<Command> timers, State state) {
        return Behaviors.receive(Command.class)
                .onMessage(RecoveryLoaded.class, cmd -> {
                    timers.cancel(new RecoveryTimedOut());
                    try {
                        State recovered = applyRecovery(state, cmd.loaded().state());
                        return stash.unstashAll(running(stash, timers, recovered));
                    } catch (RuntimeException ex) {
                        return stash.unstashAll(faulted(stash, state, "recovery replay failed: " + ex.getMessage()));
                    }
                })
                .onMessage(RecoveryFailed.class, cmd -> { timers.cancel(new RecoveryTimedOut()); return stash.unstashAll(faulted(stash, state, "recovery failed: " + cmd.failed().message())); })
                .onMessage(RecoveryTimedOut.class, cmd -> stash.unstashAll(faulted(stash, state, "recovery timed out")))
                .onMessage(PersistenceStopped.class, cmd -> stash.unstashAll(faulted(stash, state, "persistence actor stopped during recovery")))
                .onAnyMessage(cmd -> {
                    stash.stash(cmd);
                    return Behaviors.same();
                })
                .build();
    }

    private static Behavior<Command> running(StashBuffer<Command> stash, TimerScheduler<Command> timers, State state) {
        return Behaviors.receive(Command.class)
                .onMessage(PlaceOrder.class, cmd -> {
                    try (MdcUtil.Scope ignored = MdcUtil.withRequest(cmd.requestId())) {
                        log.info("SymbolActor placeOrder: symbol={}, orderId={}", state.symbol(),
                                cmd.order() == null ? null : cmd.order().getOrderId());
                    }
                    try {
                        OrderBook.validateNewOrder(cmd.order());
                        if (!state.symbol().equals(cmd.order().getSymbol())) {
                            throw new IllegalArgumentException("order symbol does not match actor symbol");
                        }
                    } catch (IllegalArgumentException ex) {
                        MatchResult result = new MatchResult();
                        result.setMessage("INVALID_ORDER: " + ex.getMessage());
                        cmd.replyTo().tell(result);
                        return Behaviors.same();
                    }
                    Order existing = state.orderBook().findByClientOrderId(
                            cmd.order().getUserId(), cmd.order().getClientOrderId());
                    if (existing != null) {
                        MatchResult result = new MatchResult();
                        if (existing.sameBusinessRequest(cmd.order())) {
                            result.setFinalOrder(existing.copy());
                            result.setBooked(state.orderBook().hasOpenOrder(existing.getOrderId()));
                            result.setMessage("already accepted; status=" + existing.getStatus()
                                    + "; remaining=" + existing.getRemainingQuantity());
                        } else {
                            result.setMessage("IDEMPOTENCY_CONFLICT: clientOrderId was used with different order parameters");
                        }
                        cmd.replyTo().tell(result);
                        return Behaviors.same();
                    }
                    if (state.orderBook().hasSeenOrderId(cmd.order().getOrderId())) {
                        MatchResult result = new MatchResult();
                        result.setMessage("ORDER_ID_CONFLICT: server orderId already exists");
                        cmd.replyTo().tell(result);
                        return Behaviors.same();
                    }
                    if (state.orderBook().isAtCapacity()) {
                        reject(cmd, "historical order capacity exceeded; max="
                                + state.orderBook().maxHistoricalOrders());
                        log.error("Symbol {} historical order capacity exceeded: count={}",
                                state.symbol(), state.orderBook().historicalOrderCount());
                        return Behaviors.same();
                    }
                    if (state.persistenceActor() == null) return completePlace(stash, timers, state, cmd);
                    long id = nextWriteRequestId.incrementAndGet();
                    state.persistenceActor().tell(new PersistenceActor.AppendPlace(id, cmd.order(), state.persistenceReplyTo()));
                    timers.startSingleTimer(new WriteTimedOut(id), Duration.ofSeconds(10));
                    return awaiting(stash, timers, state, new Pending(id, cmd));
                })
                .onMessage(CancelOrder.class, cmd -> {
                    try (MdcUtil.Scope ignored = MdcUtil.withRequest(cmd.requestId())) {
                        log.info("SymbolActor cancelOrder: symbol={}, orderId={}", state.symbol(), cmd.orderId());
                    }
                    try {
                        OrderRules.validateId(cmd.orderId(), "orderId");
                    } catch (IllegalArgumentException ex) {
                        cmd.replyTo().tell(new CancelResult(false, "INVALID_ORDER: " + ex.getMessage()));
                        return Behaviors.same();
                    }
                    if (!state.orderBook().hasOpenOrder(cmd.orderId())) {
                        cmd.replyTo().tell(new CancelResult(false, "order not found"));
                        return Behaviors.same();
                    }
                    if (state.persistenceActor() == null) return completeCancel(stash, timers, state, cmd);
                    long id = nextWriteRequestId.incrementAndGet();
                    state.persistenceActor().tell(new PersistenceActor.AppendCancel(id, state.symbol(), cmd.orderId(), state.persistenceReplyTo()));
                    timers.startSingleTimer(new WriteTimedOut(id), Duration.ofSeconds(10));
                    return awaiting(stash, timers, state, new Pending(id, cmd));
                })
                .onMessage(GetBook.class, cmd -> {
                    try (MdcUtil.Scope ignored = MdcUtil.withRequest(cmd.requestId())) {
                        log.info("SymbolActor getBook: symbol={}", state.symbol());
                    }
                    cmd.replyTo().tell(state.orderBook().snapshot());
                    return Behaviors.same();
                })
                .onMessage(PlaceOrderNoReply.class, cmd -> {
                    state.orderBook().placeForBenchmark(cmd.order());
                    return running(stash, timers, state.incrementProcessedCount());
                })
                .onMessage(GetProcessedCount.class, cmd -> {
                    cmd.replyTo().tell(state.processedCount());
                    return Behaviors.same();
                })
                .onMessage(RecoveryLoaded.class, cmd -> Behaviors.same())
                .onMessage(RecoveryFailed.class, cmd -> Behaviors.same())
                .onMessage(WriteSucceeded.class, cmd -> Behaviors.same())
                .onMessage(WriteFailed.class, cmd -> Behaviors.same())
                .onMessage(WriteTimedOut.class, cmd -> Behaviors.same())
                .onMessage(RecoveryTimedOut.class, cmd -> Behaviors.same())
                .onMessage(PersistenceStopped.class, cmd -> faulted(stash, state, "persistence actor stopped"))
                .build();
    }

    private static Behavior<Command> awaiting(StashBuffer<Command> stash, TimerScheduler<Command> timers, State state, Pending pending) {
        return Behaviors.receive(Command.class)
                .onMessage(WriteSucceeded.class, cmd -> {
                    if (cmd.result().requestId() != pending.requestId()) return Behaviors.same();
                    timers.cancel(new WriteTimedOut(pending.requestId()));
                    Behavior<Command> next = pending.command() instanceof PlaceOrder place
                            ? completePlace(stash, timers, state.withLastSequence(cmd.result().sequence()), place)
                            : completeCancel(stash, timers, state.withLastSequence(cmd.result().sequence()), (CancelOrder) pending.command());
                    return stash.unstashAll(next);
                })
                .onMessage(WriteFailed.class, cmd -> {
                    if (cmd.result().requestId() != pending.requestId()) return Behaviors.same();
                    timers.cancel(new WriteTimedOut(pending.requestId()));
                    reject(pending.command(), "journal write failed; outcome unknown: " + cmd.result().message());
                    return stash.unstashAll(faulted(stash, state, "journal write failed; outcome unknown: " + cmd.result().message()));
                })
                .onMessage(WriteTimedOut.class, cmd -> {
                    if (cmd.requestId() != pending.requestId()) return Behaviors.same();
                    reject(pending.command(), "journal write timed out; outcome unknown");
                    return stash.unstashAll(faulted(stash, state, "journal write timed out; outcome unknown"));
                })
                .onMessage(PersistenceStopped.class, cmd -> {
                    timers.cancel(new WriteTimedOut(pending.requestId()));
                    reject(pending.command(), "persistence actor stopped; outcome unknown");
                    return stash.unstashAll(faulted(stash, state, "persistence actor stopped; outcome unknown"));
                })
                .onAnyMessage(cmd -> { stash.stash(cmd); return Behaviors.same(); })
                .build();
    }

    private static Behavior<Command> completePlace(StashBuffer<Command> stash, TimerScheduler<Command> timers, State state, PlaceOrder cmd) {
        MatchResult result = state.orderBook().place(cmd.order(), state.lastSequence());
        State next = state.incrementProcessedCount();
        if (state.persistenceActor() != null) next = maybeCompact(next.incrementPendingEvents());
        FaultInjectionBarrier.fatalAt("actor-fatal-before-reply", next.lastSequence());
        FaultInjectionBarrier.crashAt("actor-before-reply", next.lastSequence());
        FaultInjectionBarrier.stopAt("before-business-reply", next.lastSequence());
        cmd.replyTo().tell(result);
        return running(stash, timers, next);
    }

    private static Behavior<Command> completeCancel(StashBuffer<Command> stash, TimerScheduler<Command> timers, State state, CancelOrder cmd) {
        boolean success = state.orderBook().cancel(cmd.orderId());
        State next = state;
        if (success && state.persistenceActor() != null) next = maybeCompact(state.incrementPendingEvents());
        cmd.replyTo().tell(new CancelResult(success, success ? "ok" : "order not found"));
        return running(stash, timers, next);
    }

    private static void reject(Command cmd, String reason) {
        if (cmd instanceof PlaceOrder place) {
            MatchResult result = new MatchResult();
            result.setMessage("SERVICE_UNAVAILABLE: " + reason);
            place.replyTo().tell(result);
        } else if (cmd instanceof CancelOrder cancel) {
            cancel.replyTo().tell(new CancelResult(false, "SERVICE_UNAVAILABLE: " + reason));
        }
    }

    private static Behavior<Command> faulted(StashBuffer<Command> stash, State state, String reason) {
        log.error("Symbol {} unavailable: {}", state.symbol(), reason);
        return Behaviors.receive(Command.class)
                .onMessage(PlaceOrder.class, cmd -> { reject(cmd, reason); return Behaviors.same(); })
                .onMessage(CancelOrder.class, cmd -> { reject(cmd, reason); return Behaviors.same(); })
                .onMessage(GetBook.class, cmd -> { cmd.replyTo().tell(List.of("SERVICE_UNAVAILABLE: " + reason)); return Behaviors.same(); })
                .onMessage(GetProcessedCount.class, cmd -> { cmd.replyTo().tell(state.processedCount()); return Behaviors.same(); })
                .onAnyMessage(cmd -> Behaviors.same())
                .build();
    }

    private static State applyRecovery(State state, com.example.matching.persistence.OrderEventStore.RecoveryState recoveryState) {
        State next = state.withLastSequence(recoveryState.lastSequence());
        if (recoveryState.snapshot() != null) {
            next = next.withProcessedCount(recoveryState.snapshot().getProcessedCount());
            next.orderBook().restoreSnapshot(recoveryState.snapshot().getOpenOrders(),
                    recoveryState.snapshot().getAllOrders());
        }
        next = next.withPendingEventCount(recoveryState.events().size());

        for (var event : recoveryState.events()) {
            if (event.getType() == com.example.matching.persistence.OrderEvent.Type.PLACE) {
                next.orderBook().place(event.toOrder(), event.getSequence());
                next = next.incrementProcessedCount();
            } else if (event.getType() == com.example.matching.persistence.OrderEvent.Type.CANCEL) {
                next.orderBook().cancel(event.getOrderId());
            }
        }
        return next;
    }

    private static State maybeCompact(State state) {
        if (state.persistenceActor() != null && shouldCompactNow(state)) {
            OrderBookSnapshot snapshot = new OrderBookSnapshot();
            snapshot.setProcessedCount(state.processedCount());
            snapshot.setLastAppliedSequence(state.lastSequence());
            snapshot.setFormatVersion(com.example.matching.persistence.OrderEvent.FORMAT_VERSION);
            snapshot.setOpenOrders(state.orderBook().exportOpenOrders());
            snapshot.setAllOrders(state.orderBook().exportAllOrders());
            // 快照写入委托给持久化 Actor，避免撮合线程同步刷盘。
            state.persistenceActor().tell(new PersistenceActor.Compact(snapshot));
            return state.withPendingEventCount(0);
        }
        return state;
    }

    private static boolean shouldCompactNow(State state) {
        if (state.pendingEventCount() < state.snapshotThreshold()) {
            return false;
        }
        if (state.orderBook().openOrderCount() <= state.compactLowLoadOpenOrders()) {
            return true;
        }
        long forcedThreshold = Math.max((long) state.snapshotThreshold() * 4L,
                state.snapshotThreshold());
        return state.pendingEventCount() >= forcedThreshold;
    }
}
