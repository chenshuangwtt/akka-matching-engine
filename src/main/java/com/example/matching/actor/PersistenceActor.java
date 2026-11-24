package com.example.matching.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

import com.example.matching.domain.Order;
import com.example.matching.persistence.OrderBookSnapshot;
import com.example.matching.persistence.OrderEventStore;

import java.nio.file.Path;

public class PersistenceActor extends AbstractBehavior<PersistenceActor.Command> {

    public sealed interface Command permits AppendPlace, AppendCancel, Compact, LoadRecovery {}

    public sealed interface Response
            permits RecoveryLoaded, RecoveryFailed, WriteSucceeded, WriteFailed {}

    public record AppendPlace(
            long requestId, Order order, akka.actor.typed.ActorRef<Response> replyTo)
            implements Command {}

    public record AppendCancel(
            long requestId,
            String symbol,
            String orderId,
            akka.actor.typed.ActorRef<Response> replyTo)
            implements Command {}

    public record Compact(OrderBookSnapshot snapshot) implements Command {}

    public record LoadRecovery(akka.actor.typed.ActorRef<Response> replyTo) implements Command {}

    public record RecoveryLoaded(OrderEventStore.RecoveryState state) implements Response {}

    public record RecoveryFailed(String message) implements Response {}

    public record WriteSucceeded(long requestId, long sequence) implements Response {}

    public record WriteFailed(long requestId, String message) implements Response {}

    public static Behavior<Command> create(
            Path persistenceDir, String symbol, int snapshotThreshold) {
        return Behaviors.setup(
                ctx -> new PersistenceActor(ctx, persistenceDir, symbol, snapshotThreshold));
    }

    private final String symbol;
    private final OrderEventStore eventStore;

    private PersistenceActor(
            ActorContext<Command> context,
            Path persistenceDir,
            String symbol,
            int snapshotThreshold) {
        super(context);
        this.symbol = symbol;
        this.eventStore = new OrderEventStore(persistenceDir, symbol, snapshotThreshold);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(AppendPlace.class, this::onAppendPlace)
                .onMessage(AppendCancel.class, this::onAppendCancel)
                .onMessage(Compact.class, this::onCompact)
                .onMessage(LoadRecovery.class, this::onLoadRecovery)
                .build();
    }

    private Behavior<Command> onAppendPlace(AppendPlace cmd) {
        try {
            long sequence = eventStore.appendPlace(cmd.order());
            cmd.replyTo().tell(new WriteSucceeded(cmd.requestId(), sequence));
        } catch (RuntimeException ex) {
            getContext().getLog().error("Journal append failed for {}", symbol, ex);
            cmd.replyTo().tell(new WriteFailed(cmd.requestId(), ex.getMessage()));
        }
        return this;
    }

    private Behavior<Command> onAppendCancel(AppendCancel cmd) {
        try {
            long sequence = eventStore.appendCancel(symbol, cmd.orderId());
            cmd.replyTo().tell(new WriteSucceeded(cmd.requestId(), sequence));
        } catch (RuntimeException ex) {
            getContext().getLog().error("Journal append failed for {}", symbol, ex);
            cmd.replyTo().tell(new WriteFailed(cmd.requestId(), ex.getMessage()));
        }
        return this;
    }

    private Behavior<Command> onCompact(Compact cmd) {
        try {
            eventStore.compact(cmd.snapshot());
            getContext()
                    .getLog()
                    .info(
                            "Snapshot committed for {}, sequence={}",
                            symbol,
                            cmd.snapshot().getLastAppliedSequence());
        } catch (RuntimeException ex) {
            getContext().getLog().error("Snapshot commit failed for {}", symbol, ex);
            throw ex;
        }
        return this;
    }

    private Behavior<Command> onLoadRecovery(LoadRecovery cmd) {
        try {
            cmd.replyTo().tell(new RecoveryLoaded(eventStore.loadRecoveryState()));
        } catch (RuntimeException ex) {
            getContext().getLog().error("Failed to load recovery state for symbol " + symbol, ex);
            cmd.replyTo().tell(new RecoveryFailed(ex.getMessage()));
        }
        return this;
    }
}
