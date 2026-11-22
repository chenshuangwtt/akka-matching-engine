package com.example.matching.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.example.matching.domain.Order;
import com.example.matching.engine.MatchResult;
import com.example.matching.util.MdcUtil;
import com.example.matching.util.SymbolKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EngineRootActor extends AbstractBehavior<EngineRootActor.Command> {

    private static final Logger log = LoggerFactory.getLogger(EngineRootActor.class);

    public sealed interface Command permits RoutePlaceOrder, RouteCancelOrder, RouteGetBook, SymbolStopped {}

    private record SymbolStopped(String symbol, ActorRef<SymbolActor.Command> actor) implements Command {}

    public record RoutePlaceOrder(Order order, String requestId, ActorRef<MatchResult> replyTo) implements Command {
        public RoutePlaceOrder(Order order, ActorRef<MatchResult> replyTo) {
            this(order, "", replyTo);
        }
    }
    public record RouteCancelOrder(String symbol, String orderId, String requestId, ActorRef<SymbolActor.CancelResult> replyTo) implements Command {
        public RouteCancelOrder(String symbol, String orderId, ActorRef<SymbolActor.CancelResult> replyTo) {
            this(symbol, orderId, "", replyTo);
        }
    }
    public record RouteGetBook(String symbol, String requestId, ActorRef<List<String>> replyTo) implements Command {
        public RouteGetBook(String symbol, ActorRef<List<String>> replyTo) {
            this(symbol, "", replyTo);
        }
    }

    public static Behavior<Command> create() {
        return create(false, null, 1_000, 64);
    }

    public static Behavior<Command> create(boolean persistenceEnabled, Path persistenceDir) {
        return create(persistenceEnabled, persistenceDir, 1_000, 64);
    }

    public static Behavior<Command> create(boolean persistenceEnabled,
                                           Path persistenceDir,
                                           int snapshotThreshold) {
        return create(persistenceEnabled, persistenceDir, snapshotThreshold, 64);
    }

    public static Behavior<Command> create(boolean persistenceEnabled,
                                           Path persistenceDir,
                                           int snapshotThreshold,
                                           int compactLowLoadOpenOrders) {
        return create(
                persistenceEnabled,
                persistenceDir,
                snapshotThreshold,
                compactLowLoadOpenOrders,
                List.of()
        );
    }

    public static Behavior<Command> create(boolean persistenceEnabled,
                                           Path persistenceDir,
                                           int snapshotThreshold,
                                           int compactLowLoadOpenOrders,
                                           List<String> preloadSymbols) {
        return create(persistenceEnabled, persistenceDir, snapshotThreshold,
                compactLowLoadOpenOrders, preloadSymbols,
                com.example.matching.engine.OrderBook.DEFAULT_MAX_HISTORICAL_ORDERS);
    }

    public static Behavior<Command> create(boolean persistenceEnabled,
                                           Path persistenceDir,
                                           int snapshotThreshold,
                                           int compactLowLoadOpenOrders,
                                           List<String> preloadSymbols,
                                           int maxHistoricalOrders) {
        return Behaviors.setup(ctx -> new EngineRootActor(
                ctx,
                persistenceEnabled,
                persistenceDir,
                snapshotThreshold,
                compactLowLoadOpenOrders,
                preloadSymbols,
                maxHistoricalOrders
        ));
    }

    private final Map<String, ActorRef<SymbolActor.Command>> symbolActors = new HashMap<>();
    private final boolean persistenceEnabled;
    private final Path persistenceDir;
    private final int snapshotThreshold;
    private final int compactLowLoadOpenOrders;
    private final int maxHistoricalOrders;

    private EngineRootActor(ActorContext<Command> context,
                            boolean persistenceEnabled,
                            Path persistenceDir,
                            int snapshotThreshold,
                            int compactLowLoadOpenOrders,
                            List<String> preloadSymbols,
                            int maxHistoricalOrders) {
        super(context);
        this.persistenceEnabled = persistenceEnabled;
        this.persistenceDir = persistenceDir;
        this.snapshotThreshold = snapshotThreshold;
        this.compactLowLoadOpenOrders = compactLowLoadOpenOrders;
        this.maxHistoricalOrders = maxHistoricalOrders;
        preload(preloadSymbols);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(RoutePlaceOrder.class, this::onRoutePlaceOrder)
                .onMessage(RouteCancelOrder.class, this::onRouteCancelOrder)
                .onMessage(RouteGetBook.class, this::onRouteGetBook)
                .onMessage(SymbolStopped.class, this::onSymbolStopped)
                .build();
    }

    private Behavior<Command> onRoutePlaceOrder(RoutePlaceOrder cmd) {
        try (MdcUtil.Scope ignored = MdcUtil.withRequest(cmd.requestId())) {
            log.info("Route placeOrder: symbol={}, orderId={}", cmd.order().getSymbol(), cmd.order().getOrderId());
        }
        symbolActor(cmd.order().getSymbol()).tell(new SymbolActor.PlaceOrder(cmd.order(), cmd.requestId(), cmd.replyTo()));
        return this;
    }

    private Behavior<Command> onRouteCancelOrder(RouteCancelOrder cmd) {
        try (MdcUtil.Scope ignored = MdcUtil.withRequest(cmd.requestId())) {
            log.info("Route cancelOrder: symbol={}, orderId={}", cmd.symbol(), cmd.orderId());
        }
        symbolActor(cmd.symbol()).tell(new SymbolActor.CancelOrder(cmd.orderId(), cmd.requestId(), cmd.replyTo()));
        return this;
    }

    private Behavior<Command> onRouteGetBook(RouteGetBook cmd) {
        try (MdcUtil.Scope ignored = MdcUtil.withRequest(cmd.requestId())) {
            log.info("Route getBook: symbol={}", cmd.symbol());
        }
        symbolActor(cmd.symbol()).tell(new SymbolActor.GetBook(cmd.requestId(), cmd.replyTo()));
        return this;
    }

    private ActorRef<SymbolActor.Command> symbolActor(String symbol) {
        return symbolActors.computeIfAbsent(symbol, s -> {
            ActorRef<SymbolActor.Command> child = getContext().spawn(
                        Behaviors.supervise(SymbolActor.create(s, persistenceEnabled, persistenceDir,
                                snapshotThreshold, compactLowLoadOpenOrders, maxHistoricalOrders)).onFailure(Exception.class, SupervisorStrategy.restart()),
                        SymbolKeyUtil.toActorName(s),
                        DispatcherSelector.fromConfig("akka.actor.matching-dispatcher")
                );
            getContext().watchWith(child, new SymbolStopped(s, child));
            return child;
        });
    }

    private Behavior<Command> onSymbolStopped(SymbolStopped stopped) {
        if (symbolActors.remove(stopped.symbol(), stopped.actor())) {
            log.error("Symbol actor stopped; next request will recover symbol {}", stopped.symbol());
        }
        return this;
    }

    private void preload(List<String> preloadSymbols) {
        for (String symbol : sanitizePreloadSymbols(preloadSymbols)) {
            symbolActor(symbol);
        }
    }

    private List<String> sanitizePreloadSymbols(List<String> preloadSymbols) {
        if (preloadSymbols == null || preloadSymbols.isEmpty()) {
            return List.of();
        }

        List<String> sanitized = new ArrayList<>();
        for (String symbol : preloadSymbols) {
            if (symbol == null) {
                continue;
            }
            String trimmed = symbol.trim();
            if (!trimmed.isEmpty() && !sanitized.contains(trimmed)) {
                sanitized.add(trimmed);
            }
        }
        return Collections.unmodifiableList(sanitized);
    }
}
