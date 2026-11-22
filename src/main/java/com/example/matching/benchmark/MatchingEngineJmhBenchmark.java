package com.example.matching.benchmark;

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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class MatchingEngineJmhBenchmark {

    // 基线场景：纯撮合逻辑，包含 MatchResult / Trade 对象分配。
    @Benchmark
    public void orderBook(OrderBatchState state, Blackhole blackhole) {
        OrderBook orderBook = new OrderBook();
        long totalTrades = 0L;

        for (OrderSeed seed : state.seeds) {
            MatchResult result = orderBook.place(seed.toOrder());
            totalTrades += result.getTrades().size();
        }

        blackhole.consume(totalTrades);
        blackhole.consume(orderBook.snapshot().size());
    }

    // 基线场景：纯撮合逻辑，但不包含 MatchResult / Trade 对象分配。
    @Benchmark
    public void orderBookLight(OrderBatchState state, Blackhole blackhole) {
        OrderBook orderBook = new OrderBook();

        for (OrderSeed seed : state.seeds) {
            orderBook.placeForBenchmark(seed.toOrder());
        }

        blackhole.consume(orderBook.snapshot().size());
    }

    // 单一热点 symbol 的 Actor 开销，不走持久化写路径。
    @Benchmark
    public void singleSymbolActor(ActorBatchState state, Blackhole blackhole) throws Exception {
        for (OrderSeed seed : state.seeds) {
            state.singleActor.tell(new SymbolActor.PlaceOrderNoReply(seed.toOrder()));
        }

        blackhole.consume(waitUntilProcessed(state.system, state.singleActor, state.seeds.size()));
    }

    // 同样的 Actor 路径，但会额外走持久化写入，并委托给 persistence-dispatcher。
    @Benchmark
    public void singleSymbolActorWithPersistence(PersistentActorBatchState state, Blackhole blackhole) throws Exception {
        for (OrderSeed seed : state.seeds) {
            state.singleActor.tell(new SymbolActor.PlaceOrder(seed.toOrder(), state.replyProbe));
        }

        blackhole.consume(waitUntilProcessed(state.system, state.singleActor, state.seeds.size()));
    }

    // 多 symbol 分片场景，用来观察按 symbol 拆 Actor 后的并行收益。
    @Benchmark
    public void multiSymbolActor(MultiActorBatchState state, Blackhole blackhole) throws Exception {
        for (OrderSeed seed : state.seeds) {
            state.actorMap.get(seed.symbol()).tell(new SymbolActor.PlaceOrderNoReply(seed.toOrder()));
        }

        blackhole.consume(waitUntilProcessed(state.system, state.actorMap, state.seeds.size()));
    }

    @State(Scope.Thread)
    public static class OrderBatchState {
        @Param({"1000", "10000"})
        public int orders;

        @Param({"100"})
        public int priceLevels;

        @Param({"true", "false"})
        public boolean cross;

        public List<OrderSeed> seeds;

        @Setup(Level.Trial)
        public void setup() {
            seeds = OrderSeed.generate(orders, 1, priceLevels, cross);
        }
    }

    @State(Scope.Thread)
    public static class ActorBatchState {
        @Param({"1000"})
        public int orders;

        @Param({"100"})
        public int priceLevels;

        @Param({"true"})
        public boolean cross;

        public List<OrderSeed> seeds;
        public ActorSystem<Void> system;
        public ActorRef<SymbolActor.Command> singleActor;

        @Setup(Level.Invocation)
        public void setup() {
            seeds = OrderSeed.generate(orders, 1, priceLevels, cross);
            system = ActorSystem.create(Behaviors.empty(), "jmh-single-symbol-" + System.nanoTime());
            singleActor = system.systemActorOf(
                    SymbolActor.create("SYM0", false, null),
                    "symbol-sym0",
                    // benchmark 场景使用更激进的 dispatcher 配置。
                    Props.empty().withDispatcherFromConfig("akka.actor.benchmark-dispatcher")
            );
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            system.terminate();
        }
    }

    @State(Scope.Thread)
    public static class MultiActorBatchState {
        @Param({"1000"})
        public int orders;

        @Param({"4", "16"})
        public int symbols;

        @Param({"100"})
        public int priceLevels;

        @Param({"true"})
        public boolean cross;

        public List<OrderSeed> seeds;
        public ActorSystem<Void> system;
        public Map<String, ActorRef<SymbolActor.Command>> actorMap;

        @Setup(Level.Invocation)
        public void setup() {
            seeds = OrderSeed.generate(orders, symbols, priceLevels, cross);
            system = ActorSystem.create(Behaviors.empty(), "jmh-multi-symbol-" + System.nanoTime());
            actorMap = new HashMap<>();

            for (int i = 0; i < symbols; i++) {
                String symbol = "SYM" + i;
                actorMap.put(
                        symbol,
                        system.systemActorOf(
                                SymbolActor.create(symbol, false, null),
                                "symbol-" + symbol.toLowerCase(),
                                Props.empty().withDispatcherFromConfig("akka.actor.benchmark-dispatcher")
                        )
                );
            }
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            system.terminate();
        }
    }

    @State(Scope.Thread)
    public static class PersistentActorBatchState {
        @Param({"1000"})
        public int orders;

        @Param({"100"})
        public int priceLevels;

        @Param({"true"})
        public boolean cross;

        public List<OrderSeed> seeds;
        public ActorSystem<Void> system;
        public ActorRef<SymbolActor.Command> singleActor;
        public ActorRef<MatchResult> replyProbe;
        public Path persistenceDir;

        @Setup(Level.Invocation)
        public void setup() throws IOException {
            seeds = OrderSeed.generate(orders, 1, priceLevels, cross);
            Path benchmarkRoot = Path.of("target", "p3-jmh-data").toAbsolutePath().normalize();
            Files.createDirectories(benchmarkRoot);
            persistenceDir = Files.createTempDirectory(benchmarkRoot, "run-");
            system = ActorSystem.create(Behaviors.empty(), "jmh-single-symbol-persistence-" + System.nanoTime());
            replyProbe = system.ignoreRef();
            singleActor = system.systemActorOf(
                    SymbolActor.create("SYM0", true, persistenceDir),
                    "symbol-sym0",
                    // 撮合仍跑在 matching-dispatcher，上层持久化写入在 SymbolActor 内部再做委托。
                    Props.empty().withDispatcherFromConfig("akka.actor.matching-dispatcher")
            );
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws Exception {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
            deleteDirectory(persistenceDir);
        }
    }

    private static long waitUntilProcessed(ActorSystem<Void> system,
                                           ActorRef<SymbolActor.Command> actor,
                                           long expected) throws Exception {
        Duration timeout = Duration.ofSeconds(30);

        while (true) {
            long processed = AskPattern.ask(
                    actor,
                    SymbolActor.GetProcessedCount::new,
                    timeout,
                    system.scheduler()
            ).toCompletableFuture().get();

            if (processed >= expected) {
                return processed;
            }

            Thread.sleep(1);
        }
    }

    private static long waitUntilProcessed(ActorSystem<Void> system,
                                           Map<String, ActorRef<SymbolActor.Command>> actorMap,
                                           long expected) throws Exception {
        Duration timeout = Duration.ofSeconds(3);

        while (true) {
            long totalProcessed = 0L;

            for (ActorRef<SymbolActor.Command> actor : actorMap.values()) {
                totalProcessed += AskPattern.ask(
                        actor,
                        SymbolActor.GetProcessedCount::new,
                        timeout,
                        system.scheduler()
                ).toCompletableFuture().get();
            }

            if (totalProcessed >= expected) {
                return totalProcessed;
            }

            Thread.sleep(1);
        }
    }

    private static void deleteDirectory(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }

        // 每次 JMH 调用都用独立临时目录，避免上一次持久化文件影响当前结果。
        try (var stream = Files.walk(path)) {
            stream.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }

    private record OrderSeed(String orderId,
                             String userId,
                             String symbol,
                             Side side,
                             long price,
                             long quantity,
                             long createTime) {

        private Order toOrder() {
            return new Order(orderId, userId, symbol, side, price, quantity, createTime, orderId);
        }

        private static List<OrderSeed> generate(int totalOrders, int symbolCount, int priceLevels, boolean crossMatch) {
            java.util.ArrayList<OrderSeed> orders = new java.util.ArrayList<>(totalOrders);

            for (int i = 0; i < totalOrders; i++) {
                String symbol = "SYM" + (i % symbolCount);
                Side side = (i % 2 == 0) ? Side.BUY : Side.SELL;
                long base = 10_000L;
                long offset = i % priceLevels;

                // cross=true 更偏向成交路径；false 更偏向挂单路径。
                long price = crossMatch
                        ? ((side == Side.BUY) ? base + offset : base + offset - 1)
                        : ((side == Side.BUY) ? base - 100 - offset : base + 100 + offset);

                orders.add(new OrderSeed(
                        "JMH-O" + i,
                        "JMH-U" + (i % 10_000),
                        symbol,
                        side,
                        price,
                        1L,
                        1_700_000_000_000L + i
                ));
            }

            return orders;
        }
    }
}
