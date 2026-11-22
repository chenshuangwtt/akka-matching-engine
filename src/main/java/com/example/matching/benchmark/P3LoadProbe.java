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
import com.example.matching.persistence.OrderBookSnapshot;
import com.example.matching.persistence.OrderEvent;
import com.example.matching.persistence.OrderEventStore;
import com.example.matching.util.SymbolKeyUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Bounded, explicit P3 measurement; outputs data only, never asserts a performance target. */
public final class P3LoadProbe {
    private record Sample(MatchResult result, long latencyNanos) {}

    private P3LoadProbe() {}

    public static void main(String[] args) throws Exception {
        int measured = args.length > 0 ? Integer.parseInt(args[0]) : 400;
        int warmup = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int ordersPerSymbol = args.length > 2 ? Integer.parseInt(args[2]) : 2_500;
        int symbols = args.length > 3 ? Integer.parseInt(args[3]) : 4;
        if (measured < 1 || measured > 5_000 || warmup < 0 || warmup > 500
                || ordersPerSymbol < 1 || ordersPerSymbol > 100_000 || symbols < 1 || symbols > 8
                || (long) ordersPerSymbol * symbols > 100_000) {
            throw new IllegalArgumentException("Probe bounds: measured 1-5000, warmup 0-500, orders/symbol 1-100000, symbols 1-8, total <=100000");
        }
        Path root = Path.of("target", "p3-probe-" + System.currentTimeMillis()).toAbsolutePath().normalize();
        Files.createDirectories(root);
        System.out.printf("P3_ENV java=%s processors=%d maxHeap=%d output=%s%n",
                System.getProperty("java.version"), Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory(), root);
        measureCore(warmup, measured);
        measureActorWithoutJournal(warmup, measured);
        measureJournal(root.resolve("journal"), warmup, measured);
        measureCapacity(root.resolve("capacity"), symbols, ordersPerSymbol);
    }

    private static void measureCore(int warmup, int measured) {
        OrderBook book = new OrderBook();
        long[] latencies = new long[measured];
        long measuredStarted = 0;
        for (int i = 0; i < warmup + measured; i++) {
            Order order = new Order("core-order-" + i, "probe", "P3JOURNAL", Side.BUY,
                    10_000, 1, i + 1L, "core-key-" + i);
            if (i == warmup) measuredStarted = System.nanoTime();
            long started = System.nanoTime();
            MatchResult result = book.place(order);
            if (result.getFinalOrder() == null) throw new IllegalStateException("Core order was not accepted");
            if (i >= warmup) latencies[i - warmup] = System.nanoTime() - started;
        }
        double seconds = (System.nanoTime() - measuredStarted) / 1_000_000_000.0;
        Arrays.sort(latencies);
        System.out.printf("P3_CORE warmup=%d measured=%d elapsedSeconds=%.3f tps=%.2f "
                        + "p50Ms=%.6f p95Ms=%.6f p99Ms=%.6f%n",
                warmup, measured, seconds, measured / seconds,
                percentile(latencies, 0.50), percentile(latencies, 0.95), percentile(latencies, 0.99));
    }

    private static void measureActorWithoutJournal(int warmup, int measured) throws Exception {
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "p3-actor-probe");
        try {
            ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                    SymbolActor.create("P3JOURNAL", false, null), "symbol-p3-actor",
                    Props.empty().withDispatcherFromConfig("akka.actor.matching-dispatcher"));
            sendBatch(system, actor, 0, warmup, 16);
            long started = System.nanoTime();
            long[] latencies = sendBatch(system, actor, warmup, measured, 16);
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            Arrays.sort(latencies);
            System.out.printf("P3_ACTOR_ACK warmup=%d measured=%d inFlight=16 elapsedSeconds=%.3f "
                            + "tps=%.2f p50Ms=%.3f p95Ms=%.3f p99Ms=%.3f%n",
                    warmup, measured, seconds, measured / seconds,
                    percentile(latencies, 0.50), percentile(latencies, 0.95), percentile(latencies, 0.99));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    private static void measureJournal(Path dir, int warmup, int measured) throws Exception {
        Files.createDirectories(dir);
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "p3-journal-probe");
        try {
            ActorRef<SymbolActor.Command> actor = system.systemActorOf(
                    SymbolActor.create("P3JOURNAL", true, dir, 1_000), "symbol-p3-journal",
                    Props.empty().withDispatcherFromConfig("akka.actor.matching-dispatcher"));
            sendBatch(system, actor, 0, warmup, 16);
            long started = System.nanoTime();
            long[] latencies = sendBatch(system, actor, warmup, measured, 16);
            double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
            Path journal = dir.resolve(SymbolKeyUtil.toPersistenceStem("P3JOURNAL") + ".events.jsonl");
            Path snapshot = dir.resolve(SymbolKeyUtil.toPersistenceStem("P3JOURNAL") + ".snapshot.json");
            long journalBytes = Files.size(journal);
            long recoveryStarted = System.nanoTime();
            var recovery = new OrderEventStore(dir, "P3JOURNAL").loadRecoveryState();
            double recoveryMillis = (System.nanoTime() - recoveryStarted) / 1_000_000.0;
            long checkpoint = recovery.snapshot() == null ? 0 : recovery.snapshot().getLastAppliedSequence();
            if (recovery.lastSequence() != warmup + measured
                    || checkpoint + recovery.events().size() != warmup + measured) {
                throw new IllegalStateException("Journal recovery count differs from confirmed order count");
            }
            Arrays.sort(latencies);
            System.out.printf("P3_JOURNAL warmup=%d measured=%d inFlight=16 elapsedSeconds=%.3f tps=%.2f "
                            + "p50Ms=%.3f p95Ms=%.3f p99Ms=%.3f journalBytes=%d pendingEvents=%d "
                            + "snapshotBytes=%d snapshotSequence=%d recoveryMs=%.3f lastSequence=%d%n",
                    warmup, measured, seconds, measured / seconds,
                    percentile(latencies, 0.50), percentile(latencies, 0.95), percentile(latencies, 0.99),
                    journalBytes, recovery.events().size(),
                    Files.exists(snapshot) ? Files.size(snapshot) : 0, checkpoint, recoveryMillis,
                    recovery.lastSequence());
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    private static long[] sendBatch(ActorSystem<Void> system, ActorRef<SymbolActor.Command> actor,
                                    int first, int count, int maxInFlight) throws Exception {
        ArrayDeque<CompletableFuture<Sample>> pending = new ArrayDeque<>();
        long[] latencies = new long[count];
        int completed = 0;
        for (int index = first; index < first + count; index++) {
            Order order = new Order("p3-order-" + index, "probe", "P3JOURNAL", Side.BUY,
                    10_000, 1, index + 1L, "p3-key-" + index);
            long sent = System.nanoTime();
            CompletableFuture<Sample> future = AskPattern.<SymbolActor.Command, MatchResult>ask(
                    actor, replyTo -> new SymbolActor.PlaceOrder(order, replyTo),
                    Duration.ofSeconds(30), system.scheduler())
                    .thenApply(result -> new Sample(result, System.nanoTime() - sent)).toCompletableFuture();
            pending.addLast(future);
            if (pending.size() >= maxInFlight) {
                Sample sample = pending.removeFirst().get(30, TimeUnit.SECONDS);
                if (sample.result().getFinalOrder() == null) throw new IllegalStateException(sample.result().getMessage());
                latencies[completed++] = sample.latencyNanos();
            }
        }
        while (!pending.isEmpty()) {
            Sample sample = pending.removeFirst().get(30, TimeUnit.SECONDS);
            if (sample.result().getFinalOrder() == null) throw new IllegalStateException(sample.result().getMessage());
            latencies[completed++] = sample.latencyNanos();
        }
        return latencies;
    }

    private static double percentile(long[] sorted, double fraction) {
        int index = Math.max(0, (int) Math.ceil(sorted.length * fraction) - 1);
        return sorted[index] / 1_000_000.0;
    }

    private static void measureCapacity(Path dir, int symbols, int perSymbol) throws Exception {
        Files.createDirectories(dir);
        usedHeapAfterGc();
        long before = usedHeapAfterGc();
        List<OrderBook> books = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        long snapshotBytes = 0;
        for (int symbolIndex = 0; symbolIndex < symbols; symbolIndex++) {
            String symbol = "P3SYM" + symbolIndex;
            OrderBook book = new OrderBook();
            for (int i = 0; i < perSymbol; i++) {
                String id = "00000000-0000-0000-0000-" + String.format("%012d", i);
                book.placeForBenchmark(new Order(id, "probe", symbol, Side.BUY,
                        10_000, 1, i + 1L, "cli-" + symbolIndex + "-" + i));
            }
            if (perSymbol == OrderBook.DEFAULT_MAX_HISTORICAL_ORDERS) {
                if (!book.isAtCapacity()) throw new IllegalStateException("Capacity boundary not reached");
                try {
                    book.placeForBenchmark(new Order("over-capacity", "probe", symbol,
                            Side.BUY, 10_000, 1, perSymbol + 1L, "over-capacity"));
                    throw new IllegalStateException("Capacity did not reject the next order");
                } catch (IllegalStateException expected) {
                    if (!expected.getMessage().contains("capacity exceeded")) throw expected;
                }
            }
            books.add(book);
            OrderBookSnapshot snapshot = new OrderBookSnapshot();
            snapshot.setFormatVersion(OrderEvent.FORMAT_VERSION);
            snapshot.setLastAppliedSequence((long) perSymbol);
            snapshot.setProcessedCount(perSymbol);
            snapshot.setOpenOrders(book.exportOpenOrders());
            snapshot.setAllOrders(book.exportAllOrders());
            Path file = dir.resolve(symbol + ".synthetic-snapshot.json");
            mapper.writeValue(file.toFile(), snapshot);
            snapshotBytes += Files.size(file);
        }
        long after = usedHeapAfterGc();
        int total = symbols * perSymbol;
        if (books.stream().mapToInt(OrderBook::historicalOrderCount).sum() != total) {
            throw new IllegalStateException("Historical count mismatch");
        }
        System.out.printf("P3_CAPACITY symbols=%d ordersPerSymbol=%d totalOrders=%d heapBefore=%d "
                        + "heapAfter=%d heapDelta=%d syntheticSnapshotBytes=%d snapshotBytesPerOrder=%.1f "
                        + "atDefaultLimit=%s%n",
                symbols, perSymbol, total, before, after, after - before,
                snapshotBytes, (double) snapshotBytes / total,
                perSymbol == OrderBook.DEFAULT_MAX_HISTORICAL_ORDERS);
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        System.gc();
        Thread.sleep(100);
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
}
