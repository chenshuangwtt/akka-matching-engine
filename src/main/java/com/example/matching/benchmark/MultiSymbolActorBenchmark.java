package com.example.matching.benchmark;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AskPattern;
import com.example.matching.actor.SymbolActor;
import com.example.matching.domain.Order;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class MultiSymbolActorBenchmark {

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
        System.out.println("Running MultiSymbolActorBenchmark with: " + config);

        List<Order> orders = BenchmarkOrderFactory.generateOrders(config);

        ActorSystem<Void> system = ActorSystem.create(
                akka.actor.typed.javadsl.Behaviors.empty(),
                "multi-symbol-benchmark-system"
        );

        Map<String, ActorRef<SymbolActor.Command>> actorMap = new HashMap<>();
        // 每个 symbol 一个 Actor，用来观察按 symbol 分片后的并行收益。
        for (int i = 0; i < config.getSymbolCount(); i++) {
            String symbol = "SYM" + i;
            actorMap.put(symbol, system.systemActorOf(SymbolActor.create(symbol, false, null), "symbol-" + symbol.toLowerCase(),
                    Props.empty().withDispatcherFromConfig("akka.actor.benchmark-dispatcher")));
        }

        long start = System.nanoTime();

        for (Order order : orders) {
            ActorRef<SymbolActor.Command> actor = actorMap.get(order.getSymbol());
            actor.tell(new SymbolActor.PlaceOrderNoReply(order));
        }

        waitUntilProcessed(system, actorMap, config.getTotalOrders());

        long end = System.nanoTime();

        BenchmarkStats.print("MultiSymbolActorBenchmark", config.getTotalOrders(), start, end);

        system.terminate();
    }

    private static void waitUntilProcessed(ActorSystem<Void> system,
                                           Map<String, ActorRef<SymbolActor.Command>> actorMap,
                                           long expected) throws Exception {
        // 汇总所有 symbol actor 的已处理数量，直到消费完本轮消息。
        Duration timeout = Duration.ofSeconds(3);

        while (true) {
            long totalProcessed = 0L;

            for (ActorRef<SymbolActor.Command> actor : actorMap.values()) {
                CompletionStage<Long> future = AskPattern.ask(
                        actor,
                        (ActorRef<Long> replyTo) -> new SymbolActor.GetProcessedCount(replyTo),
                        timeout,
                        system.scheduler()
                );

                totalProcessed += future.toCompletableFuture().get();
            }

            if (totalProcessed >= expected) {
                return;
            }

            Thread.sleep(10);
        }
    }
}
