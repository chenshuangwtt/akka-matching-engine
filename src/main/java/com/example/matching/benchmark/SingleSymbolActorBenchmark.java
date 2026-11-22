package com.example.matching.benchmark;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.javadsl.AskPattern;
import com.example.matching.actor.SymbolActor;
import com.example.matching.domain.Order;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class SingleSymbolActorBenchmark {

    public static void main(String[] args) throws Exception {
        BenchmarkConfig raw = BenchmarkConfig.fromArgs(args);
        // 单 symbol 场景强制收敛到一个撮合 Actor。
        BenchmarkConfig config = new BenchmarkConfig(
                raw.getTotalOrders(),
                1,
                raw.getPriceLevels(),
                raw.isCrossMatch()
        );

        System.out.println("Running SingleSymbolActorBenchmark with: " + config);

        List<Order> orders = BenchmarkOrderFactory.generateOrders(config);

        ActorSystem<Void> system = ActorSystem.create(
                akka.actor.typed.javadsl.Behaviors.empty(),
                "single-symbol-benchmark-system"
        );

        ActorRef<SymbolActor.Command> symbolActor =
                system.systemActorOf(SymbolActor.create("SYM0", false, null), "symbol-sym0", Props.empty().withDispatcherFromConfig("akka.actor.benchmark-dispatcher"));

        long start = System.nanoTime();

        for (Order order : orders) {
            symbolActor.tell(new SymbolActor.PlaceOrderNoReply(order));
        }

        waitUntilProcessed(system, symbolActor, config.getTotalOrders());

        long end = System.nanoTime();

        BenchmarkStats.print("SingleSymbolActorBenchmark", config.getTotalOrders(), start, end);

        system.terminate();
    }

    private static void waitUntilProcessed(ActorSystem<Void> system,
                                           ActorRef<SymbolActor.Command> actor,
                                           long expected) throws Exception {
        // 通过 ask 已处理数量，等待 actor 真正消费完全部消息。
        Duration timeout = Duration.ofSeconds(3);

        while (true) {
            CompletionStage<Long> future = AskPattern.ask(
                    actor,
                    (ActorRef<Long> replyTo) -> new SymbolActor.GetProcessedCount(replyTo),
                    timeout,
                    system.scheduler()
            );

            long processed = future.toCompletableFuture().get();

            if (processed >= expected) {
                return;
            }

            Thread.sleep(10);
        }
    }
}
