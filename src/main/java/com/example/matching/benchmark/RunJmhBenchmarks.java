package com.example.matching.benchmark;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collection;
import java.util.Locale;

public class RunJmhBenchmarks {

    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
        Options options = args.length == 0
                ? new OptionsBuilder()
                .include(MatchingEngineJmhBenchmark.class.getSimpleName())
                .forks(1)
                .build()
                : new CommandLineOptions(args);

        Collection<RunResult> results = new Runner(options).run();
        // 在 JMH 原始结果之外，再补一份按订单数换算后的吞吐视图。
        printDerivedOrderThroughput(results);
    }

    private static void printDerivedOrderThroughput(Collection<RunResult> results) {
        if (results.isEmpty()) {
            return;
        }

        System.out.println();
        System.out.println("========== Derived Order Throughput ==========");
        for (RunResult result : results) {
            String benchmark = result.getParams().getBenchmark();
            String ordersParam = result.getParams().getParam("orders");
            String symbolsParam = safeParam(result, "symbols");
            String crossParam = safeParam(result, "cross");
            String priceLevelsParam = safeParam(result, "priceLevels");
            String persistenceParam = detectPersistenceMode(benchmark);
            double opsPerSecond = result.getPrimaryResult().getScore();

            if (ordersParam == null) {
                System.out.printf(
                        Locale.US,
                        "%s | persistence=%s | symbols=%s | cross=%s | priceLevels=%s | ops/s=%.2f%n",
                        benchmark,
                        persistenceParam,
                        symbolsParam,
                        crossParam,
                        priceLevelsParam,
                        opsPerSecond
                );
                continue;
            }

            long ordersPerInvocation = Long.parseLong(ordersParam);
            double ordersPerSecond = opsPerSecond * ordersPerInvocation;

            System.out.printf(
                    Locale.US,
                    "%s | orders=%d | persistence=%s | symbols=%s | cross=%s | priceLevels=%s | ops/s=%.2f | derived orders/s=%.2f%n",
                    benchmark,
                    ordersPerInvocation,
                    persistenceParam,
                    symbolsParam,
                    crossParam,
                    priceLevelsParam,
                    opsPerSecond,
                    ordersPerSecond
            );
        }
        System.out.println("==============================================");
        System.out.println();
    }

    private static String safeParam(RunResult result, String name) {
        String value = result.getParams().getParam(name);
        return value == null ? "-" : value;
    }

    private static String detectPersistenceMode(String benchmark) {
        // 持久化场景目前通过 benchmark 名称区分。
        return benchmark.contains("WithPersistence") ? "on" : "off";
    }
}
