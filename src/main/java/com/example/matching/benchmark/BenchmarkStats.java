package com.example.matching.benchmark;

public class BenchmarkStats {

    public static void print(String title, int totalOrders, long startNanos, long endNanos) {
        // 手写 benchmark 统一输出总耗时和每秒订单数。
        long elapsedNanos = endNanos - startNanos;
        double elapsedMs = elapsedNanos / 1_000_000.0;
        double elapsedSec = elapsedNanos / 1_000_000_000.0;
        double ops = totalOrders / elapsedSec;

        System.out.println();
        System.out.println("========== " + title + " ==========");
        System.out.println("totalOrders = " + totalOrders);
        System.out.println("elapsedMs   = " + String.format("%.3f", elapsedMs));
        System.out.println("throughput  = " + String.format("%.2f", ops) + " orders/sec");
        System.out.println("====================================");
        System.out.println();
    }
}
