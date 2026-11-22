package com.example.matching.benchmark;

import com.example.matching.domain.Order;
import com.example.matching.engine.MatchResult;
import com.example.matching.engine.OrderBook;

import java.util.List;

public class OrderBookBenchmark {

    public static void main(String[] args) {
        // 纯撮合基线，包含 MatchResult / Trade 分配成本。
        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
        System.out.println("Running OrderBookBenchmark with: " + config);

        List<Order> orders = BenchmarkOrderFactory.generateOrders(config);
        OrderBook orderBook = new OrderBook();

        long totalTrades = 0L;

        long start = System.nanoTime();
        for (Order order : orders) {
            MatchResult result = orderBook.place(order);
            totalTrades += result.getTrades().size();
        }
        long end = System.nanoTime();

        BenchmarkStats.print("OrderBookBenchmark", config.getTotalOrders(), start, end);
        System.out.println("totalTrades = " + totalTrades);
        System.out.println("bookSnapshotLines = " + orderBook.snapshot().size());
    }
}
