package com.example.matching.benchmark;

import com.example.matching.domain.Order;
import com.example.matching.engine.OrderBook;

import java.util.List;

public class OrderBookLightBenchmark {

    public static void main(String[] args) {
        // 纯撮合轻量基线，不生成 MatchResult / Trade。
        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
        System.out.println("Running OrderBookLightBenchmark with: " + config);

        List<Order> orders = BenchmarkOrderFactory.generateOrders(config);
        OrderBook orderBook = new OrderBook();

        long start = System.nanoTime();
        for (Order order : orders) {
            orderBook.placeForBenchmark(order);
        }
        long end = System.nanoTime();

        BenchmarkStats.print("OrderBookLightBenchmark", config.getTotalOrders(), start, end);
        System.out.println("bookSnapshotLines = " + orderBook.snapshot().size());
    }
}
