package com.example.matching.benchmark;

import com.example.matching.domain.Order;
import com.example.matching.domain.Side;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkOrderFactory {

    private static final AtomicLong ORDER_SEQ = new AtomicLong(1);

    public static List<Order> generateOrders(BenchmarkConfig config) {
        // 固定规则生成订单，方便不同 benchmark 场景做横向对比。
        List<Order> orders = new ArrayList<>(config.getTotalOrders());

        for (int i = 0; i < config.getTotalOrders(); i++) {
            String symbol = "SYM" + (i % config.getSymbolCount());
            Side side = (i % 2 == 0) ? Side.BUY : Side.SELL;

            long base = 10_000L;
            long offset = i % config.getPriceLevels();

            long price;
            if (config.isCrossMatch()) {
                // 更容易成交
                price = (side == Side.BUY)
                        ? base + offset
                        :base + offset - 1;
            } else {
                // 尽量不成交
                price = (side == Side.BUY)
                        ? base - 100 - offset
                        : base + 100 + offset;
            }

            long quantity = 1;
            String orderId = "O" + ORDER_SEQ.getAndIncrement();

            orders.add(new Order(
                    orderId,
                    "U" + (i % 10_000),
                    symbol,
                    side,
                    price,
                    quantity,
                    System.currentTimeMillis(),
                    orderId
            ));
        }

        return orders;
    }
}
