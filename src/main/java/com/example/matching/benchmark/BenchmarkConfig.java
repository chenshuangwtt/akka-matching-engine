package com.example.matching.benchmark;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
public class BenchmarkConfig {

    // 手写 benchmark 和 JMH 共用的参数载体。
    private final int totalOrders;
    private final int symbolCount;
    private final int priceLevels;
    private final boolean crossMatch;

    public static BenchmarkConfig fromArgs(String[] args) {
        // 默认值偏向快速本地试跑。
        int totalOrders = 100_000;
        int symbolCount = 1;
        int priceLevels = 100;
        boolean crossMatch = true;

        for (String arg : args) {
            if (arg.startsWith("--orders=")) {
                totalOrders = Integer.parseInt(arg.substring("--orders=".length()));
            } else if (arg.startsWith("--symbols=")) {
                symbolCount = Integer.parseInt(arg.substring("--symbols=".length()));
            } else if (arg.startsWith("--price-levels=")) {
                priceLevels = Integer.parseInt(arg.substring("--price-levels=".length()));
            } else if (arg.startsWith("--cross=")) {
                crossMatch = Boolean.parseBoolean(arg.substring("--cross=".length()));
            }
        }

        return new BenchmarkConfig(totalOrders, symbolCount, priceLevels, crossMatch);
    }
}
