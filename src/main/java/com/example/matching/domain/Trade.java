package com.example.matching.domain;

import lombok.Data;


@Data
public class Trade {
    private final String tradeId;
    private final String symbol;
    private final String buyOrderId;
    private final String sellOrderId;
    private final long price;
    private final long quantity;
    private final long tradeTime;

    public Trade(String tradeId,
                 String symbol,
                 String buyOrderId,
                 String sellOrderId,
                 long price,
                 long quantity,
                 long tradeTime) {
        this.tradeId = tradeId;
        this.symbol = symbol;
        this.buyOrderId = buyOrderId;
        this.sellOrderId = sellOrderId;
        this.price = price;
        this.quantity = quantity;
        this.tradeTime = tradeTime;
    }

}