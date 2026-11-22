package com.example.matching.engine;

import com.example.matching.domain.Side;

public record OrderRef(Side side, long price, OrderBook.OrderNode node) {
}
