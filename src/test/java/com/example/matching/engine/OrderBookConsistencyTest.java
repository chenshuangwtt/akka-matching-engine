package com.example.matching.engine;

import static org.junit.jupiter.api.Assertions.*;

import com.example.matching.domain.Order;
import com.example.matching.domain.OrderStatus;
import com.example.matching.domain.Side;

import org.junit.jupiter.api.Test;

class OrderBookConsistencyTest {

    @Test
    void incompleteSnapshotCannotSilentlyDropOpenOrders() {
        OrderBook book = new OrderBook();
        Order active = order("ACTIVE", Side.BUY, 10000, 2);
        assertThrows(
                IllegalStateException.class,
                () -> book.restoreSnapshot(java.util.List.of(), java.util.List.of(active)));
        assertThrows(
                IllegalStateException.class,
                () ->
                        book.restoreSnapshot(
                                java.util.List.of(active, active), java.util.List.of(active)));
    }

    @Test
    void tradeIdsAtSameSequenceAreDistinctAcrossSymbols() {
        assertNotEquals(
                com.example.matching.util.IdGenerator.tradeId("BTC/USDT", 2, 1),
                com.example.matching.util.IdGenerator.tradeId("ETH/USDT", 2, 1));
        assertEquals(
                com.example.matching.util.IdGenerator.tradeId("BTC/USDT", 2, 1),
                com.example.matching.util.IdGenerator.tradeId("BTC/USDT", 2, 1));
    }

    private Order order(String id, Side side, long price, long quantity) {
        return new Order(id, "U", "BTCUSDT", side, price, quantity, 1, id);
    }

    @Test
    void priceTimePriorityAndPartialCancelKeepQuantitiesConsistent() {
        OrderBook book = new OrderBook();
        Order first = order("FIRST", Side.BUY, 10000, 2);
        Order second = order("SECOND", Side.BUY, 10000, 5);
        MatchResult accepted = book.place(first);
        book.place(second);
        MatchResult fill = book.place(order("SELL", Side.SELL, 10000, 3));
        assertEquals(2, fill.getTrades().size());
        assertEquals("FIRST", fill.getTrades().get(0).getBuyOrderId());
        assertEquals(2, fill.getTrades().get(0).getQuantity());
        assertEquals("SECOND", fill.getTrades().get(1).getBuyOrderId());
        assertEquals(1, fill.getTrades().get(1).getQuantity());
        assertEquals(OrderStatus.FILLED, first.getStatus());
        assertEquals(OrderStatus.PARTIALLY_FILLED, second.getStatus());
        assertEquals(4, second.getRemainingQuantity());
        assertEquals(
                OrderStatus.NEW,
                accepted.getFinalOrder().getStatus(),
                "an earlier response must not change when its book order later fills");
        assertTrue(book.cancel("SECOND"));
        assertFalse(book.cancel("SECOND"));
        assertEquals(OrderStatus.CANCELED, second.getStatus());
        assertEquals(4, second.getRemainingQuantity());
        assertEquals(0, book.openOrderCount());
    }

    @Test
    void snapshotRestoreKeepsPriceTimeOrderAndHistoricalStatuses() {
        OrderBook before = new OrderBook();
        before.place(order("OLDER", Side.BUY, 10000, 2));
        before.place(order("NEWER", Side.BUY, 10000, 3));
        before.place(order("LOWER", Side.BUY, 9900, 4));
        OrderBook restored = new OrderBook();
        restored.restoreSnapshot(before.exportOpenOrders(), before.exportAllOrders());
        MatchResult sell = restored.place(order("SELL", Side.SELL, 9900, 4));
        assertEquals("OLDER", sell.getTrades().get(0).getBuyOrderId());
        assertEquals("NEWER", sell.getTrades().get(1).getBuyOrderId());
        assertEquals(2, sell.getTrades().get(0).getQuantity());
        assertEquals(2, sell.getTrades().get(1).getQuantity());
        assertEquals(
                OrderStatus.FILLED,
                restored.exportAllOrders().stream()
                        .filter(o -> o.getOrderId().equals("OLDER"))
                        .findFirst()
                        .orElseThrow()
                        .getStatus());
    }

    @Test
    void invalidQuantityCannotEnterBookOrReduceRemainingBelowZero() {
        OrderBook book = new OrderBook();
        assertThrows(
                IllegalArgumentException.class,
                () -> book.place(order("ZERO", Side.BUY, 10000, 0)));
        assertThrows(
                IllegalArgumentException.class,
                () -> book.place(order("NEGATIVE", Side.BUY, 10000, -1)));
        assertEquals(0, book.openOrderCount());
        assertTrue(book.exportAllOrders().isEmpty());
        Order valid = order("VALID", Side.BUY, 10000, 2);
        assertThrows(IllegalArgumentException.class, () -> valid.reduceRemaining(3));
        assertEquals(2, valid.getRemainingQuantity());
        assertEquals(OrderStatus.NEW, valid.getStatus());
    }
}
