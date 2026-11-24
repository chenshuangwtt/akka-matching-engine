package com.example.matching.persistence;

import com.example.matching.domain.Order;
import com.example.matching.domain.Side;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class OrderEvent {

    public static final int FORMAT_VERSION = 3;

    public enum Type {
        PLACE,
        CANCEL
    }

    private Type type;
    private int formatVersion;
    private Long sequence;
    private String symbol;
    private String orderId;
    private String userId;
    private String clientOrderId;
    private Side side;
    private long price;
    private long quantity;
    private long createTime;

    public static OrderEvent place(Order order) {
        OrderEvent event = new OrderEvent();
        event.formatVersion = FORMAT_VERSION;
        event.type = Type.PLACE;
        event.symbol = order.getSymbol();
        event.orderId = order.getOrderId();
        event.userId = order.getUserId();
        event.clientOrderId = order.getClientOrderId();
        event.side = order.getSide();
        event.price = order.getPrice();
        event.quantity = order.getQuantity();
        event.createTime = order.getCreateTime();
        return event;
    }

    public static OrderEvent cancel(String symbol, String orderId) {
        OrderEvent event = new OrderEvent();
        event.formatVersion = FORMAT_VERSION;
        event.type = Type.CANCEL;
        event.symbol = symbol;
        event.orderId = orderId;
        return event;
    }

    public Order toOrder() {
        return new Order(orderId, userId, symbol, side, price, quantity, createTime, clientOrderId);
    }
}
