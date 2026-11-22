package com.example.matching.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Order {
    private final String orderId;
    private final String userId;
    private final String symbol;
    private final Side side;
    private final long price;
    private final long quantity;
    private long remainingQuantity;
    private OrderStatus status;
    private final long createTime;
    private final String clientOrderId;

    public Order(String orderId, String userId, String symbol, Side side, long price, long quantity,
                 long createTime, String clientOrderId) {
        this(orderId, userId, symbol, side, price, quantity, quantity, OrderStatus.NEW, createTime, clientOrderId);
    }

    @JsonCreator
    public Order(@JsonProperty("orderId") String orderId,
                 @JsonProperty("userId") String userId,
                 @JsonProperty("symbol") String symbol,
                 @JsonProperty("side") Side side,
                 @JsonProperty("price") long price,
                 @JsonProperty("quantity") long quantity,
                 @JsonProperty("remainingQuantity") long remainingQuantity,
                 @JsonProperty("status") OrderStatus status,
                 @JsonProperty("createTime") long createTime,
                 @JsonProperty("clientOrderId") String clientOrderId) {
        this.orderId = orderId;
        this.userId = userId;
        this.symbol = symbol;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.remainingQuantity = remainingQuantity;
        this.status = status;
        this.createTime = createTime;
        this.clientOrderId = clientOrderId;
    }

    public Order copy() {
        return new Order(orderId, userId, symbol, side, price, quantity, remainingQuantity, status,
                createTime, clientOrderId);
    }

    public boolean sameBusinessRequest(Order other) {
        return java.util.Objects.equals(userId, other.userId)
                && java.util.Objects.equals(symbol, other.symbol)
                && side == other.side && price == other.price && quantity == other.quantity;
    }

    public void reduceRemaining(long executedQty) {
        if (executedQty <= 0 || executedQty > remainingQuantity) {
            throw new IllegalArgumentException("executed quantity must be positive and no greater than remaining quantity");
        }
        this.remainingQuantity -= executedQty;
        if (this.remainingQuantity == 0) {
            this.status = OrderStatus.FILLED;
        } else if (this.remainingQuantity < this.quantity) {
            this.status = OrderStatus.PARTIALLY_FILLED;
        }
    }

    public void cancel() {
        this.status = OrderStatus.CANCELED;
    }
}
