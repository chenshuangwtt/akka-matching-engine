package com.example.matching.domain;

import java.util.regex.Pattern;

/** Input limits for the single-process integer-tick order model. */
public final class OrderRules {
    public static final long MAX_PRICE = 1_000_000_000L;
    public static final long MAX_QUANTITY = 1_000_000_000L;
    private static final int MAX_ID_LENGTH = 128;
    private static final int MAX_SYMBOL_LENGTH = 64;
    private static final Pattern SYMBOL = Pattern.compile("[A-Z0-9]+(?:[/_-][A-Z0-9]+)*");

    private OrderRules() {}

    public static void validateNewOrder(Order order) {
        if (order == null) throw new IllegalArgumentException("order is required");
        validateId(order.getOrderId(), "orderId");
        validateId(order.getUserId(), "userId");
        validateId(order.getClientOrderId(), "clientOrderId");
        validateSymbol(order.getSymbol());
        if (order.getSide() == null) throw new IllegalArgumentException("side must be BUY or SELL");
        if (order.getPrice() <= 0 || order.getPrice() > MAX_PRICE) {
            throw new IllegalArgumentException("price must be between 1 and " + MAX_PRICE);
        }
        if (order.getQuantity() <= 0 || order.getQuantity() > MAX_QUANTITY) {
            throw new IllegalArgumentException("quantity must be between 1 and " + MAX_QUANTITY);
        }
        if (order.getRemainingQuantity() != order.getQuantity() || order.getStatus() != OrderStatus.NEW) {
            throw new IllegalArgumentException("new order must have full remaining quantity and NEW status");
        }
    }

    public static void validateSymbol(String symbol) {
        if (symbol == null || symbol.length() > MAX_SYMBOL_LENGTH || !SYMBOL.matcher(symbol).matches()) {
            throw new IllegalArgumentException("symbol must be 1-64 uppercase letters/digits with optional /, _, or - separators");
        }
    }

    public static void validateId(String value, String name) {
        if (value == null || value.isBlank() || value.length() > MAX_ID_LENGTH
                || !value.equals(value.strip()) || value.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException(name + " must be a non-blank identifier of at most 128 characters");
        }
    }
}
