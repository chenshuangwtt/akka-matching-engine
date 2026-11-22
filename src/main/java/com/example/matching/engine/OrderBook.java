package com.example.matching.engine;

import com.example.matching.domain.Order;
import com.example.matching.domain.OrderStatus;
import com.example.matching.domain.OrderRules;
import com.example.matching.domain.Side;
import com.example.matching.domain.Trade;
import com.example.matching.util.IdGenerator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class OrderBook {

    public static final int DEFAULT_MAX_HISTORICAL_ORDERS = 100_000;
    private final int maxHistoricalOrders;

    public OrderBook() { this(DEFAULT_MAX_HISTORICAL_ORDERS); }

    public OrderBook(int maxHistoricalOrders) {
        if (maxHistoricalOrders <= 0) throw new IllegalArgumentException("maxHistoricalOrders must be positive");
        this.maxHistoricalOrders = maxHistoricalOrders;
    }

    private final NavigableMap<Long, PriceLevel> buyBook =
            new TreeMap<>(Comparator.reverseOrder());

    private final NavigableMap<Long, PriceLevel> sellBook =
            new TreeMap<>();

    private final Map<String, OrderRef> orderIndex = new HashMap<>();
    private final Map<String, Order> allOrders = new HashMap<>();
    private final Map<ClientKey, String> clientOrderIndex = new HashMap<>();

    private record ClientKey(String userId, String clientOrderId) {}

    public static void validateNewOrder(Order order) {
        OrderRules.validateNewOrder(order);
    }

    public Order findByClientOrderId(String userId, String clientOrderId) {
        String orderId = clientOrderIndex.get(new ClientKey(userId, clientOrderId));
        return orderId == null ? null : allOrders.get(orderId);
    }

    public boolean hasSeenOrderId(String orderId) {
        return allOrders.containsKey(orderId);
    }

    public int historicalOrderCount() { return allOrders.size(); }

    public boolean isAtCapacity() { return allOrders.size() >= maxHistoricalOrders; }

    public int maxHistoricalOrders() { return maxHistoricalOrders; }

    private void register(Order order) {
        if (allOrders.putIfAbsent(order.getOrderId(), order) != null) {
            throw new IllegalArgumentException("Duplicate server orderId: " + order.getOrderId());
        }
        String previous = clientOrderIndex.putIfAbsent(
                new ClientKey(order.getUserId(), order.getClientOrderId()), order.getOrderId());
        if (previous != null) {
            allOrders.remove(order.getOrderId());
            throw new IllegalArgumentException("Duplicate clientOrderId: " + order.getClientOrderId());
        }
    }

    public MatchResult place(Order incoming) { return place(incoming, 0); }

    public MatchResult place(Order incoming, long eventSequence) {
        validateNewOrder(incoming);
        if (hasSeenOrderId(incoming.getOrderId()) ||
                findByClientOrderId(incoming.getUserId(), incoming.getClientOrderId()) != null) {
            throw new IllegalArgumentException("Duplicate order identity");
        }
        if (isAtCapacity()) throw new IllegalStateException("historical order capacity exceeded");
        MatchResult result = new MatchResult();

        if (incoming.getSide() == Side.BUY) {
            matchBuy(incoming, result, eventSequence);
        } else {
            matchSell(incoming, result, eventSequence);
        }

        if (incoming.getRemainingQuantity() > 0) {
            addToBook(incoming);
            result.setBooked(true);
        }

        register(incoming);
        result.setFinalOrder(incoming.copy());
        result.setMessage("ok");
        return result;
    }

    public void placeForBenchmark(Order incoming) {
        validateNewOrder(incoming);
        if (hasSeenOrderId(incoming.getOrderId())) throw new IllegalArgumentException("Duplicate server orderId");
        if (isAtCapacity()) throw new IllegalStateException("historical order capacity exceeded");
        if (incoming.getSide() == Side.BUY) {
            matchBuyNoResult(incoming);
        } else {
            matchSellNoResult(incoming);
        }
        if (incoming.getRemainingQuantity() > 0) {
            addToBook(incoming);
        }
        register(incoming);
    }

    public boolean cancel(String orderId) {
        OrderRef ref = orderIndex.remove(orderId);
        if (ref == null) {
            return false;
        }

        Order order = ref.node().order;
        NavigableMap<Long, PriceLevel> book = ref.side() == Side.BUY ? buyBook : sellBook;
        PriceLevel level = book.get(ref.price());
        if (level == null) {
            return false;
        }

        level.remove(ref.node());
        if (level.isEmpty()) {
            book.remove(ref.price());
        }

        order.cancel();
        return true;
    }

    public boolean hasOpenOrder(String orderId) {
        return orderIndex.containsKey(orderId);
    }

    public List<Order> exportOpenOrders() {
        List<Order> orders = new ArrayList<>(orderIndex.size());
        collectOrders(sellBook, orders);
        collectOrders(buyBook, orders);
        return orders;
    }

    public List<Order> exportAllOrders() {
        return allOrders.values().stream().map(Order::copy).toList();
    }

    public void restoreSnapshot(List<Order> openOrders, List<Order> historicalOrders) {
        if (openOrders == null || historicalOrders == null) {
            throw new IllegalArgumentException("snapshot requires open and historical orders");
        }
        clear();
        for (Order order : historicalOrders) {
            if (order == null || order.getStatus() == null || order.getQuantity() <= 0
                    || order.getRemainingQuantity() < 0 || order.getRemainingQuantity() > order.getQuantity()
                    || (order.getStatus() == OrderStatus.NEW && order.getRemainingQuantity() != order.getQuantity())
                    || (order.getStatus() == OrderStatus.PARTIALLY_FILLED
                    && (order.getRemainingQuantity() == 0 || order.getRemainingQuantity() == order.getQuantity()))
                    || (order.getStatus() == OrderStatus.FILLED && order.getRemainingQuantity() != 0)) {
                throw new IllegalStateException("Invalid historical order state in snapshot");
            }
            register(order.copy());
        }
        for (Order open : openOrders) {
            Order order = allOrders.get(open.getOrderId());
            if (order == null || order.getRemainingQuantity() != open.getRemainingQuantity()
                    || order.getStatus() != open.getStatus()
                    || !Objects.equals(order.getUserId(), open.getUserId())
                    || !Objects.equals(order.getSymbol(), open.getSymbol())
                    || order.getSide() != open.getSide()
                    || order.getPrice() != open.getPrice()
                    || order.getQuantity() != open.getQuantity()
                    || !Objects.equals(order.getClientOrderId(), open.getClientOrderId())) {
                throw new IllegalStateException("Open order differs from historical index: " + open.getOrderId());
            }
            if (order.getRemainingQuantity() <= 0
                    || (order.getStatus() != OrderStatus.NEW
                    && order.getStatus() != OrderStatus.PARTIALLY_FILLED)
                    || orderIndex.containsKey(order.getOrderId())) {
                throw new IllegalStateException("Invalid or duplicate open order in snapshot: " + open.getOrderId());
            }
            addToBook(order);
        }
        for (Order historical : allOrders.values()) {
            boolean shouldBeOpen = historical.getRemainingQuantity() > 0
                    && (historical.getStatus() == OrderStatus.NEW
                    || historical.getStatus() == OrderStatus.PARTIALLY_FILLED);
            if (shouldBeOpen != orderIndex.containsKey(historical.getOrderId())) {
                throw new IllegalStateException("Historical/open order mismatch in snapshot: "
                        + historical.getOrderId());
            }
        }
    }

    public List<String> snapshot() {
        List<String> lines = new ArrayList<>();
        lines.add("===== SELL BOOK =====");
        appendSnapshotLines(sellBook, "SELL", lines);
        lines.add("===== BUY BOOK =====");
        appendSnapshotLines(buyBook, "BUY", lines);
        return lines;
    }

    public int openOrderCount() {
        return orderIndex.size();
    }

    private void clear() {
        buyBook.clear();
        sellBook.clear();
        orderIndex.clear();
        allOrders.clear();
        clientOrderIndex.clear();
    }

    private void matchBuy(Order incoming, MatchResult result, long eventSequence) {
        while (incoming.getRemainingQuantity() > 0 && !sellBook.isEmpty()) {
            Map.Entry<Long, PriceLevel> bestAsk = sellBook.firstEntry();
            long bestAskPrice = bestAsk.getKey();

            if (incoming.getPrice() < bestAskPrice) {
                break;
            }

            OrderNode makerNode = bestAsk.getValue().head;
            Order maker = makerNode.order;
            execute(incoming, maker, bestAskPrice, result.getTrades(), eventSequence);

            if (maker.getRemainingQuantity() == 0) {
                removeFilled(bestAsk.getKey(), bestAsk.getValue(), makerNode, sellBook);
            }
        }
    }

    private void matchSell(Order incoming, MatchResult result, long eventSequence) {
        while (incoming.getRemainingQuantity() > 0 && !buyBook.isEmpty()) {
            Map.Entry<Long, PriceLevel> bestBid = buyBook.firstEntry();
            long bestBidPrice = bestBid.getKey();

            if (incoming.getPrice() > bestBidPrice) {
                break;
            }

            OrderNode makerNode = bestBid.getValue().head;
            Order maker = makerNode.order;
            execute(maker, incoming, bestBidPrice, result.getTrades(), eventSequence);

            if (maker.getRemainingQuantity() == 0) {
                removeFilled(bestBid.getKey(), bestBid.getValue(), makerNode, buyBook);
            }
        }
    }

    private void matchBuyNoResult(Order incoming) {
        while (incoming.getRemainingQuantity() > 0 && !sellBook.isEmpty()) {
            Map.Entry<Long, PriceLevel> bestAsk = sellBook.firstEntry();
            long bestAskPrice = bestAsk.getKey();

            if (incoming.getPrice() < bestAskPrice) {
                break;
            }

            OrderNode makerNode = bestAsk.getValue().head;
            Order maker = makerNode.order;
            executeNoResult(incoming, maker);

            if (maker.getRemainingQuantity() == 0) {
                removeFilled(bestAsk.getKey(), bestAsk.getValue(), makerNode, sellBook);
            }
        }
    }

    private void matchSellNoResult(Order incoming) {
        while (incoming.getRemainingQuantity() > 0 && !buyBook.isEmpty()) {
            Map.Entry<Long, PriceLevel> bestBid = buyBook.firstEntry();
            long bestBidPrice = bestBid.getKey();

            if (incoming.getPrice() > bestBidPrice) {
                break;
            }

            OrderNode makerNode = bestBid.getValue().head;
            Order maker = makerNode.order;
            executeNoResult(maker, incoming);

            if (maker.getRemainingQuantity() == 0) {
                removeFilled(bestBid.getKey(), bestBid.getValue(), makerNode, buyBook);
            }
        }
    }

    private void execute(Order buyOrder,
                         Order sellOrder,
                         long tradePrice,
                         List<Trade> trades,
                         long eventSequence) {
        long executedQty = Math.min(buyOrder.getRemainingQuantity(), sellOrder.getRemainingQuantity());

        buyOrder.reduceRemaining(executedQty);
        sellOrder.reduceRemaining(executedQty);

        trades.add(new Trade(
                eventSequence > 0
                        ? IdGenerator.tradeId(buyOrder.getSymbol(), eventSequence, trades.size() + 1)
                        : IdGenerator.transientTradeId(),
                buyOrder.getSymbol(),
                buyOrder.getOrderId(),
                sellOrder.getOrderId(),
                tradePrice,
                executedQty,
                System.currentTimeMillis()
        ));
    }

    private void executeNoResult(Order buyOrder, Order sellOrder) {
        long executedQty = Math.min(buyOrder.getRemainingQuantity(), sellOrder.getRemainingQuantity());

        buyOrder.reduceRemaining(executedQty);
        sellOrder.reduceRemaining(executedQty);
    }

    private void addToBook(Order order) {
        NavigableMap<Long, PriceLevel> book = order.getSide() == Side.BUY ? buyBook : sellBook;
        PriceLevel level = book.computeIfAbsent(order.getPrice(), ignored -> new PriceLevel());
        OrderNode node = level.addLast(order);
        orderIndex.put(order.getOrderId(), new OrderRef(order.getSide(), order.getPrice(), node));
    }

    private void removeFilled(long price,
                              PriceLevel level,
                              OrderNode node,
                              NavigableMap<Long, PriceLevel> book) {
        level.remove(node);
        orderIndex.remove(node.order.getOrderId());
        if (level.isEmpty()) {
            book.remove(price);
        }
    }

    private void appendSnapshotLines(NavigableMap<Long, PriceLevel> book,
                                     String side,
                                     List<String> lines) {
        for (Map.Entry<Long, PriceLevel> entry : book.entrySet()) {
            OrderNode current = entry.getValue().head;
            while (current != null) {
                Order order = current.order;
                lines.add(side + " price=" + order.getPrice()
                        + ", remain=" + order.getRemainingQuantity()
                        + ", id=" + order.getOrderId());
                current = current.next;
            }
        }
    }

    private void collectOrders(NavigableMap<Long, PriceLevel> book, List<Order> orders) {
        for (PriceLevel level : book.values()) {
            OrderNode current = level.head;
            while (current != null) {
                Order order = current.order;
                orders.add(order.copy());
                current = current.next;
            }
        }
    }

    private static final class PriceLevel {
        private OrderNode head;
        private OrderNode tail;

        private OrderNode addLast(Order order) {
            OrderNode node = new OrderNode(order);
            if (tail == null) {
                head = node;
                tail = node;
            } else {
                tail.next = node;
                node.prev = tail;
                tail = node;
            }
            return node;
        }

        private void remove(OrderNode node) {
            if (node.prev != null) {
                node.prev.next = node.next;
            } else {
                head = node.next;
            }

            if (node.next != null) {
                node.next.prev = node.prev;
            } else {
                tail = node.prev;
            }

            node.prev = null;
            node.next = null;
        }

        private boolean isEmpty() {
            return head == null;
        }
    }

    public static final class OrderNode {
        private final Order order;
        private OrderNode prev;
        private OrderNode next;

        private OrderNode(Order order) {
            this.order = order;
        }

        public Order getOrder() {
            return order;
        }
    }
}
