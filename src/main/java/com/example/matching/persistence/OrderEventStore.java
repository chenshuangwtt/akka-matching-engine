package com.example.matching.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.matching.util.SymbolKeyUtil;
import com.example.matching.domain.Order;
import com.example.matching.domain.OrderStatus;
import com.example.matching.domain.OrderRules;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class OrderEventStore {

    public static final class RecoveryState {
        private final OrderBookSnapshot snapshot;
        private final List<OrderEvent> events;
        private final long lastSequence;

        public RecoveryState(OrderBookSnapshot snapshot, List<OrderEvent> events, long lastSequence) {
            this.snapshot = snapshot;
            this.events = events;
            this.lastSequence = lastSequence;
        }

        public OrderBookSnapshot snapshot() {
            return snapshot;
        }

        public List<OrderEvent> events() {
            return events;
        }

        public long lastSequence() { return lastSequence; }
    }

    private static final int DEFAULT_SNAPSHOT_THRESHOLD = 1_000;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Path eventFile;
    private final Path snapshotFile;
    private final String symbol;
    private final int snapshotThreshold;
    private int pendingEvents;
    private long lastSequence;

    public OrderEventStore(Path baseDir, String symbol) {
        this(baseDir, symbol, DEFAULT_SNAPSHOT_THRESHOLD);
    }

    public OrderEventStore(Path baseDir, String symbol, int snapshotThreshold) {
        this.symbol = symbol;
        String fileStem = SymbolKeyUtil.toPersistenceStem(symbol);
        this.eventFile = baseDir.resolve(fileStem + ".events.jsonl");
        this.snapshotFile = baseDir.resolve(fileStem + ".snapshot.json");
        this.snapshotThreshold = snapshotThreshold;
        try {
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create persistence directory: " + baseDir, e);
        }
        this.pendingEvents = countPendingEvents();
    }

    public RecoveryState loadRecoveryState() {
        OrderBookSnapshot snapshot = loadSnapshot();
        List<OrderEvent> events = loadEvents(snapshot);
        long checkpoint = snapshot == null ? 0 : snapshot.getLastAppliedSequence();
        this.lastSequence = events.isEmpty() ? checkpoint : events.get(events.size() - 1).getSequence();
        this.pendingEvents = events.size();
        return new RecoveryState(snapshot, events.stream().filter(e -> e.getSequence() > checkpoint).toList(), lastSequence);
    }

    public long appendPlace(com.example.matching.domain.Order order) {
        OrderRules.validateNewOrder(order);
        if (!symbol.equals(order.getSymbol())) throw new IllegalArgumentException("order symbol differs from journal symbol");
        return append(OrderEvent.place(order));
    }

    public long appendCancel(String symbol, String orderId) {
        OrderRules.validateSymbol(symbol);
        OrderRules.validateId(orderId, "orderId");
        if (!this.symbol.equals(symbol)) throw new IllegalArgumentException("cancel symbol differs from journal symbol");
        return append(OrderEvent.cancel(symbol, orderId));
    }

    public void compact(OrderBookSnapshot snapshot) {
        try {
            if (snapshot.getFormatVersion() != OrderEvent.FORMAT_VERSION
                    || snapshot.getAllOrders() == null || snapshot.getOpenOrders() == null
                    || snapshot.getLastAppliedSequence() == null || snapshot.getLastAppliedSequence() != lastSequence) {
                throw new IllegalStateException("Snapshot sequence does not match journal head");
            }
            replaceAtomically(snapshotFile, objectMapper.writeValueAsBytes(snapshot), true);
            FaultInjectionBarrier.stopAt("snapshot-committed", lastSequence);
            // The old complete journal remains recoverable until the snapshot replacement succeeds.
            replaceAtomically(eventFile, new byte[0], false);
            pendingEvents = 0;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to compact persistence files for " + eventFile, e);
        }
    }

    public boolean shouldCompact() {
        return pendingEvents >= snapshotThreshold;
    }

    public boolean hasPendingEvents() {
        return pendingEvents > 0;
    }

    public int getPendingEvents() {
        return pendingEvents;
    }

    private OrderBookSnapshot loadSnapshot() {
        if (!Files.exists(snapshotFile)) {
            return null;
        }

        try {
            String content = Files.readString(snapshotFile, StandardCharsets.UTF_8);
            if (content.isBlank()) throw new IllegalStateException("Existing snapshot is empty: " + snapshotFile);
            if (objectMapper.readTree(content).path("formatVersion").asInt(0) != OrderEvent.FORMAT_VERSION) {
                throw new IllegalStateException("Incompatible snapshot format version: " + snapshotFile);
            }
            OrderBookSnapshot snapshot = objectMapper.readValue(content, OrderBookSnapshot.class);
            if (snapshot.getLastAppliedSequence() == null || snapshot.getLastAppliedSequence() < 0
                    || snapshot.getOpenOrders() == null || snapshot.getAllOrders() == null) {
                throw new IllegalStateException("Incomplete snapshot: " + snapshotFile);
            }
                Set<String> historicalIds = new HashSet<>();
                Set<String> clientKeys = new HashSet<>();
                for (Order order : snapshot.getAllOrders()) {
                    if (order == null || !symbol.equals(order.getSymbol()) || order.getOrderId() == null
                            || !historicalIds.add(order.getOrderId()) || order.getSide() == null
                            || order.getQuantity() <= 0 || order.getRemainingQuantity() < 0
                            || order.getRemainingQuantity() > order.getQuantity() || order.getStatus() == null
                            || order.getClientOrderId() == null || order.getClientOrderId().isBlank()
                            || order.getUserId() == null || order.getUserId().isBlank()) {
                        throw new IllegalStateException("Invalid historical order in snapshot: " + snapshotFile);
                    }
                    if (!clientKeys.add(order.getUserId() + "\u0000" + order.getClientOrderId())) {
                        throw new IllegalStateException("Duplicate client identity in snapshot: " + snapshotFile);
                    }
                }
            Set<String> ids = new HashSet<>();
            for (Order order : snapshot.getOpenOrders()) {
                if (order == null || !symbol.equals(order.getSymbol()) || order.getOrderId() == null
                        || !ids.add(order.getOrderId()) || order.getSide() == null
                        || order.getQuantity() <= 0 || order.getRemainingQuantity() <= 0
                        || order.getRemainingQuantity() > order.getQuantity()
                        || (order.getStatus() != OrderStatus.NEW && order.getStatus() != OrderStatus.PARTIALLY_FILLED)) {
                    throw new IllegalStateException("Invalid open order in snapshot: " + snapshotFile);
                }
            }
            return snapshot;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load snapshot from " + snapshotFile, e);
        }
    }

    private List<OrderEvent> loadEvents(OrderBookSnapshot snapshot) {
        if (!Files.exists(eventFile)) {
            if (snapshot != null) throw new IllegalStateException("Journal missing after checkpoint: " + eventFile);
            return List.of();
        }

        try {
            String content = Files.readString(eventFile, StandardCharsets.UTF_8);
            if (!content.isEmpty() && !content.endsWith("\n")) {
                throw new IllegalStateException("Incomplete journal tail: " + eventFile);
            }
            List<OrderEvent> events = new ArrayList<>();
            long checkpoint = snapshot == null ? 0 : snapshot.getLastAppliedSequence();
            long expected = 0;
            for (String line : content.split("\n")) {
                if (line.isEmpty()) continue;
                if (line.isBlank()) throw new IllegalStateException("Blank journal record: " + eventFile);
                if (objectMapper.readTree(line).path("formatVersion").asInt(0) != OrderEvent.FORMAT_VERSION) {
                    throw new IllegalStateException("Incompatible journal format version: " + eventFile);
                }
                OrderEvent event = objectMapper.readValue(line, OrderEvent.class);
                if (event.getType() == null || event.getOrderId() == null || !symbol.equals(event.getSymbol())) {
                    throw new IllegalStateException("Invalid journal event: " + eventFile);
                }
                if (event.getSequence() == null) {
                    throw new IllegalStateException("Journal event is missing its sequence: " + eventFile);
                }
                if (expected == 0 && checkpoint > 0 && event.getSequence() == checkpoint + 1) {
                    expected = checkpoint;
                }
                if (event.getSequence() != expected + 1) {
                    throw new IllegalStateException("Duplicate or out-of-sequence journal event at " + event.getSequence());
                }
                expected = event.getSequence();
                if (event.getType() == OrderEvent.Type.PLACE) {
                    try {
                        OrderRules.validateNewOrder(event.toOrder());
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalStateException("Invalid place event: " + eventFile, ex);
                    }
                } else {
                    try {
                        OrderRules.validateId(event.getOrderId(), "orderId");
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalStateException("Invalid cancel event: " + eventFile, ex);
                    }
                }
                events.add(event);
            }
            if (!events.isEmpty() && events.get(events.size() - 1).getSequence() < checkpoint) {
                throw new IllegalStateException("Journal ends before snapshot position: " + eventFile);
            }
            return events;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load events from " + eventFile, e);
        }
    }

    private long append(OrderEvent event) {
        try {
            event.setSequence(lastSequence + 1);
            String line = objectMapper.writeValueAsString(event) + System.lineSeparator();
            byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
            boolean newlyCreated = !Files.exists(eventFile);
            try (FileChannel channel = FileChannel.open(eventFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                FaultInjectionBarrier.stopAt("journal-before-force", event.getSequence());
                channel.force(true);
            }
            if (newlyCreated) forceDirectoryIfSupported(eventFile.getParent());
            pendingEvents++;
            lastSequence = event.getSequence();
            return lastSequence;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to append event to " + eventFile, e);
        }
    }

    private void replaceAtomically(Path destination, byte[] content, boolean validateSnapshot) throws IOException {
        Path temporary = destination.resolveSibling(destination.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            if (validateSnapshot && FaultInjectionBarrier.enabled("snapshot-temp-partial")) {
                int firstPart = Math.max(1, content.length / 2);
                buffer.limit(firstPart);
                while (buffer.hasRemaining()) channel.write(buffer);
                FaultInjectionBarrier.stopAt("snapshot-temp-partial", lastSequence);
                buffer.limit(content.length);
            }
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        if (!Arrays.equals(content, Files.readAllBytes(temporary))) {
            throw new IOException("Temporary file verification failed: " + temporary);
        }
        if (validateSnapshot) {
            OrderBookSnapshot verified = objectMapper.readValue(temporary.toFile(), OrderBookSnapshot.class);
            if (verified.getLastAppliedSequence() == null || verified.getLastAppliedSequence() != lastSequence
                    || verified.getOpenOrders() == null || verified.getAllOrders() == null
                    || verified.getFormatVersion() != OrderEvent.FORMAT_VERSION) {
                throw new IOException("Temporary snapshot verification failed: " + temporary);
            }
        }
        if (!validateSnapshot) FaultInjectionBarrier.stopAt("journal-cleanup-temp", lastSequence);
        try {
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ex) {
            throw new IOException("Atomic replacement is unavailable for " + destination, ex);
        }
        forceDirectoryIfSupported(destination.getParent());
    }

    private void forceDirectoryIfSupported(Path directory) throws IOException {
        // Windows does not expose portable directory fsync through FileChannel.
        // File contents are forced; metadata durability after sudden power loss is not guaranteed.
        if (System.getProperty("os.name").startsWith("Windows")) return;
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private int countPendingEvents() {
        if (!Files.exists(eventFile)) {
            return 0;
        }

        try {
            int count = 0;
            for (String line : Files.readAllLines(eventFile, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    count++;
                }
            }
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to inspect events from " + eventFile, e);
        }
    }
}
