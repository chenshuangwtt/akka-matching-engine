package com.example.matching.persistence;

import com.example.matching.domain.Order;
import com.example.matching.domain.Side;
import com.example.matching.engine.OrderBook;
import com.example.matching.engine.MatchResult;
import com.example.matching.util.SymbolKeyUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.io.UncheckedIOException;

import static org.junit.jupiter.api.Assertions.*;

class OrderEventStoreCheckpointTest {
    @TempDir Path dir;

    private Path journal() { return dir.resolve(SymbolKeyUtil.toPersistenceStem("BTCUSDT") + ".events.jsonl"); }
    private Path snapshot() { return dir.resolve(SymbolKeyUtil.toPersistenceStem("BTCUSDT") + ".snapshot.json"); }
    private OrderEventStore store() { return new OrderEventStore(dir, "BTCUSDT", 2); }
    private Order order(String id, Side side, long quantity) {
        return new Order(id, "U", "BTCUSDT", side, 10000, quantity, 1, id);
    }

    @Test void tradeIdsRemainDistinctAfterRepeatedCheckpointRecovery() {
        OrderEventStore initial = store();
        OrderBook firstBook = new OrderBook();
        Order buy = order("B", Side.BUY, 3);
        firstBook.place(buy, initial.appendPlace(buy));
        Order sell1 = order("S1", Side.SELL, 1);
        MatchResult firstTrade = firstBook.place(sell1, initial.appendPlace(sell1));
        String id1 = firstTrade.getTrades().get(0).getTradeId();
        checkpoint(initial, firstBook, 2);

        OrderEventStore secondStore = store();
        var secondRecovery = secondStore.loadRecoveryState();
        assertEquals(2, secondRecovery.snapshot().getLastAppliedSequence());
        OrderBook secondBook = new OrderBook();
        secondBook.restoreSnapshot(secondRecovery.snapshot().getOpenOrders(), secondRecovery.snapshot().getAllOrders());
        Order sell2 = order("S2", Side.SELL, 1);
        String id2 = secondBook.place(sell2, secondStore.appendPlace(sell2)).getTrades().get(0).getTradeId();
        checkpoint(secondStore, secondBook, 3);

        OrderEventStore thirdStore = store();
        var thirdRecovery = thirdStore.loadRecoveryState();
        assertEquals(3, thirdRecovery.snapshot().getLastAppliedSequence());
        OrderBook thirdBook = new OrderBook();
        thirdBook.restoreSnapshot(thirdRecovery.snapshot().getOpenOrders(), thirdRecovery.snapshot().getAllOrders());
        Order sell3 = order("S3", Side.SELL, 1);
        String id3 = thirdBook.place(sell3, thirdStore.appendPlace(sell3)).getTrades().get(0).getTradeId();
        assertEquals("T:QlRDVVNEVA:2:1", id1);
        assertEquals("T:QlRDVVNEVA:3:1", id2);
        assertEquals("T:QlRDVVNEVA:4:1", id3);
        assertEquals(0, thirdBook.openOrderCount());
        assertEquals(4, store().loadRecoveryState().lastSequence());
    }

    private void checkpoint(OrderEventStore store, OrderBook book, long sequence) {
        OrderBookSnapshot snapshot = new OrderBookSnapshot();
        snapshot.setFormatVersion(OrderEvent.FORMAT_VERSION);
        snapshot.setLastAppliedSequence(sequence);
        snapshot.setProcessedCount(sequence);
        snapshot.setOpenOrders(book.exportOpenOrders());
        snapshot.setAllOrders(book.exportAllOrders());
        store.compact(snapshot);
    }

    @Test
    void shouldSkipJournalAlreadyIncludedInSnapshot() throws Exception {
        OrderEventStore store = store();
        OrderBook book = new OrderBook();
        Order buy = order("B", Side.BUY, 5);
        Order sell = order("S", Side.SELL, 3);
        assertEquals(1, store.appendPlace(buy));
        book.place(buy);
        assertEquals(2, store.appendPlace(sell));
        book.place(sell);
        assertEquals(2, buy.getRemainingQuantity());
        String fullJournal = Files.readString(journal());
        OrderBookSnapshot checkpoint = new OrderBookSnapshot();
        checkpoint.setFormatVersion(OrderEvent.FORMAT_VERSION);
        checkpoint.setProcessedCount(2);
        checkpoint.setLastAppliedSequence(2L);
        checkpoint.setOpenOrders(book.exportOpenOrders());
        checkpoint.setAllOrders(book.exportAllOrders());
        store.compact(checkpoint);
        Files.writeString(journal(), fullJournal, StandardCharsets.UTF_8); // crash after snapshot replacement
        Files.writeString(journal().resolveSibling(journal().getFileName() + ".tmp"), "{incomplete");

        for (int i = 0; i < 3; i++) {
            OrderEventStore.RecoveryState recovered = store().loadRecoveryState();
            assertEquals(2, recovered.lastSequence());
            assertTrue(recovered.events().isEmpty());
            OrderBook rebuilt = new OrderBook();
            rebuilt.restoreSnapshot(recovered.snapshot().getOpenOrders(), recovered.snapshot().getAllOrders());
            assertEquals(1, rebuilt.openOrderCount());
            assertTrue(rebuilt.snapshot().stream().anyMatch(line -> line.contains("id=B") && line.contains("remain=2")));
            assertFalse(rebuilt.snapshot().stream().anyMatch(line -> line.contains("id=S")));
        }
    }

    @Test
    void shouldIgnoreInterruptedTemporarySnapshotAndJournalReplacement() throws Exception {
        OrderEventStore store = store();
        store.appendPlace(order("B", Side.BUY, 5));
        Files.writeString(snapshot().resolveSibling(snapshot().getFileName() + ".tmp"), "{broken");
        Files.writeString(journal().resolveSibling(journal().getFileName() + ".tmp"), "{broken");
        OrderEventStore.RecoveryState recovered = store().loadRecoveryState();
        assertNull(recovered.snapshot());
        assertEquals(1, recovered.events().size());
        assertEquals("B", recovered.events().get(0).getOrderId());
    }

    @Test
    void shouldRejectIncompleteDuplicateAndGapJournalRecords() throws Exception {
        OrderEventStore store = store();
        store.appendPlace(order("B", Side.BUY, 5));
        String first = Files.readString(journal());
        Files.writeString(journal(), first + "{\"type\":\"PLACE\"", StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> store().loadRecoveryState());
        Files.writeString(journal(), first + first, StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> store().loadRecoveryState());
        Files.writeString(journal(), first + first.replace("\"sequence\":1", "\"sequence\":3"), StandardCharsets.UTF_8);
        assertThrows(IllegalStateException.class, () -> store().loadRecoveryState());
    }

    @Test
    void shouldRejectIncompatibleJournalWithoutChangingIt() throws Exception {
        String oldRecord = "{\"type\":\"PLACE\",\"sequence\":1,\"symbol\":\"BTCUSDT\",\"orderId\":\"OLD\"}\n";
        Files.writeString(journal(), oldRecord);
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> store().loadRecoveryState());
        assertTrue(error.getMessage().contains("Incompatible journal format version"));
        assertEquals(oldRecord, Files.readString(journal()));
    }

    @Test
    void shouldRejectIncompatibleSnapshotWithoutChangingIt() throws Exception {
        OrderEventStore store = store();
        store.appendPlace(order("CURRENT", Side.BUY, 2));
        String oldSnapshot = "{\"schemaVersion\":2,\"lastAppliedSequence\":1,\"openOrders\":[],\"allOrders\":[]}";
        Files.writeString(snapshot(), oldSnapshot);
        IllegalStateException error = assertThrows(IllegalStateException.class, () -> store().loadRecoveryState());
        assertTrue(error.getMessage().contains("Incompatible snapshot format version"));
        assertEquals(oldSnapshot, Files.readString(snapshot()));
        assertTrue(Files.readString(journal()).contains("CURRENT"));
    }

    @Test
    void openJournalHandleDoesNotCauseSilentLossDuringWindowsReplacement() throws Exception {
        OrderEventStore store = store();
        Order order = order("B", Side.BUY, 5);
        store.appendPlace(order);
        OrderBookSnapshot checkpoint = new OrderBookSnapshot();
        checkpoint.setFormatVersion(OrderEvent.FORMAT_VERSION);
        checkpoint.setProcessedCount(1);
        checkpoint.setLastAppliedSequence(1L);
        checkpoint.setOpenOrders(java.util.List.of(order));
        checkpoint.setAllOrders(java.util.List.of(order));
        boolean replaced;
        try (FileChannel openReader = FileChannel.open(journal(), StandardOpenOption.READ)) {
            try {
                store.compact(checkpoint);
                replaced = true;
            } catch (UncheckedIOException ex) {
                replaced = false; // Windows may prohibit replacement of an open file.
            }
        }
        assertEquals(dir, snapshot().resolveSibling(snapshot().getFileName() + ".tmp").getParent());
        assertEquals(dir, journal().resolveSibling(journal().getFileName() + ".tmp").getParent());
        assertEquals(Files.getFileStore(dir), Files.getFileStore(journal()));
        if (Files.exists(snapshot())) assertEquals(Files.getFileStore(dir), Files.getFileStore(snapshot()));
        OrderEventStore.RecoveryState recovered = store().loadRecoveryState();
        assertEquals(1, recovered.lastSequence());
        OrderBook rebuilt = new OrderBook();
        if (recovered.snapshot() != null) rebuilt.restoreSnapshot(recovered.snapshot().getOpenOrders(), recovered.snapshot().getAllOrders());
        recovered.events().forEach(event -> rebuilt.place(event.toOrder(), event.getSequence()));
        assertTrue(rebuilt.snapshot().stream().anyMatch(line -> line.contains("id=B") && line.contains("remain=5")));
        System.out.println("WINDOWS_OPEN_HANDLE_REPLACE succeeded=" + replaced + " recoveredSequence=1");
    }
}
