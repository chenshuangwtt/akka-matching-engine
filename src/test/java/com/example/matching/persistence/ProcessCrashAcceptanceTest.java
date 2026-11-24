package com.example.matching.persistence;

import static org.junit.jupiter.api.Assertions.*;

import com.example.matching.util.SymbolKeyUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class ProcessCrashAcceptanceTest {
    @TempDir Path temporaryRoot;

    @Test
    void crashBeforeJournalForce() throws Exception {
        verify("journal-before-force", false);
    }

    @Test
    void crashAfterJournalForceBeforeBusinessReply() throws Exception {
        verify("before-business-reply", false);
    }

    @Test
    void crashDuringTemporarySnapshotWrite() throws Exception {
        verify("snapshot-temp-partial", true);
    }

    @Test
    void crashAfterSnapshotCommitBeforeJournalCleanup() throws Exception {
        verify("snapshot-committed", true);
    }

    @Test
    void crashDuringJournalCleanupReplacement() throws Exception {
        verify("journal-cleanup-temp", true);
    }

    private void verify(String point, boolean expectPartialFill) throws Exception {
        Path caseDir = Files.createDirectory(temporaryRoot.resolve(point));
        Path data = Files.createDirectory(caseDir.resolve("data"));
        Path signals = Files.createDirectory(caseDir.resolve("signals"));
        Process crashed = start("crash", data, signals, point, caseDir.resolve("crash.log"));
        try {
            awaitFile(signals.resolve(point + ".reached"), crashed, Duration.ofSeconds(20));
            assertTrue(
                    Files.exists(signals.resolve("confirmed-1")),
                    "baseline order was not confirmed");
            assertEquals("2", Files.readString(signals.resolve(point + ".reached")));
        } finally {
            crashed.destroyForcibly();
            assertTrue(crashed.waitFor(10, TimeUnit.SECONDS));
        }
        if (point.equals("snapshot-temp-partial")) {
            Path temporarySnapshot =
                    data.resolve(SymbolKeyUtil.toPersistenceStem("BTCUSDT") + ".snapshot.json.tmp");
            assertTrue(Files.exists(temporarySnapshot));
            assertEquals(Files.getFileStore(data), Files.getFileStore(temporarySnapshot));
        }
        if (point.equals("journal-cleanup-temp")) {
            Path temporaryJournal =
                    data.resolve(SymbolKeyUtil.toPersistenceStem("BTCUSDT") + ".events.jsonl.tmp");
            assertTrue(Files.exists(temporaryJournal));
            assertEquals(Files.getFileStore(data), Files.getFileStore(temporaryJournal));
        }

        boolean secondConfirmed = Files.exists(signals.resolve("confirmed-2"));
        if (secondConfirmed) {
            assertEquals("3", Files.readString(signals.resolve("trade-2")));
        }
        if (point.equals("journal-before-force") || point.equals("before-business-reply")) {
            assertFalse(secondConfirmed, "reply crossed the crash barrier");
        }
        Process recovery = start("recover", data, signals, point, caseDir.resolve("recovery.log"));
        assertTrue(recovery.waitFor(20, TimeUnit.SECONDS), "recovery child timed out");
        assertEquals(0, recovery.exitValue(), Files.readString(caseDir.resolve("recovery.log")));
        Properties result = new Properties();
        try (var input = Files.newInputStream(signals.resolve("recovered.properties"))) {
            result.load(input);
        }
        String book = result.getProperty("book");
        String sequence = result.getProperty("sequence");
        long lastConfirmed = secondConfirmed ? 2 : 1;
        System.out.printf(
                "CRASH_CASE point=%s confirmed=%d recovered=%s snapshot=%s available=%s open=%s"
                        + " baseStatus=%s baseRemaining=%s replayedTradeQty=%s book=%s%n",
                point,
                lastConfirmed,
                sequence,
                result.getProperty("snapshotSequence"),
                result.getProperty("available"),
                result.getProperty("openCount"),
                result.getProperty("baseStatus"),
                result.getProperty("baseRemaining"),
                result.getProperty("replayedTradeQuantity"),
                book);

        if (expectPartialFill || point.equals("before-business-reply")) {
            assertEquals("true", result.getProperty("available"));
            assertEquals("2", sequence);
            assertTrue(book.contains("id=BASE") && book.contains("remain=2"), book);
            assertFalse(book.contains("id=CRASH"), book);
            assertEquals("1", result.getProperty("openCount"));
            assertEquals("2", result.getProperty("baseRemaining"));
            assertEquals("PARTIALLY_FILLED", result.getProperty("baseStatus"));
            if ("0".equals(result.getProperty("snapshotSequence"))) {
                assertEquals("3", result.getProperty("replayedTradeQuantity"));
                assertEquals("FILLED", result.getProperty("crashStatus"));
            }
        } else if ("true".equals(result.getProperty("available"))) {
            // The unacknowledged write may or may not have reached stable storage.
            assertTrue(sequence.equals("1") || sequence.equals("2"));
            assertTrue(book.contains("id=BASE"));
            assertTrue(book.contains(sequence.equals("1") ? "remain=5" : "remain=2"), book);
            assertEquals(sequence.equals("1") ? "5" : "2", result.getProperty("baseRemaining"));
            assertEquals(
                    sequence.equals("1") ? "NEW" : "PARTIALLY_FILLED",
                    result.getProperty("baseStatus"));
        } else {
            assertEquals("UNKNOWN", sequence, "unrecoverable journal must fail closed");
            assertTrue(book.contains("SERVICE_UNAVAILABLE:"), book);
        }
        if ("true".equals(result.getProperty("available"))) {
            assertEquals("2", result.getProperty("postRetrySequence"));
            if ("2".equals(sequence)) {
                assertEquals("CRASH", result.getProperty("retryOrderId"));
                assertEquals("0", result.getProperty("retryTradeQuantity"));
                assertTrue(result.getProperty("retryMessage").startsWith("already accepted"));
            } else {
                assertEquals("RETRY", result.getProperty("retryOrderId"));
                assertEquals("3", result.getProperty("retryTradeQuantity"));
            }
        }
        if (expectPartialFill) {
            assertEquals(
                    point.equals("snapshot-temp-partial") ? "0" : "2",
                    result.getProperty("snapshotSequence"));
        }
    }

    private Process start(String mode, Path data, Path signals, String point, Path log)
            throws Exception {
        Path java = Path.of(System.getProperty("java.home"), "bin", "java.exe");
        return new ProcessBuilder(
                        java.toString(),
                        "-cp",
                        System.getProperty("java.class.path"),
                        CrashProcessWorker.class.getName(),
                        mode,
                        data.toString(),
                        signals.toString(),
                        point)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
    }

    private void awaitFile(Path marker, Process child, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(marker)) {
                return;
            }
            if (!child.isAlive()) {
                fail("child exited before barrier: " + child.exitValue());
            }
            Thread.sleep(10); // polling only; the marker, not elapsed time, selects the crash point
        }
        fail("barrier not reached: " + marker);
    }
}
