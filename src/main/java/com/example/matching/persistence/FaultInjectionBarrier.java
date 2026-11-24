package com.example.matching.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/** Deterministic crash-test barrier, active only in an explicitly configured child JVM. */
public final class FaultInjectionBarrier {
    private static final String POINT = "matching.test.faultPoint";
    private static final String SIGNAL_DIR = "matching.test.signalDir";
    private static final ConcurrentHashMap<String, CountDownLatch> releases =
            new ConcurrentHashMap<>();

    private FaultInjectionBarrier() {}

    public static boolean enabled(String point) {
        return point.equals(System.getProperty(POINT)) && System.getProperty(SIGNAL_DIR) != null;
    }

    public static void stopAt(String point, long sequence) {
        if (!enabled(point)) {
            return;
        }
        try {
            Path signal = Path.of(System.getProperty(SIGNAL_DIR)).resolve(point + ".reached");
            Files.writeString(signal, Long.toString(sequence));
            if (Boolean.getBoolean("matching.test.releasable")) {
                releases.computeIfAbsent(point, ignored -> new CountDownLatch(1)).await();
            } else {
                new CountDownLatch(1).await();
            }
        } catch (IOException | InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Fault injection barrier failed at " + point, ex);
        }
    }

    public static void release(String point) {
        CountDownLatch latch = releases.remove(point);
        if (latch != null) {
            latch.countDown();
        }
    }

    public static void crashAt(String point, long sequence) {
        if (!enabled(point)) {
            return;
        }
        try {
            Path signal = Path.of(System.getProperty(SIGNAL_DIR)).resolve(point + ".reached");
            Files.writeString(signal, Long.toString(sequence));
        } catch (IOException ex) {
            throw new IllegalStateException("Fault injection marker failed at " + point, ex);
        }
        throw new IllegalStateException("Injected actor failure at " + point);
    }

    public static void fatalAt(String point, long sequence) {
        if (!enabled(point)) {
            return;
        }
        try {
            Files.writeString(
                    Path.of(System.getProperty(SIGNAL_DIR)).resolve(point + ".reached"),
                    Long.toString(sequence));
        } catch (IOException ex) {
            throw new IllegalStateException("Fault injection marker failed at " + point, ex);
        }
        throw new AssertionError("Injected fatal actor stop at " + point);
    }
}
