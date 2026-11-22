package com.example.matching.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

public class IdGenerator {
    public static String tradeId(String symbol, long eventSequence, int matchOrdinal) {
        if (symbol == null || symbol.isBlank() || eventSequence <= 0 || matchOrdinal <= 0) {
            throw new IllegalArgumentException("trade identity requires symbol, event sequence and match ordinal");
        }
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(symbol.getBytes(StandardCharsets.UTF_8));
        return "T:" + encoded + ":" + eventSequence + ":" + matchOrdinal;
    }

    public static String transientTradeId() {
        return "U:" + UUID.randomUUID();
    }
}
