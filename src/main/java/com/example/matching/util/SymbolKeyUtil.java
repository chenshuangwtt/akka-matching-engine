package com.example.matching.util;

import java.util.Locale;

public final class SymbolKeyUtil {

    private SymbolKeyUtil() {}

    public static String toSafeKey(String symbol) {
        String normalized = symbol == null ? "" : symbol.trim().toLowerCase(Locale.ROOT);
        StringBuilder builder = new StringBuilder();
        boolean lastWasUnderscore = false;
        for (int i = 0; i < normalized.length(); i++) {
            char ch = normalized.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                builder.append(ch);
                lastWasUnderscore = false;
            } else if (!lastWasUnderscore) {
                builder.append('_');
                lastWasUnderscore = true;
            }
        }

        String base = builder.toString().replaceAll("^_+|_+$", "");
        if (base.isBlank()) {
            base = "symbol";
        }
        String hash = Integer.toUnsignedString(normalized.hashCode(), 16);
        return base + "-" + hash;
    }

    public static String toActorName(String symbol) {
        return "symbol-" + toSafeKey(symbol);
    }

    public static String toPersistenceStem(String symbol) {
        return toSafeKey(symbol);
    }
}
