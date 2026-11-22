package com.example.matching.util;

import org.slf4j.MDC;

public final class MdcUtil {

    private static final String REQUEST_ID = "requestId";
    private static final String CLIENT_ORDER_ID = "clientOrderId";

    private MdcUtil() {
    }

    public static Scope withRequest(String requestId) {
        return with(requestId, null);
    }

    public static Scope with(String requestId, String clientOrderId) {
        String previousRequestId = MDC.get(REQUEST_ID);
        String previousClientOrderId = MDC.get(CLIENT_ORDER_ID);

        putOrRemove(REQUEST_ID, requestId);
        putOrRemove(CLIENT_ORDER_ID, clientOrderId);

        return new Scope(previousRequestId, previousClientOrderId);
    }

    private static void putOrRemove(String key, String value) {
        if (value == null || value.isBlank()) {
            MDC.remove(key);
            return;
        }
        MDC.put(key, value);
    }

    public static final class Scope implements AutoCloseable {
        private final String previousRequestId;
        private final String previousClientOrderId;

        private Scope(String previousRequestId, String previousClientOrderId) {
            this.previousRequestId = previousRequestId;
            this.previousClientOrderId = previousClientOrderId;
        }

        @Override
        public void close() {
            putOrRemove(REQUEST_ID, previousRequestId);
            putOrRemove(CLIENT_ORDER_ID, previousClientOrderId);
        }
    }
}
