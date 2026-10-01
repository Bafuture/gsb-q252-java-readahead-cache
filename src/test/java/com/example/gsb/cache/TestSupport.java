package com.example.gsb.cache;

import java.util.function.BooleanSupplier;

final class TestSupport {

    private TestSupport() {
    }

    /** 轮询等待异步条件成立，超时则失败。 */
    static void awaitTrue(BooleanSupplier condition, long timeoutMillis, String description) {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for: " + description);
            }
        }
        throw new AssertionError("timed out waiting for: " + description);
    }
}
