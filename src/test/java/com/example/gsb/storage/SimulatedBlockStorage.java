package com.example.gsb.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link BlockStorage} with a configurable per-read latency, used to emulate a
 * slow disk and to count how often the backend is actually touched.
 */
final class SimulatedBlockStorage implements BlockStorage {

    private final int blockSize;
    private final long readLatencyMillis;
    private final Map<Long, byte[]> blocks = new ConcurrentHashMap<>();
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    SimulatedBlockStorage(int blockSize, long readLatencyMillis) {
        this.blockSize = blockSize;
        this.readLatencyMillis = readLatencyMillis;
    }

    @Override
    public byte[] readBlock(long blockId) {
        readCount.incrementAndGet();
        sleep();
        return blocks.computeIfAbsent(blockId, this::defaultContents).clone();
    }

    @Override
    public void writeBlock(long blockId, byte[] data) {
        writeCount.incrementAndGet();
        blocks.put(blockId, data.clone());
    }

    long readCount() {
        return readCount.get();
    }

    long writeCount() {
        return writeCount.get();
    }

    private byte[] defaultContents(long blockId) {
        byte[] data = new byte[blockSize];
        for (int i = 0; i < blockSize; i++) {
            data[i] = (byte) (blockId * 31 + i);
        }
        return data;
    }

    private void sleep() {
        if (readLatencyMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(readLatencyMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted during simulated read", e);
        }
    }
}
