package com.example.gsb.cache;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存块存储，每次读取注入固定延迟以模拟慢速底层设备，并统计底层读取次数。
 * 读路径无锁（数据初始化后只读或按块整体替换），写路径按块加锁。
 */
public class LatencyBlockStore implements BlockStore {

    private final int blockSize;
    private final byte[][] blocks;
    private final long readLatencyMillis;
    private final AtomicLong readCount = new AtomicLong();

    public LatencyBlockStore(long blockCount, int blockSize, long readLatencyMillis) {
        if (blockCount < 1 || blockSize < 1 || readLatencyMillis < 0) {
            throw new IllegalArgumentException();
        }
        this.blockSize = blockSize;
        this.blocks = new byte[(int) blockCount][];
        for (int i = 0; i < blockCount; i++) {
            byte[] data = new byte[blockSize];
            // 每块填入与块号相关的可校验内容
            Arrays.fill(data, (byte) (i * 31 + 7));
            blocks[i] = data;
        }
        this.readLatencyMillis = readLatencyMillis;
    }

    @Override
    public int blockSize() {
        return blockSize;
    }

    @Override
    public long blockCount() {
        return blocks.length;
    }

    @Override
    public byte[] readBlock(long blockIndex) throws IOException {
        checkIndex(blockIndex);
        readCount.incrementAndGet();
        sleep();
        return Arrays.copyOf(blocks[(int) blockIndex], blockSize);
    }

    @Override
    public void writeBlock(long blockIndex, byte[] data) throws IOException {
        checkIndex(blockIndex);
        if (data.length != blockSize) {
            throw new IOException("data length " + data.length + " != blockSize " + blockSize);
        }
        synchronized (blocks) {
            blocks[(int) blockIndex] = Arrays.copyOf(data, blockSize);
        }
    }

    /** 底层实际被读取的次数。 */
    public long readCount() {
        return readCount.get();
    }

    private void checkIndex(long blockIndex) throws IOException {
        if (blockIndex < 0 || blockIndex >= blocks.length) {
            throw new IOException("block index out of range: " + blockIndex);
        }
    }

    private void sleep() throws IOException {
        if (readLatencyMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(readLatencyMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted during simulated latency", e);
        }
    }
}
