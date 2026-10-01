package com.example.gsb.cache;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 带 LRU 读缓存与自适应顺序预读的 {@link BlockStore} 装饰器。
 *
 * <p>核心行为：
 * <ul>
 *   <li>按块缓存，容量可配，访问顺序 LRU 淘汰；</li>
 *   <li>顺序模式识别：当前访问块号恰好等于上次访问块号 +1 时，连续步进计数 streak
 *       加一，否则清零；streak 达到 {@code sequentialThreshold}（默认 2）即判定为
 *       顺序流，对后续 {@code depth} 个块注册预读。每次顺序访问（含缓存命中）都会
 *       重新推进预读窗口，使流水线保持预热；</li>
 *   <li>并发去重 + 预读统一走 in-flight 表：预读在触发时（调用线程上）同步把
 *       Future 注册进 inFlight 表，实际读取异步执行。因此同一块的并发需求读、
 *       重复触发的预读永远共享同一次底层读取，也不会产生“入队后重复读取”；</li>
 *   <li>预读深度自适应：以 {@code adjustmentWindow}（默认 16）次“预读观察”为窗口
 *       （预读命中 + 无效预读），窗口内有效率 &ge; 60% 则加深（最大 maxDepth），
 *       &le; 30% 则减半（最小 minDepth）。随机访问下 streak 难以达到阈值，几乎不
 *       触发预读；偶发预读若大多无效，深度也会被自动压低；</li>
 *   <li>一致性：{@link #writeBlock} 写穿到底层并使对应缓存项失效；每次加载携带块
 *       版本号，失效前发起的在途读取完成后会因版本不匹配被丢弃，旧数据不会写回缓存；</li>
 *   <li>统计：命中率、预读命中率、无效预读数、底层读取次数，见 {@link CacheStats}。</li>
 * </ul>
 */
public class CachedBlockStore implements BlockStore, AutoCloseable {

    /** 预读深度自适应配置。 */
    public static final class ReadaheadPolicy {
        private final int sequentialThreshold;
        private final int initialDepth;
        private final int minDepth;
        private final int maxDepth;
        private final int adjustmentWindow;

        private ReadaheadPolicy(Builder b) {
            this.sequentialThreshold = b.sequentialThreshold;
            this.initialDepth = b.initialDepth;
            this.minDepth = b.minDepth;
            this.maxDepth = b.maxDepth;
            this.adjustmentWindow = b.adjustmentWindow;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static ReadaheadPolicy defaults() {
            return builder().build();
        }

        public static final class Builder {
            private int sequentialThreshold = 2;
            private int initialDepth = 4;
            private int minDepth = 1;
            private int maxDepth = 32;
            private int adjustmentWindow = 16;

            /** 连续步进访问达到多少次判定为顺序流。 */
            public Builder sequentialThreshold(int v) {
                this.sequentialThreshold = v;
                return this;
            }

            public Builder initialDepth(int v) {
                this.initialDepth = v;
                return this;
            }

            public Builder minDepth(int v) {
                this.minDepth = v;
                return this;
            }

            public Builder maxDepth(int v) {
                this.maxDepth = v;
                return this;
            }

            /** 每多少次预读观察重新评估一次深度。 */
            public Builder adjustmentWindow(int v) {
                this.adjustmentWindow = v;
                return this;
            }

            public ReadaheadPolicy build() {
                if (sequentialThreshold < 1 || initialDepth < 0 || minDepth < 0
                        || maxDepth < minDepth || adjustmentWindow < 1) {
                    throw new IllegalArgumentException("invalid readahead policy");
                }
                return new ReadaheadPolicy(this);
            }
        }
    }

    private static final class CacheEntry {
        final byte[] data;
        boolean prefetched;

        CacheEntry(byte[] data, boolean prefetched) {
            this.data = data;
            this.prefetched = prefetched;
        }
    }

    /** 一次在途底层读取：全局唯一，需求读与预读共享。 */
    private static final class Load {
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        final long version;
        final boolean prefetched;

        Load(long version, boolean prefetched) {
            this.version = version;
            this.prefetched = prefetched;
        }
    }

    private final BlockStore delegate;
    private final int capacity;
    private final ReadaheadPolicy policy;

    private final Object lock = new Object();
    /** access-order LinkedHashMap 实现 LRU，仅由 lock 保护。 */
    private final LinkedHashMap<Long, CacheEntry> cache;
    /** 每块版本号：失效时递增，用于丢弃失效前发起的在途读取结果。 */
    private final Map<Long, Long> blockVersions = new LinkedHashMap<>();

    private final ConcurrentHashMap<Long, Load> inFlight = new ConcurrentHashMap<>();

    private final ExecutorService ioExecutor;

    // 顺序流检测状态（lock 保护）
    private long lastAccessedIndex = -1;
    private int sequentialStreak = 0;

    // 动态预读状态（lock 保护）
    private volatile boolean readaheadEnabled = true;
    private int currentDepth;
    private int windowHits = 0;
    private int windowUseless = 0;

    // 统计（lock 保护，除 underlyingReads）
    private long hits = 0;
    private long misses = 0;
    private long prefetchHits = 0;
    private long uselessPrefetches = 0;
    private final AtomicLong underlyingReads = new AtomicLong();

    public CachedBlockStore(BlockStore delegate, int capacity) {
        this(delegate, capacity, ReadaheadPolicy.defaults());
    }

    public CachedBlockStore(BlockStore delegate, int capacity, ReadaheadPolicy policy) {
        if (delegate == null || capacity < 1 || policy == null) {
            throw new IllegalArgumentException("delegate/policy must be non-null, capacity >= 1");
        }
        this.delegate = delegate;
        this.capacity = capacity;
        this.policy = policy;
        this.currentDepth = policy.initialDepth;
        this.cache = new LinkedHashMap<>(16, 0.75f, true);
        this.ioExecutor = Executors.newFixedThreadPool(
                Math.max(4, Runtime.getRuntime().availableProcessors()), r -> daemon(r, "cache-io"));
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    @Override
    public int blockSize() {
        return delegate.blockSize();
    }

    @Override
    public long blockCount() {
        return delegate.blockCount();
    }

    @Override
    public byte[] readBlock(long blockIndex) throws IOException {
        trackAccess(blockIndex);
        synchronized (lock) {
            CacheEntry entry = cache.get(blockIndex);
            if (entry != null) {
                hits++;
                if (entry.prefetched) {
                    entry.prefetched = false;
                    prefetchHits++;
                    observePrefetchOutcome(true);
                }
                scheduleReadaheadHoldingNoLock(blockIndex);
                return entry.data;
            }
            misses++;
        }
        // 未命中：注册（或加入）在途读取，再按顺序窗口推进预读
        scheduleReadaheadHoldingNoLock(blockIndex);
        return demandLoad(blockIndex, 0);
    }

    @Override
    public void writeBlock(long blockIndex, byte[] data) throws IOException {
        delegate.writeBlock(blockIndex, data);
        invalidate(blockIndex);
    }

    /** 使指定块的缓存失效；在途读取若始于失效之前，其结果会被丢弃。 */
    public void invalidate(long blockIndex) {
        synchronized (lock) {
            blockVersions.merge(blockIndex, 1L, Long::sum);
            CacheEntry removed = cache.remove(blockIndex);
            if (removed != null && removed.prefetched) {
                uselessPrefetches++;
                observePrefetchOutcome(false);
            }
        }
    }

    /** 清空整个缓存。 */
    public void invalidateAll() {
        synchronized (lock) {
            for (Map.Entry<Long, CacheEntry> e : cache.entrySet()) {
                blockVersions.merge(e.getKey(), 1L, Long::sum);
                if (e.getValue().prefetched) {
                    uselessPrefetches++;
                    observePrefetchOutcome(false);
                }
            }
            cache.clear();
        }
    }

    /** 动态开关预读。 */
    public void setReadaheadEnabled(boolean enabled) {
        this.readaheadEnabled = enabled;
    }

    public boolean isReadaheadEnabled() {
        return readaheadEnabled;
    }

    /** 手动设置当前预读深度（会被自适应逻辑继续调整）。 */
    public void setReadaheadDepth(int depth) {
        if (depth < 0 || depth > policy.maxDepth) {
            throw new IllegalArgumentException("depth out of range: " + depth);
        }
        synchronized (lock) {
            this.currentDepth = depth;
        }
    }

    public int currentReadaheadDepth() {
        synchronized (lock) {
            return currentDepth;
        }
    }

    public CacheStats stats() {
        synchronized (lock) {
            return new CacheStats(hits, misses, prefetchHits, uselessPrefetches,
                    underlyingReads.get(), cache.size(), capacity, currentDepth, readaheadEnabled);
        }
    }

    // ---- 内部实现 ----

    private void trackAccess(long blockIndex) {
        synchronized (lock) {
            if (blockIndex == lastAccessedIndex + 1) {
                sequentialStreak++;
            } else if (blockIndex != lastAccessedIndex) {
                sequentialStreak = 0;
            }
            lastAccessedIndex = blockIndex;
        }
    }

    private void scheduleReadaheadHoldingNoLock(long blockIndex) {
        if (!readaheadEnabled) {
            return;
        }
        final int depth;
        synchronized (lock) {
            if (sequentialStreak < policy.sequentialThreshold) {
                return;
            }
            depth = currentDepth;
        }
        if (depth <= 0) {
            return;
        }
        long maxIndex = delegate.blockCount() - 1;
        for (long idx = blockIndex + 1; idx <= blockIndex + depth && idx <= maxIndex; idx++) {
            if (isCachedOrInFlight(idx)) {
                continue;
            }
            // 同步注册在途读取：重复触发/并发需求读都只会加入这个 Future
            long version = versionOf(idx);
            registerLoad(idx, version, true);
        }
    }

    private boolean isCachedOrInFlight(long blockIndex) {
        if (inFlight.containsKey(blockIndex)) {
            return true;
        }
        synchronized (lock) {
            return cache.containsKey(blockIndex);
        }
    }

    private long versionOf(long blockIndex) {
        synchronized (lock) {
            return blockVersions.getOrDefault(blockIndex, 0L);
        }
    }

    /**
     * 需求读：注册或加入该块唯一的在途读取并阻塞等待。
     * 若加入的是预读注册的读取，则该次消费计为一次预读命中。
     */
    private byte[] demandLoad(long blockIndex, int retry) throws IOException {
        // 二次检查：未命中判定与注册之间，预读可能恰好完成并已写入缓存
        synchronized (lock) {
            CacheEntry entry = cache.get(blockIndex);
            if (entry != null) {
                hits++;
                misses--;
                if (entry.prefetched) {
                    entry.prefetched = false;
                    prefetchHits++;
                    observePrefetchOutcome(true);
                }
                return entry.data;
            }
        }
        long version = versionOf(blockIndex);
        Load load = registerLoad(blockIndex, version, false);
        byte[] data;
        try {
            data = load.future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while loading block " + blockIndex, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            throw new IOException("failed to load block " + blockIndex, cause);
        }
        if (load.prefetched) {
            synchronized (lock) {
                CacheEntry entry = cache.get(blockIndex);
                if (entry != null && entry.prefetched) {
                    entry.prefetched = false;
                    prefetchHits++;
                    observePrefetchOutcome(true);
                }
            }
        }
        // 在途读取期间块被失效/更新：丢弃旧数据，重新读取新版本
        if (versionOf(blockIndex) != load.version && retry < 3) {
            synchronized (lock) {
                if (cache.containsKey(blockIndex)) {
                    return cache.get(blockIndex).data;
                }
            }
            return demandLoad(blockIndex, retry + 1);
        }
        return data;
    }

    private Load registerLoad(long blockIndex, long version, boolean prefetched) {
        return inFlight.computeIfAbsent(blockIndex, k -> {
            Load load = new Load(version, prefetched);
            ioExecutor.execute(() -> runLoad(blockIndex, load));
            return load;
        });
    }

    private void runLoad(long blockIndex, Load load) {
        try {
            byte[] data;
            synchronized (lock) {
                // 读底层前最后确认：缓存可能已被并发路径填充，版本可能已变
                CacheEntry existing = cache.get(blockIndex);
                if (existing != null) {
                    load.future.complete(existing.data);
                    return;
                }
                if (blockVersions.getOrDefault(blockIndex, 0L) != load.version) {
                    inFlight.remove(blockIndex, load);
                    return;
                }
            }
            underlyingReads.incrementAndGet();
            data = delegate.readBlock(blockIndex);
            synchronized (lock) {
                if (blockVersions.getOrDefault(blockIndex, 0L) == load.version) {
                    cache.computeIfAbsent(blockIndex, k -> new CacheEntry(data, load.prefetched));
                    evictIfNecessary();
                    load.future.complete(data);
                } else {
                    // 读取期间块被更新：旧数据交给等待者（其 demandLoad 会检测版本并重试），不污染缓存
                    load.future.complete(data);
                }
            }
        } catch (Throwable t) {
            load.future.completeExceptionally(t);
        } finally {
            inFlight.remove(blockIndex, load);
        }
    }

    private void evictIfNecessary() {
        // 调用方必须持有 lock
        Iterator<Map.Entry<Long, CacheEntry>> it = cache.entrySet().iterator();
        while (cache.size() > capacity && it.hasNext()) {
            Map.Entry<Long, CacheEntry> eldest = it.next();
            it.remove();
            if (eldest.getValue().prefetched) {
                uselessPrefetches++;
                observePrefetchOutcome(false);
            }
        }
    }

    /** 记录一次预读观察，窗口满时按有效率调整预读深度。调用方必须持有 lock。 */
    private void observePrefetchOutcome(boolean hit) {
        if (hit) {
            windowHits++;
        } else {
            windowUseless++;
        }
        if (windowHits + windowUseless >= policy.adjustmentWindow) {
            double effectiveness = (double) windowHits / (windowHits + windowUseless);
            if (effectiveness >= 0.6) {
                currentDepth = Math.min(policy.maxDepth, Math.max(currentDepth + 1, currentDepth * 2));
            } else if (effectiveness <= 0.3) {
                currentDepth = Math.max(policy.minDepth, currentDepth / 2);
            }
            windowHits = 0;
            windowUseless = 0;
        }
    }

    @Override
    public void close() {
        ioExecutor.shutdownNow();
        try {
            ioExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
