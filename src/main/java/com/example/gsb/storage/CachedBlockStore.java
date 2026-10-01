package com.example.gsb.storage;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A block read-cache with sequential read-ahead on top of a {@link BlockStorage}.
 *
 * <h2>Read cache</h2>
 * Blocks are cached in a bounded access-order LRU map. When the configured capacity is
 * exceeded the least-recently-used block is evicted; a prefetched block that is evicted
 * before it was ever read counts as a <em>wasted prefetch</em>.
 *
 * <h2>Sequential pattern detection</h2>
 * Every {@link #readBlock} is classified against the immediately previous access:
 * {@code blockId == previousBlockId + 1} is a sequential step, otherwise the access is
 * random. Two signals are derived:
 * <ul>
 *   <li>a <b>sequential streak</b> (number of consecutive sequential steps); once the
 *       streak reaches {@code sequentialThreshold} read-ahead kicks in, and
 *       {@code blockId+1 .. blockId+depth} are prefetched asynchronously;</li>
 *   <li>a <b>sliding window</b> (last 32 accesses) holding the fraction of sequential
 *       steps, which drives the adaptive prefetch depth.</li>
 * </ul>
 *
 * <h2>Adaptive prefetch depth</h2>
 * The current depth is recomputed on every access as
 * {@code round(maxPrefetchDepth * sequentialRatio)}. Purely sequential access quickly
 * reaches {@code maxPrefetchDepth}; random access drives it to 0, so no useless blocks
 * are read. Read-ahead can be turned off entirely with {@link #setPrefetchEnabled(boolean)}.
 *
 * <h2>Consistency</h2>
 * {@link #writeBlock} and {@link #invalidate} drop the cached copy and bump a per-block
 * version. An in-flight read (demand or prefetch) captures the version before touching
 * the backend and discards its result if the version changed, so a stale result can never
 * populate the cache after an update.
 *
 * <h2>Concurrent de-duplication</h2>
 * Concurrent reads of the same block share a single in-flight {@link CompletableFuture}:
 * exactly one thread (or prefetch task) performs the backend read, all other callers wait
 * on it and reuse the result.
 *
 * <p>The returned byte arrays are owned by the cache; callers must not modify them.
 */
public class CachedBlockStore implements AutoCloseable {

    private static final int PATTERN_WINDOW = 32;

    private final BlockStorage backend;
    private final int capacity;
    private final int maxPrefetchDepth;
    private final int sequentialThreshold;
    private final ExecutorService prefetchExecutor;

    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedHashMap<Long, CacheEntry> cache;
    private final ConcurrentHashMap<Long, Inflight> inflight = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> versions = new ConcurrentHashMap<>();

    // Access-pattern state, guarded by lock.
    private long lastBlockId = Long.MIN_VALUE;
    private int sequentialStreak = 0;
    private final Deque<Boolean> accessWindow = new ArrayDeque<>(PATTERN_WINDOW);
    private int sequentialInWindow = 0;

    private volatile boolean prefetchEnabled = true;
    private volatile int currentPrefetchDepth = 0;

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong prefetchHits = new AtomicLong();
    private final AtomicLong prefetchesIssued = new AtomicLong();
    private final AtomicLong wastedPrefetches = new AtomicLong();
    private final AtomicLong backendReads = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong invalidations = new AtomicLong();

    public CachedBlockStore(BlockStorage backend, int capacity) {
        this(backend, capacity, 4, 2);
    }

    /**
     * @param capacity            maximum number of cached blocks
     * @param maxPrefetchDepth    prefetch depth used for a fully sequential workload
     * @param sequentialThreshold consecutive sequential steps required before prefetching
     */
    public CachedBlockStore(BlockStorage backend, int capacity,
                            int maxPrefetchDepth, int sequentialThreshold) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        if (maxPrefetchDepth < 0) {
            throw new IllegalArgumentException("maxPrefetchDepth must be >= 0");
        }
        if (sequentialThreshold <= 0) {
            throw new IllegalArgumentException("sequentialThreshold must be positive");
        }
        this.backend = backend;
        this.capacity = capacity;
        this.maxPrefetchDepth = maxPrefetchDepth;
        this.sequentialThreshold = sequentialThreshold;
        this.cache = new LinkedHashMap<>(Math.min(capacity, 64) * 2, 0.75f, true);
        int threads = Math.max(1, maxPrefetchDepth);
        this.prefetchExecutor = Executors.newFixedThreadPool(threads, runnable -> {
            Thread thread = new Thread(runnable, "cache-prefetch");
            thread.setDaemon(true);
            return thread;
        });
    }

    public byte[] readBlock(long blockId) {
        byte[] cached = cachedCopy(blockId);
        if (cached != null) {
            recordAccess(blockId);
            return cached;
        }

        Inflight mine = new Inflight(false);
        Inflight existing = inflight.putIfAbsent(blockId, mine);
        if (existing != null) {
            // Another read (or prefetch) is already loading this block: wait and reuse.
            hits.incrementAndGet();
            if (existing.prefetched) {
                prefetchHits.incrementAndGet();
            }
            byte[] data = existing.future.join();
            recordAccess(blockId);
            return data;
        }

        misses.incrementAndGet();
        backendReads.incrementAndGet();
        long version = versionOf(blockId);
        try {
            byte[] data = backend.readBlock(blockId);
            if (version == versionOf(blockId)) {
                insert(blockId, data, false);
            }
            mine.future.complete(data);
            recordAccess(blockId);
            return data;
        } catch (RuntimeException e) {
            mine.future.completeExceptionally(e);
            throw e;
        } finally {
            inflight.remove(blockId, mine);
        }
    }

    /** Write-through: update the backend and invalidate any cached copy of the block. */
    public void writeBlock(long blockId, byte[] data) {
        versions.merge(blockId, 1L, Long::sum);
        backend.writeBlock(blockId, data);
        removeCached(blockId);
    }

    /**
     * Invalidate a block whose contents were changed out of band (e.g. written directly
     * to the backend). The next read fetches fresh contents.
     */
    public void invalidate(long blockId) {
        versions.merge(blockId, 1L, Long::sum);
        removeCached(blockId);
    }

    public boolean isPrefetchEnabled() {
        return prefetchEnabled;
    }

    /** Turn read-ahead on or off at runtime. Does not discard already prefetched blocks. */
    public void setPrefetchEnabled(boolean enabled) {
        this.prefetchEnabled = enabled;
    }

    /** Depth currently selected by the adaptive controller (0 means no read-ahead). */
    public int getCurrentPrefetchDepth() {
        return prefetchEnabled ? currentPrefetchDepth : 0;
    }

    public int getMaxPrefetchDepth() {
        return maxPrefetchDepth;
    }

    public CacheStats getStats() {
        return new CacheStats(hits.get(), misses.get(), prefetchHits.get(),
                prefetchesIssued.get(), wastedPrefetches.get(), backendReads.get(),
                evictions.get(), invalidations.get());
    }

    /** Current number of cached blocks (mainly visible for tests). */
    int cachedBlockCount() {
        lock.lock();
        try {
            return cache.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        prefetchExecutor.shutdown();
    }

    // ---- internals ---------------------------------------------------------

    private byte[] cachedCopy(long blockId) {
        lock.lock();
        try {
            CacheEntry entry = cache.get(blockId);
            if (entry == null) {
                return null;
            }
            hits.incrementAndGet();
            if (entry.prefetched) {
                entry.prefetched = false;
                prefetchHits.incrementAndGet();
            }
            return entry.data;
        } finally {
            lock.unlock();
        }
    }

    private void insert(long blockId, byte[] data, boolean prefetched) {
        lock.lock();
        try {
            cache.put(blockId, new CacheEntry(data, prefetched));
            if (cache.size() > capacity) {
                Iterator<Map.Entry<Long, CacheEntry>> iterator = cache.entrySet().iterator();
                Map.Entry<Long, CacheEntry> eldest = iterator.next();
                iterator.remove();
                evictions.incrementAndGet();
                if (eldest.getValue().prefetched) {
                    wastedPrefetches.incrementAndGet();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeCached(long blockId) {
        lock.lock();
        try {
            if (cache.remove(blockId) != null) {
                invalidations.incrementAndGet();
            }
        } finally {
            lock.unlock();
        }
    }

    private void recordAccess(long blockId) {
        boolean sequential;
        int streak;
        lock.lock();
        try {
            sequential = blockId == lastBlockId + 1;
            if (sequential) {
                sequentialStreak++;
            } else {
                sequentialStreak = 0;
            }
            lastBlockId = blockId;

            if (accessWindow.size() == PATTERN_WINDOW && accessWindow.removeFirst()) {
                sequentialInWindow--;
            }
            accessWindow.addLast(sequential);
            if (sequential) {
                sequentialInWindow++;
            }
            double sequentialRatio = (double) sequentialInWindow / accessWindow.size();
            currentPrefetchDepth = (int) Math.round(maxPrefetchDepth * sequentialRatio);
            streak = sequentialStreak;
        } finally {
            lock.unlock();
        }

        if (prefetchEnabled && sequential && streak >= sequentialThreshold) {
            triggerPrefetch(blockId);
        }
    }

    private void triggerPrefetch(long baseBlockId) {
        int depth = currentPrefetchDepth;
        if (depth <= 0) {
            return;
        }
        for (int offset = 1; offset <= depth; offset++) {
            long target = baseBlockId + offset;

            lock.lock();
            try {
                if (cache.containsKey(target)) {
                    continue;
                }
            } finally {
                lock.unlock();
            }
            Inflight infl = new Inflight(true);
            if (inflight.putIfAbsent(target, infl) != null) {
                continue;
            }
            prefetchesIssued.incrementAndGet();
            try {
                prefetchExecutor.execute(() -> runPrefetch(target, infl));
            } catch (RuntimeException e) {
                inflight.remove(target, infl);
                throw e;
            }
        }
    }

    private void runPrefetch(long blockId, Inflight infl) {
        backendReads.incrementAndGet();
        long version = versionOf(blockId);
        try {
            byte[] data = backend.readBlock(blockId);
            if (version == versionOf(blockId)) {
                insert(blockId, data, true);
            }
            infl.future.complete(data);
        } catch (RuntimeException e) {
            infl.future.completeExceptionally(e);
        } finally {
            inflight.remove(blockId, infl);
        }
    }

    private long versionOf(long blockId) {
        return versions.getOrDefault(blockId, 0L);
    }

    private static final class CacheEntry {
        final byte[] data;
        volatile boolean prefetched;

        CacheEntry(byte[] data, boolean prefetched) {
            this.data = data;
            this.prefetched = prefetched;
        }
    }

    private static final class Inflight {
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        final boolean prefetched;

        Inflight(boolean prefetched) {
            this.prefetched = prefetched;
        }
    }
}
