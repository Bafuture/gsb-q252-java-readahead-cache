package com.example.gsb.cache;

/**
 * 缓存运行统计的不可变快照。
 */
public final class CacheStats {

    private final long hits;
    private final long misses;
    private final long prefetchHits;
    private final long uselessPrefetches;
    private final long underlyingReads;
    private final int cacheSize;
    private final int capacity;
    private final int currentDepth;
    private final boolean readaheadEnabled;

    CacheStats(long hits, long misses, long prefetchHits, long uselessPrefetches,
               long underlyingReads, int cacheSize, int capacity,
               int currentDepth, boolean readaheadEnabled) {
        this.hits = hits;
        this.misses = misses;
        this.prefetchHits = prefetchHits;
        this.uselessPrefetches = uselessPrefetches;
        this.underlyingReads = underlyingReads;
        this.cacheSize = cacheSize;
        this.capacity = capacity;
        this.currentDepth = currentDepth;
        this.readaheadEnabled = readaheadEnabled;
    }

    public long hits() {
        return hits;
    }

    public long misses() {
        return misses;
    }

    public long prefetchHits() {
        return prefetchHits;
    }

    /** 预取后在被实际访问前即被淘汰或失效的块数（浪费的预读）。 */
    public long uselessPrefetches() {
        return uselessPrefetches;
    }

    /** 实际穿透到底层的读取次数（并发同块请求只计一次）。 */
    public long underlyingReads() {
        return underlyingReads;
    }

    public int cacheSize() {
        return cacheSize;
    }

    public int capacity() {
        return capacity;
    }

    public int currentReadaheadDepth() {
        return currentDepth;
    }

    public boolean readaheadEnabled() {
        return readaheadEnabled;
    }

    public long totalRequests() {
        return hits + misses;
    }

    /** 缓存命中率 = 命中 / (命中 + 未命中)，预读命中也属于命中。 */
    public double hitRate() {
        long total = totalRequests();
        return total == 0 ? 0.0 : (double) hits / total;
    }

    /** 预读有效率 = 预读命中 / (预读命中 + 无效预读)。 */
    public double prefetchHitRate() {
        long observed = prefetchHits + uselessPrefetches;
        return observed == 0 ? 0.0 : (double) prefetchHits / observed;
    }

    @Override
    public String toString() {
        return String.format(
                "CacheStats{requests=%d, hits=%d (%.1f%%), misses=%d, prefetchHits=%d (%.1f%%), "
                        + "uselessPrefetches=%d, underlyingReads=%d, depth=%d, readahead=%s, cache=%d/%d}",
                totalRequests(), hits, hitRate() * 100, misses,
                prefetchHits, prefetchHitRate() * 100, uselessPrefetches, underlyingReads,
                currentDepth, readaheadEnabled, cacheSize, capacity);
    }
}
