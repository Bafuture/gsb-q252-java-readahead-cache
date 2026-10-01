package com.example.gsb.storage;

/**
 * Immutable snapshot of {@link CachedBlockStore} statistics.
 *
 * @param hits             reads served without issuing a new backend read (cache hit or
 *                         joining an in-flight read of the same block)
 * @param misses           reads that had to issue a new backend read
 * @param prefetchHits     reads served by a block that was loaded by the read-ahead
 *                         (either already cached as prefetched, or joined an in-flight prefetch)
 * @param prefetchesIssued number of prefetch backend reads issued
 * @param wastedPrefetches prefetched blocks evicted or invalidated before ever being used
 * @param backendReads     total reads issued to the underlying storage (demand + prefetch)
 * @param evictions        entries evicted from the cache due to capacity
 * @param invalidations    entries dropped because the block was written or invalidated
 */
public record CacheStats(long hits,
                         long misses,
                         long prefetchHits,
                         long prefetchesIssued,
                         long wastedPrefetches,
                         long backendReads,
                         long evictions,
                         long invalidations) {

    public long totalReads() {
        return hits + misses;
    }

    /** Fraction of reads served without issuing a backend read, in [0, 1]. */
    public double hitRate() {
        long total = totalReads();
        return total == 0 ? 0.0 : (double) hits / total;
    }

    /** Fraction of issued prefetches that were actually consumed by a read, in [0, 1]. */
    public double prefetchHitRate() {
        return prefetchesIssued == 0 ? 0.0 : (double) prefetchHits / prefetchesIssued;
    }

    @Override
    public String toString() {
        return String.format(
                "CacheStats{reads=%d, hitRate=%.1f%%, prefetchHitRate=%.1f%%, "
                        + "prefetchesIssued=%d, wastedPrefetches=%d, backendReads=%d, "
                        + "evictions=%d, invalidations=%d}",
                totalReads(), hitRate() * 100, prefetchHitRate() * 100,
                prefetchesIssued, wastedPrefetches, backendReads, evictions, invalidations);
    }
}
