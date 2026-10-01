package com.example.gsb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 需求 1：按块缓存、容量可配、按访问情况（LRU）淘汰；需求 6：命中率统计。 */
class CacheEvictionTest {

    @Test
    void cacheHitAvoidsUnderlyingRead() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 4)) {
            cache.setReadaheadEnabled(false);

            byte[] first = cache.readBlock(3);
            byte[] second = cache.readBlock(3);

            assertThat(second).isEqualTo(first);
            assertThat(store.readCount()).isEqualTo(1); // 第二次命中缓存
            CacheStats stats = cache.stats();
            assertThat(stats.hits()).isEqualTo(1);
            assertThat(stats.misses()).isEqualTo(1);
            assertThat(stats.hitRate()).isEqualTo(0.5);
            assertThat(stats.underlyingReads()).isEqualTo(1);
        }
    }

    @Test
    void leastRecentlyUsedEntryIsEvictedFirst() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 3)) {
            cache.setReadaheadEnabled(false);

            cache.readBlock(0);
            cache.readBlock(1);
            cache.readBlock(2);
            assertThat(cache.stats().cacheSize()).isEqualTo(3);

            cache.readBlock(0); // 刷新 0 的访问时间，1 成为最久未使用
            cache.readBlock(3); // 触发淘汰，应淘汰块 1

            assertThat(cache.stats().cacheSize()).isEqualTo(3);
            long readsBefore = store.readCount();

            cache.readBlock(0); // 仍应命中
            cache.readBlock(2); // 仍应命中
            assertThat(store.readCount()).isEqualTo(readsBefore);

            cache.readBlock(1); // 已被淘汰，必须重新读底层
            assertThat(store.readCount()).isEqualTo(readsBefore + 1);
        }
    }

    @Test
    void capacityIsRespectedUnderHeavyLoad() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(256, 64, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 8)) {
            cache.setReadaheadEnabled(false);
            for (long i = 0; i < 200; i++) {
                cache.readBlock(i);
            }
            assertThat(cache.stats().cacheSize()).isEqualTo(8);
            assertThat(store.readCount()).isEqualTo(200); // 容量太小，全部未命中
        }
    }
}
