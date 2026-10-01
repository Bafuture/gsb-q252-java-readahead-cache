package com.example.gsb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** 需求 4：底层块更新后缓存必须失效，绝不能读到旧数据。 */
class ConsistencyTest {

    @Test
    void writeThroughInvalidatesCachedCopy() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 16)) {
            cache.setReadaheadEnabled(false);

            byte[] original = cache.readBlock(7);
            assertThat(cache.stats().hits()).isZero();

            byte[] updated = new byte[128];
            Arrays.fill(updated, (byte) 0x5A);
            cache.writeBlock(7, updated);

            byte[] reread = cache.readBlock(7);
            assertThat(reread).isEqualTo(updated);
            assertThat(reread).isNotEqualTo(original);
            // 失效后必须重新穿透到底层读取一次
            assertThat(store.readCount()).isEqualTo(2);
        }
    }

    @Test
    void externalModificationPlusInvalidateYieldsFreshData() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 16)) {
            cache.setReadaheadEnabled(false);

            byte[] original = cache.readBlock(9);

            // 模拟绕过缓存直接更新底层（如其他节点写入），随后收到失效通知
            byte[] updated = new byte[128];
            Arrays.fill(updated, (byte) 0x11);
            store.writeBlock(9, updated);
            cache.invalidate(9);

            assertThat(cache.readBlock(9)).isEqualTo(updated);
            assertThat(cache.readBlock(9)).isNotEqualTo(original);
        }
    }

    @Test
    void invalidateAllDropsEveryCachedBlock() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 16)) {
            cache.setReadaheadEnabled(false);
            for (long i = 0; i < 5; i++) {
                cache.readBlock(i);
            }
            assertThat(cache.stats().cacheSize()).isEqualTo(5);

            cache.invalidateAll();
            assertThat(cache.stats().cacheSize()).isZero();

            long readsBefore = store.readCount();
            for (long i = 0; i < 5; i++) {
                cache.readBlock(i);
            }
            assertThat(store.readCount()).isEqualTo(readsBefore + 5); // 全部重新读底层
        }
    }
}
