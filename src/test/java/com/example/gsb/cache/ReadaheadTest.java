package com.example.gsb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.gsb.cache.CachedBlockStore.ReadaheadPolicy;
import org.junit.jupiter.api.Test;

/** 需求 2：识别顺序访问模式（连续块号步进达到阈值）后预读后续块。 */
class ReadaheadTest {

    @Test
    void sequentialAccessTriggersPrefetchOfFollowingBlocks() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 1);
        ReadaheadPolicy policy = ReadaheadPolicy.builder()
                .sequentialThreshold(2)
                .initialDepth(4)
                .adjustmentWindow(1000) // 本测试不关心自适应
                .build();
        try (CachedBlockStore cache = new CachedBlockStore(store, 32, policy)) {
            // 连续读 0,1：streak 达到阈值 2，触发对 2..5 的异步预读
            cache.readBlock(0);
            cache.readBlock(1);

            // 等待预读完成：底层读取 = 2 次需求读 + 4 次预读
            TestSupport.awaitTrue(() -> cache.stats().underlyingReads() >= 6, 5_000,
                    "prefetch of blocks 2..5");

            for (long i = 2; i <= 5; i++) {
                cache.readBlock(i); // 全部应命中预读缓存
            }

            CacheStats stats = cache.stats();
            assertThat(stats.underlyingReads()).isEqualTo(6); // 6 块数据只读 6 次底层
            assertThat(stats.prefetchHits()).isEqualTo(4);
            assertThat(stats.hits()).isEqualTo(4);
            assertThat(stats.uselessPrefetches()).isZero();
        }
    }

    @Test
    void nonSequentialAccessDoesNotTriggerPrefetch() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 0);
        ReadaheadPolicy policy = ReadaheadPolicy.builder()
                .sequentialThreshold(3)
                .initialDepth(4)
                .build();
        try (CachedBlockStore cache = new CachedBlockStore(store, 32, policy)) {
            // 跳跃访问，永远凑不齐连续步进
            for (long i : new long[] {10, 30, 5, 55, 20, 40, 0, 63}) {
                cache.readBlock(i);
            }
            Thread.sleep(100); // 给潜在的误触发预读留出时间
            assertThat(cache.stats().underlyingReads()).isEqualTo(8);
            assertThat(cache.stats().prefetchHits()).isZero();
            assertThat(cache.stats().uselessPrefetches()).isZero();
        }
    }

    @Test
    void readaheadCanBeDisabled() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 32)) {
            cache.setReadaheadEnabled(false);
            for (long i = 0; i < 10; i++) {
                cache.readBlock(i);
            }
            Thread.sleep(100);
            assertThat(cache.stats().underlyingReads()).isEqualTo(10); // 无预读
        }
    }
}
