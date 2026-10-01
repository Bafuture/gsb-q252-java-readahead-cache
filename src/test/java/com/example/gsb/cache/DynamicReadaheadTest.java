package com.example.gsb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.gsb.cache.CachedBlockStore.ReadaheadPolicy;
import org.junit.jupiter.api.Test;

/** 需求 3：预读深度随访问模式动态调整——随机为主时降低，顺序为主时加深。 */
class DynamicReadaheadTest {

    private static ReadaheadPolicy policy() {
        return ReadaheadPolicy.builder()
                .sequentialThreshold(1)
                .initialDepth(8)
                .minDepth(1)
                .maxDepth(16)
                .adjustmentWindow(8)
                .build();
    }

    @Test
    void randomAccessLowersDepthAndSequentialAccessRaisesIt() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(20_000, 64, 0);
        // 容量大于 maxDepth 但远小于访问跨度：随机阶段未被使用的预读块会被淘汰
        // 计为无效预读；顺序阶段预读窗口（最深 16）又能完整留在缓存中被消费
        try (CachedBlockStore cache = new CachedBlockStore(store, 64, policy())) {
            assertThat(cache.currentReadaheadDepth()).isEqualTo(8);

            // 阶段一：伪随机访问——每次只读 base, base+1 两块便跳走，
            // 触发的预读块全部无人使用，被淘汰后计为无效预读
            long base = 0;
            for (int i = 0; i < 60; i++) {
                cache.readBlock(base);
                cache.readBlock(base + 1);
                base += 97; // 大步跳跃，预读块永远用不上
            }
            TestSupport.awaitTrue(() -> cache.currentReadaheadDepth() == 1, 5_000,
                    "depth to shrink to min under random access");

            CacheStats afterRandom = cache.stats();
            assertThat(afterRandom.uselessPrefetches()).isGreaterThan(0);
            assertThat(afterRandom.prefetchHitRate()).isLessThan(0.3);

            // 阶段二：纯顺序访问，预读块全部被消费，深度应回升到上限
            for (long i = 10_000; i < 10_400; i++) {
                cache.readBlock(i);
            }
            TestSupport.awaitTrue(() -> cache.currentReadaheadDepth() == 16, 5_000,
                    "depth to grow to max under sequential access");

            CacheStats afterSequential = cache.stats();
            assertThat(afterSequential.prefetchHits())
                    .isGreaterThan(afterRandom.prefetchHits() + 100);
            // 顺序阶段几乎不再产生新的无效预读
            assertThat(afterSequential.uselessPrefetches() - afterRandom.uselessPrefetches())
                    .isLessThan(32);
        }
    }

    @Test
    void manualDepthOverrideIsRespected() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(1_000, 64, 0);
        try (CachedBlockStore cache = new CachedBlockStore(store, 64, policy())) {
            cache.setReadaheadDepth(3);
            assertThat(cache.currentReadaheadDepth()).isEqualTo(3);
            assertThat(cache.stats().currentReadaheadDepth()).isEqualTo(3);
        }
    }
}
