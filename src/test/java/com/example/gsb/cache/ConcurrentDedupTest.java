package com.example.gsb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** 需求 5：同一块的并发请求只触发一次底层读取，其余等待并复用结果。 */
class ConcurrentDedupTest {

    @Test
    void concurrentReadsOfSameBlockHitUnderlyingOnlyOnce() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 30);
        try (CachedBlockStore cache = new CachedBlockStore(store, 16)) {
            cache.setReadaheadEnabled(false);

            int threads = 16;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<byte[]>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    return cache.readBlock(7);
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            byte[] expected = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<byte[]> f : futures) {
                assertThat(f.get()).isEqualTo(expected);
            }
            pool.shutdownNow();

            assertThat(store.readCount()).isEqualTo(1);
            assertThat(cache.stats().underlyingReads()).isEqualTo(1);
        }
    }

    @Test
    void concurrentReadsOfDifferentBlocksDoNotBlockEachOther() throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(64, 128, 10);
        try (CachedBlockStore cache = new CachedBlockStore(store, 16)) {
            cache.setReadaheadEnabled(false);

            int threads = 16;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<byte[]>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                long block = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    return cache.readBlock(block);
                }));
            }
            start.countDown();
            for (int i = 0; i < threads; i++) {
                assertThat(futures.get(i).get(10, TimeUnit.SECONDS)).isNotNull();
            }
            pool.shutdownNow();

            assertThat(store.readCount()).isEqualTo(16); // 每个块各读一次，且并行完成
        }
    }
}
