package com.example.gsb.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * 需求 7：顺序 / 随机两种访问模式下，预读开 vs 关的耗时对比。
 * 结果在测试输出中打印表格，同时对顺序模式的提速、随机模式的开销做宽松断言。
 */
class PerformanceComparisonTest {

    private static final long BLOCK_COUNT = 2_048;
    private static final int BLOCK_SIZE = 4_096;
    private static final long LATENCY_MS = 2;
    private static final int REQUESTS = 256;

    private long runWorkload(boolean sequential, boolean readaheadOn) throws Exception {
        LatencyBlockStore store = new LatencyBlockStore(BLOCK_COUNT, BLOCK_SIZE, LATENCY_MS);
        CachedBlockStore.ReadaheadPolicy policy = CachedBlockStore.ReadaheadPolicy.builder()
                .sequentialThreshold(2)
                .initialDepth(8)
                .adjustmentWindow(10_000)
                .build();
        try (CachedBlockStore cache = new CachedBlockStore(store, 2_048, policy)) {
            cache.setReadaheadEnabled(readaheadOn);
            Random rng = new Random(42);
            long[] sequence = new long[REQUESTS];
            for (int i = 0; i < REQUESTS; i++) {
                sequence[i] = sequential ? i : rng.nextInt((int) BLOCK_COUNT);
            }

            long start = System.nanoTime();
            for (long block : sequence) {
                cache.readBlock(block);
            }
            if (readaheadOn) {
                // 让末尾异步预读收尾，保证统计完整
                Thread.sleep(150);
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            System.out.printf("  %-8s  readahead=%-3s  elapsed=%5d ms  %s%n",
                    sequential ? "seq" : "random",
                    readaheadOn ? "on" : "off",
                    elapsedMs, cache.stats());
            return elapsedMs;
        }
    }

    @Test
    void compareSequentialAndRandomWorkloads() throws Exception {
        System.out.println();
        System.out.println("  ================= 读缓存 / 预读性能对比 =================");
        long seqOff = runWorkload(true, false);
        long seqOn = runWorkload(true, true);
        long randOff = runWorkload(false, false);
        long randOn = runWorkload(false, true);
        System.out.printf("  ---------------------------------------------------------%n");
        System.out.printf("  顺序模式提速: %.1fx（%d ms -> %d ms）%n",
                (double) seqOff / Math.max(1, seqOn), seqOff, seqOn);
        System.out.printf("  随机模式开销: %.2fx（%d ms -> %d ms，应接近 1）%n",
                (double) randOn / Math.max(1, randOff), randOff, randOn);
        System.out.println("  =========================================================");

        // 顺序模式：预读应把大量 2ms 的串行底层读转为提前并行完成，至少提速一倍
        assertThat(seqOn).isLessThan(seqOff / 2);
        // 随机模式：顺序检测不成立、几乎不触发预读，开关预读的耗时差应在 50% 以内
        assertThat(randOn).isLessThan((long) (randOff * 1.5) + 20);
    }
}
