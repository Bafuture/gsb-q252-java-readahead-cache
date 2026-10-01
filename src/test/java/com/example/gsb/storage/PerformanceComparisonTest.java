package com.example.gsb.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * Runs the same access workload with read-ahead switched off and on, for both a
 * sequential and a random pattern, and writes a timing report to
 * {@code target/perf-comparison.txt} (also printed to stdout / surefire output).
 */
class PerformanceComparisonTest {

    private static final long BACKEND_LATENCY_MS = 3;
    private static final int TOTAL_READS = 120;
    private static final int RANDOM_RANGE = 400;
    private static final long RANDOM_SEED = 42L;

    @Test
    void compareSequentialAndRandomWithAndWithoutPrefetch() throws Exception {
        RunResult sequentialOff = runSequential(false);
        RunResult sequentialOn = runSequential(true);
        RunResult randomOff = runRandom(false);
        RunResult randomOn = runRandom(true);

        String report = buildReport(sequentialOff, sequentialOn, randomOff, randomOn);
        System.out.println(report);
        Path reportFile = Path.of("target", "perf-comparison.txt");
        Files.createDirectories(reportFile.getParent());
        Files.writeString(reportFile, report);

        // Sequential scan: read-ahead must clearly beat plain block-by-block reads.
        assertThat(sequentialOn.elapsedMillis())
                .as("prefetch should speed up sequential scans by >= 3x")
                .isLessThan(sequentialOff.elapsedMillis() / 3);
        assertThat(sequentialOn.stats().prefetchHits()).isGreaterThan(0);

        // Random scan: the adaptive controller must avoid wasteful read-ahead, so the two
        // timings stay comparable and almost no extra backend reads are issued.
        assertThat(randomOn.stats().prefetchesIssued()).isLessThanOrEqualTo(2);
        assertThat(randomOn.stats().wastedPrefetches()).isLessThanOrEqualTo(2);
        assertThat((double) randomOn.elapsedMillis())
                .as("prefetch must not meaningfully slow down random access")
                .isLessThan(randomOff.elapsedMillis() * 1.5 + 200);
    }

    private RunResult runSequential(boolean prefetchEnabled) throws Exception {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, BACKEND_LATENCY_MS);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 512, 8, 2)) {
            cache.setPrefetchEnabled(prefetchEnabled);
            long start = System.nanoTime();
            for (long blockId = 0; blockId < TOTAL_READS; blockId++) {
                cache.readBlock(blockId);
            }
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            return new RunResult(elapsed, cache.getStats());
        }
    }

    private RunResult runRandom(boolean prefetchEnabled) throws Exception {
        long[] plan = new long[TOTAL_READS];
        Random random = new Random(RANDOM_SEED);
        for (int i = 0; i < TOTAL_READS; i++) {
            plan[i] = random.nextInt(RANDOM_RANGE);
        }

        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, BACKEND_LATENCY_MS);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 512, 8, 2)) {
            cache.setPrefetchEnabled(prefetchEnabled);
            long start = System.nanoTime();
            for (long blockId : plan) {
                cache.readBlock(blockId);
            }
            // let any (rare) adaptive prefetches finish so the stats are final
            Thread.sleep(50);
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            return new RunResult(elapsed, cache.getStats());
        }
    }

    private String buildReport(RunResult seqOff, RunResult seqOn,
                               RunResult randOff, RunResult randOn) {
        StringBuilder sb = new StringBuilder();
        sb.append(System.lineSeparator())
                .append("Read-ahead performance comparison")
                .append(" (backend latency ").append(BACKEND_LATENCY_MS).append(" ms/block, ")
                .append(TOTAL_READS).append(" reads)")
                .append(System.lineSeparator())
                .append(String.format(
                        "%-12s %-8s %10s %12s %14s %10s%n",
                        "pattern", "prefetch", "time(ms)", "backendReads",
                        "prefetchHits", "wasted"));
        appendRow(sb, "sequential", "OFF", seqOff);
        appendRow(sb, "sequential", "ON", seqOn);
        appendRow(sb, "random", "OFF", randOff);
        appendRow(sb, "random", "ON", randOn);
        sb.append(String.format(
                "speedup: sequential %.2fx, random %.2fx%n",
                speedup(seqOff, seqOn), speedup(randOff, randOn)));
        return sb.toString();
    }

    private void appendRow(StringBuilder sb, String pattern, String mode, RunResult r) {
        sb.append(String.format(
                "%-12s %-8s %10d %12d %14d %10d%n",
                pattern, mode, r.elapsedMillis(),
                r.stats().backendReads(), r.stats().prefetchHits(),
                r.stats().wastedPrefetches()));
    }

    private double speedup(RunResult off, RunResult on) {
        return on.elapsedMillis() == 0 ? Double.POSITIVE_INFINITY
                : (double) off.elapsedMillis() / on.elapsedMillis();
    }

    private record RunResult(long elapsedMillis, CacheStats stats) {
    }
}
