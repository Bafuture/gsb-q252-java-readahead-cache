package com.example.gsb.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class CachedBlockStoreTest {

    private static byte[] blockOf(long blockId) {
        byte[] data = new byte[16];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (blockId * 31 + i);
        }
        return data;
    }

    @Test
    void cacheHitsAvoidBackendReadsAndLruEvictsOldestBlock() {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, 0);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 2, 0, 2)) {
            assertThat(cache.readBlock(0)).containsExactly(blockOf(0));
            assertThat(cache.readBlock(1)).containsExactly(blockOf(1));
            assertThat(backend.readCount()).isEqualTo(2);

            // repeat reads are served from the cache
            assertThat(cache.readBlock(0)).containsExactly(blockOf(0));
            assertThat(cache.readBlock(1)).containsExactly(blockOf(1));
            assertThat(backend.readCount()).isEqualTo(2);

            // order is now [0, 1]: reading 2 evicts the LRU block 0
            assertThat(cache.readBlock(2)).containsExactly(blockOf(2));
            assertThat(cache.getStats().evictions()).isEqualTo(1);
            assertThat(backend.readCount()).isEqualTo(3);

            // block 1 still cached, block 0 had to be re-read from the backend
            assertThat(cache.readBlock(1)).containsExactly(blockOf(1));
            assertThat(backend.readCount()).isEqualTo(3);
            assertThat(cache.readBlock(0)).containsExactly(blockOf(0));
            assertThat(backend.readCount()).isEqualTo(4);
            assertThat(cache.getStats().hits()).isEqualTo(3);
            assertThat(cache.getStats().misses()).isEqualTo(4);
        }
    }

    @Test
    void sequentialAccessTriggersPrefetchThatServesLaterReads() throws Exception {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, 2);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 64, 4, 2)) {
            for (long blockId = 0; blockId < 20; blockId++) {
                assertThat(cache.readBlock(blockId)).containsExactly(blockOf(blockId));
                Thread.sleep(5); // give the async read-ahead time to land in the cache
            }

            CacheStats stats = cache.getStats();
            assertThat(stats.prefetchesIssued()).isGreaterThan(0);
            // only the first few reads touch the backend directly; the read-ahead serves
            // the remaining ones from the cache
            assertThat(stats.misses()).isLessThanOrEqualTo(3);
            assertThat(stats.prefetchHits()).isGreaterThanOrEqualTo(15);
            assertThat(stats.hits()).isGreaterThanOrEqualTo(17);
        }
    }

    @Test
    void disablingPrefetchStopsReadAheadCompletely() {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, 0);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 128, 8, 2)) {
            cache.setPrefetchEnabled(false);
            for (long blockId = 0; blockId < 20; blockId++) {
                cache.readBlock(blockId);
            }
            CacheStats stats = cache.getStats();
            assertThat(stats.prefetchesIssued()).isZero();
            assertThat(stats.backendReads()).isEqualTo(20);
            assertThat(cache.getCurrentPrefetchDepth()).isZero();
        }
    }

    @Test
    void prefetchDepthAdaptsToRandomThenSequentialAccess() throws Exception {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, 0);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 1024, 8, 2)) {
            Random random = new Random(42);
            for (int i = 0; i < 100; i++) {
                cache.readBlock(random.nextInt(10_000) + 1_000_000L);
            }
            assertWithin(Duration.ofSeconds(1),
                    () -> cache.getCurrentPrefetchDepth() == 0,
                    "random access must drive prefetch depth to 0");

            long base = 2_000_000L;
            for (int i = 0; i < 64; i++) {
                cache.readBlock(base + i);
            }
            assertWithin(Duration.ofSeconds(1),
                    () -> cache.getCurrentPrefetchDepth() == 8,
                    "sequential access must raise prefetch depth to max");
        }
    }

    @Test
    void writesInvalidateCachedBlockAndNeverServeStaleData() {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, 0);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 16, 4, 2)) {
            cache.readBlock(5);
            long readsAfterPopulate = cache.getStats().backendReads();

            byte[] updated = blockOf(999);
            cache.writeBlock(5, updated);
            assertThat(cache.getStats().invalidations()).isEqualTo(1);

            // next read must hit the backend and return the new contents
            assertThat(cache.readBlock(5)).containsExactly(updated);
            assertThat(cache.getStats().backendReads()).isEqualTo(readsAfterPopulate + 1);
            assertThat(cache.readBlock(5)).containsExactly(updated);
            assertThat(cache.getStats().backendReads()).isEqualTo(readsAfterPopulate + 1);

            // an out-of-band change plus explicit invalidate() is consistent as well
            byte[] outOfBand = blockOf(123);
            backend.writeBlock(7, outOfBand);
            cache.readBlock(7);
            byte[] newer = blockOf(124);
            backend.writeBlock(7, newer);
            cache.invalidate(7);
            assertThat(cache.readBlock(7)).containsExactly(newer);
        }
    }

    @Test
    void staleInFlightPrefetchIsDiscardedAfterWrite() throws Exception {
        // The backend blocks reads of block 3, so the prefetch of 3 is in flight while
        // block 3 is overwritten; the stale result must never end up in the cache.
        GatedStorage backend = new GatedStorage(3);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 64, 4, 2)) {
            cache.readBlock(0);
            cache.readBlock(1);
            cache.readBlock(2); // streak >= 2 -> prefetch of 3..6 starts; 3 blocks
            Thread.sleep(50);

            byte[] updated = blockOf(4242);
            cache.writeBlock(3, updated);
            backend.openGate();

            // wait for the gated prefetch to finish and be discarded
            assertWithin(Duration.ofSeconds(2),
                    () -> cache.getStats().prefetchesIssued() >= 1,
                    "prefetch of block 3 should have been issued");
            Thread.sleep(100);

            assertThat(cache.readBlock(3)).containsExactly(updated);
        }
    }

    @Test
    void concurrentReadsOfSameBlockTriggerExactlyOneBackendRead() throws Exception {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, 30);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 16, 0, 2)) {
            int threads = 16;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            List<byte[]> results = new ArrayList<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        byte[] data = cache.readBlock(8);
                        synchronized (results) {
                            results.add(data);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            pool.shutdownNow();

            assertThat(failure.get()).isNull();
            assertThat(results).hasSize(threads);
            assertThat(backend.readCount()).isEqualTo(1);
            assertThat(cache.getStats().backendReads()).isEqualTo(1);
            assertThat(cache.getStats().hits()).isEqualTo(threads - 1);
            for (byte[] result : results) {
                assertThat(result).containsExactly(blockOf(8));
            }
        }
    }

    @Test
    void unusedPrefetchedBlocksAreCountedAsWastedWhenEvicted() throws Exception {
        SimulatedBlockStorage backend = new SimulatedBlockStorage(16, 0);
        try (CachedBlockStore cache = new CachedBlockStore(backend, 8, 4, 2)) {
            cache.readBlock(0);
            cache.readBlock(1);
            cache.readBlock(2); // prefetches 3,4,5

            assertWithin(Duration.ofSeconds(1),
                    () -> cache.cachedBlockCount() == 6,
                    "blocks 0..5 should be present (3 demand + 3 prefetched)");
            assertThat(cache.getStats().prefetchesIssued()).isEqualTo(3);

            // non-sequential distant reads evict the never-used prefetched blocks
            for (long blockId = 100; blockId < 100 + 37L * 10; blockId += 37) {
                cache.readBlock(blockId);
            }
            CacheStats stats = cache.getStats();
            assertThat(stats.wastedPrefetches()).isEqualTo(3);
            assertThat(stats.prefetchHits()).isZero();
        }
    }

    /**
     * Storage whose first read of one block blocks until {@link #openGate()} is called,
     * and then returns a fixed stale snapshot (as if the read had observed pre-write
     * contents). Later reads go through to the delegate and see the new contents.
     */
    private static final class GatedStorage implements BlockStorage {
        private final SimulatedBlockStorage delegate = new SimulatedBlockStorage(16, 0);
        private final long gatedBlockId;
        private final byte[] staleSnapshot;
        private final java.util.concurrent.atomic.AtomicBoolean staleServed =
                new java.util.concurrent.atomic.AtomicBoolean();
        private boolean gateOpen = false;

        GatedStorage(long gatedBlockId) {
            this.gatedBlockId = gatedBlockId;
            this.staleSnapshot = blockOf(777);
        }

        @Override
        public byte[] readBlock(long blockId) {
            if (blockId == gatedBlockId && staleServed.compareAndSet(false, true)) {
                synchronized (this) {
                    boolean interrupted = false;
                    while (!gateOpen) {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                return staleSnapshot.clone();
            }
            return delegate.readBlock(blockId);
        }

        @Override
        public void writeBlock(long blockId, byte[] data) {
            delegate.writeBlock(blockId, data);
        }

        synchronized void openGate() {
            gateOpen = true;
            notifyAll();
        }
    }

    private interface Check {
        boolean ok() throws Exception;
    }

    private static void assertWithin(Duration timeout, Check check, String description)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean ok;
        do {
            ok = check.ok();
            if (!ok) {
                Thread.sleep(5);
            }
        } while (!ok && System.nanoTime() < deadline);
        assertThat(ok).as(description).isTrue();
    }
}
