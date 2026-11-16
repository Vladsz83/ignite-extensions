/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util.migration.impl.channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationEntry;
import org.apache.ignite.util.migration.api.MigrationPartition;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.impl.MigrationCachePlan;

/** Transfers cache records through partition readers and a bounded queue of write batches. */
public class CacheDataMigrationChannel extends AbstractMigrationChannel<MigrationEntry> {
    /** */
    private final MigrationCachePlan cachePlan;

    /** */
    private final Queue<MigrationPartition> pending = new ConcurrentLinkedQueue<>();

    /** */
    private final BlockingQueue<Batch> batches;

    /** */
    private final Set<QueryCursor<MigrationEntry>> cursors = ConcurrentHashMap.newKeySet();

    /** */
    private final AtomicInteger readers;

    /** */
    private final AtomicInteger completed = new AtomicInteger();

    /** */
    private final AtomicLong read = new AtomicLong();

    /** */
    private final AtomicLong written = new AtomicLong();

    /** */
    private final BasicRateLimiter readLimiter;

    /** */
    private final BasicRateLimiter writeLimiter;

    /** */
    private int partsCnt;

    /**
     * @param src Connected source.
     * @param dest Connected destination.
     * @param cfg Migration configuration.
     * @param cachePlan Cache selection for this migration.
     */
    public CacheDataMigrationChannel(
        MigrationSource src,
        MigrationDestination dest,
        IgniteToIgniteMigrationConfiguration cfg,
        MigrationCachePlan cachePlan
    ) {
        super(src, dest, cfg);

        this.cachePlan = cachePlan;
        batches = new ArrayBlockingQueue<>(cfg.getQueueCapacity());
        readers = new AtomicInteger(cfg.getReadThreads());
        readLimiter = new BasicRateLimiter(cfg.getReadLimitInSec());
        writeLimiter = new BasicRateLimiter(cfg.getWriteLimitInSec());
    }

    /** {@inheritDoc} */
    @Override protected void transfer0() throws IgniteCheckedException {
        List<MigrationPartition> parts = src.partitions(cachePlan.cacheNames());

        pending.addAll(parts);
        partsCnt = parts.size();

        int threads = cfg.getReadThreads() + cfg.getWriteThreads();
        ExecutorService workers = Executors.newFixedThreadPool(threads,
            new IgniteThreadFactory(null, "data-migration"));
        ExecutorCompletionService<Void> completion = new ExecutorCompletionService<>(workers);
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(
            new IgniteThreadFactory(null, "data-migration-log"));

        try {
            reporter.scheduleAtFixedRate(this::logProgress, 1, 1, TimeUnit.MINUTES);

            for (int i = 0; i < cfg.getWriteThreads(); i++)
                completion.submit(this::write);

            for (int i = 0; i < cfg.getReadThreads(); i++)
                completion.submit(this::read);

            for (int i = 0; i < threads; i++)
                completion.take().get();

            logProgress();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Migration interrupted", e);
        }
        catch (ExecutionException e) {
            throw new IgniteCheckedException("Migration worker failed", e.getCause());
        }
        finally {
            reporter.shutdownNow();
            workers.shutdownNow();

            cursors.forEach(cursor -> U.close(cursor, log));

            try {
                if (!workers.awaitTermination(30, TimeUnit.SECONDS))
                    log.warning("Migration workers are still stopping; closing cluster connections");
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** */
    private Void write() throws IgniteCheckedException, InterruptedException {
        while (readers.get() != 0 || !batches.isEmpty()) {
            Batch batch = batches.poll(100, TimeUnit.MILLISECONDS);

            if (batch != null) {
                writeLimiter.acquire(batch.entries.size());
                dest.write(batch.cache, batch.entries, cfg);
                written.addAndGet(batch.entries.size());
            }
        }

        return null;
    }

    /** */
    private Void read() throws IgniteCheckedException, InterruptedException {
        try {
            MigrationPartition part;

            while ((part = pending.poll()) != null) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                read(part);
            }

            return null;
        }
        finally {
            readers.decrementAndGet();
        }
    }

    /** */
    private void read(MigrationPartition part) throws IgniteCheckedException, InterruptedException {
        long start = System.currentTimeMillis();
        long cnt = 0;
        List<MigrationEntry> batch = new ArrayList<>(cfg.getBatchSize());

        info("Reading partition [cache=" + part.sourceCache() + ", partition=" + part.partition() + ']');

        QueryCursor<MigrationEntry> cursor = src.read(part, cfg);

        cursors.add(cursor);

        try (cursor) {
            for (MigrationEntry item : cursor) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                if (item == null)
                    continue;

                readLimiter.acquire(1);
                read.incrementAndGet();
                cnt++;

                MigrationEntry entry = transform(item);

                if (entry != null)
                    batch.add(entry);

                if (batch.size() == cfg.getBatchSize()) {
                    batches.put(new Batch(part.destinationCache(), batch));
                    batch = new ArrayList<>(cfg.getBatchSize());
                }
            }
        }
        finally {
            cursors.remove(cursor);
        }

        if (!batch.isEmpty())
            batches.put(new Batch(part.destinationCache(), batch));

        info("Finished partition [cache=" + part.sourceCache() + ", partition=" + part.partition() +
            ", entries=" + cnt + ", elapsedMs=" + (System.currentTimeMillis() - start) +
            ", completed=" + completed.incrementAndGet() + ", total=" + partsCnt + ']');
    }

    /** */
    private void logProgress() {
        info("Migration records [read=" + read.get() + ", written=" + written.get() + ']');
    }

    /** A batch exclusively owned by a writer after publication. */
    private static class Batch {
        /** */
        private final String cache;

        /** */
        private final List<MigrationEntry> entries;

        /** */
        Batch(String cache, List<MigrationEntry> entries) {
            this.cache = cache;
            this.entries = entries;
        }
    }
}
