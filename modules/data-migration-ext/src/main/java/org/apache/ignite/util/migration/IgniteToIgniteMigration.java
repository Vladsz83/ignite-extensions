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

package org.apache.ignite.util.migration;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationCache;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationEntry;
import org.apache.ignite.util.migration.api.MigrationPartition;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.api.MigrationTypeMapping;
import org.apache.ignite.util.migration.impl.ThickClientMigrationOrigin;

import static org.apache.ignite.util.migration.api.MigrationChannelType.BINARY_METADATA;
import static org.apache.ignite.util.migration.api.MigrationChannelType.CACHE_CONFIGURATIONS;
import static org.apache.ignite.util.migration.api.MigrationChannelType.CACHE_DATA;
import static org.apache.ignite.util.migration.api.MigrationChannelType.METASTORAGE;
import static org.apache.ignite.util.migration.api.MigrationChannelType.TYPE_MAPPINGS;

/** One-shot migration with ordered metadata stages and a bounded, fail-fast data pipeline. */
public class IgniteToIgniteMigration {
    /** Runs a migration, closing both endpoints on success or failure. */
    public static void start(
        MigrationSource src,
        MigrationDestination dest,
        IgniteToIgniteMigrationConfiguration cfg
    ) throws IgniteCheckedException {
        cfg.validate();

        IgniteLogger log = cfg.getLogger();
        long start = System.currentTimeMillis();

        info(log, "Starting data migration [time=" + start + ", batchSize=" + cfg.getBatchSize() +
            ", readThreads=" + cfg.getReadThreads() + ", writeThreads=" + cfg.getWriteThreads() +
            ", readLimit=" + cfg.getReadLimitInSec() + ", writeLimit=" + cfg.getWriteLimitInSec() + ']');

        try (src; dest) {
            src.connect();
            dest.connect();

            if (src instanceof ThickClientMigrationOrigin && dest instanceof ThickClientMigrationOrigin &&
                ((ThickClientMigrationOrigin)src).clusterId().equals(((ThickClientMigrationOrigin)dest).clusterId()))
                throw new IgniteCheckedException("Source and destination must be different clusters");

            if (!cfg.isNoBinaryMetadataCreate()) {
                for (BinaryMetadata item : src.binaryMetadata()) {
                    BinaryMetadata meta = cfg.transform(BINARY_METADATA, item);

                    if (meta != null) {
                        dest.binaryMetadata(meta);
                        info(log, "Transferred binary metadata: " + meta.typeName());
                    }
                }
            }

            if (!cfg.isNoMappingsCreate()) {
                for (MigrationTypeMapping item : src.typeMappings()) {
                    MigrationTypeMapping mapping = cfg.transform(TYPE_MAPPINGS, item);

                    if (mapping != null) {
                        dest.typeMapping(mapping);
                        info(log, "Transferred type mapping: " + mapping.typeName());
                    }
                }
            }

            Map<String, String> names = new LinkedHashMap<>();
            Set<String> destNames = new HashSet<>();
            List<MigrationCache> caches = new ArrayList<>();

            for (MigrationCache item : src.caches()) {
                if (!cfg.includes(item.configuration()))
                    continue;

                MigrationCache cache = cfg.transform(CACHE_CONFIGURATIONS, item);

                if (cache == null)
                    continue;

                CacheConfiguration<?, ?> cacheCfg = cfg.getCacheConfigurationTransformer().apply(cache.configuration());

                if (cacheCfg == null)
                    continue;

                if (cacheCfg.getName() == null || cacheCfg.getName().isEmpty() || !destNames.add(cacheCfg.getName()))
                    throw new IgniteCheckedException("Invalid or duplicate transformed cache name: " + cacheCfg.getName());

                names.put(item.sourceName(), cacheCfg.getName());
                caches.add(new MigrationCache(item.sourceName(), cacheCfg, cache.sql()));
            }

            for (MigrationCache cache : caches) {
                dest.cache(cache, cfg);
                info(log, "Prepared destination cache [source=" + cache.sourceName() + ", creationDisabled=" +
                    cfg.isNoCacheCreate() + ", configuration=" + cache.configuration() + ']');
            }

            for (Map.Entry<String, Serializable> item : src.metastorage(cfg).entrySet()) {
                Map.Entry<String, Serializable> entry = cfg.transform(METASTORAGE, new SimpleImmutableEntry<>(item));

                if (entry != null) {
                    dest.metastorage(entry.getKey(), entry.getValue());
                    info(log, "Transferred distributed metadata: " + entry.getKey());
                }
            }

            transfer(src, dest, src.partitions(names), cfg);

            info(log, "Data migration completed [elapsedMs=" + (System.currentTimeMillis() - start) + ']');
        }
        catch (IgniteCheckedException | RuntimeException e) {
            log.error("Data migration failed", e);
            throw e;
        }
    }

    /** Runs readers and writers; the completion queue exposes either side's failures immediately. */
    private static void transfer(MigrationSource src, MigrationDestination dest, List<MigrationPartition> parts,
        IgniteToIgniteMigrationConfiguration cfg) throws IgniteCheckedException {
        Queue<MigrationPartition> pending = new ConcurrentLinkedQueue<>(parts);
        BlockingQueue<Batch> batches = new ArrayBlockingQueue<>(cfg.getQueueCapacity());
        AtomicInteger readers = new AtomicInteger(cfg.getReadThreads());
        AtomicInteger completed = new AtomicInteger();
        AtomicLong read = new AtomicLong();
        AtomicLong written = new AtomicLong();
        RateLimiter readLimiter = new RateLimiter(cfg.getReadLimitInSec());
        RateLimiter writeLimiter = new RateLimiter(cfg.getWriteLimitInSec());
        IgniteLogger log = cfg.getLogger();
        int threads = cfg.getReadThreads() + cfg.getWriteThreads();
        AtomicInteger threadIdx = new AtomicInteger();
        ExecutorService workers = Executors.newFixedThreadPool(threads,
            task -> new Thread(task, "data-migration-" + threadIdx.incrementAndGet()));
        ExecutorCompletionService<Void> completion = new ExecutorCompletionService<>(workers);
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(
            task -> new Thread(task, "data-migration-log"));

        try {
            reporter.scheduleAtFixedRate(() -> info(log, "Migration records [read=" + read.get() +
                ", written=" + written.get() + ']'), 1, 1, TimeUnit.MINUTES);

            for (int i = 0; i < cfg.getWriteThreads(); i++) {
                completion.submit(() -> {
                    while (readers.get() != 0 || !batches.isEmpty()) {
                        Batch batch = batches.poll(100, TimeUnit.MILLISECONDS);

                        if (batch != null) {
                            writeLimiter.acquire(batch.entries.size());
                            dest.write(batch.cache, batch.entries, cfg);
                            written.addAndGet(batch.entries.size());
                        }
                    }

                    return null;
                });
            }

            for (int i = 0; i < cfg.getReadThreads(); i++) {
                completion.submit(() -> {
                    try {
                        MigrationPartition part;

                        while ((part = pending.poll()) != null) {
                            long start = System.currentTimeMillis();
                            long cnt = 0;
                            List<MigrationEntry> batch = new ArrayList<>(cfg.getBatchSize());

                            info(log, "Reading partition [cache=" + part.sourceCache() + ", partition=" + part.partition() + ']');

                            try (QueryCursor<MigrationEntry> cursor = src.read(part, cfg)) {
                                for (MigrationEntry item : cursor) {
                                    readLimiter.acquire(1);
                                    read.incrementAndGet();
                                    cnt++;

                                    MigrationEntry entry = cfg.transform(CACHE_DATA, item);

                                    if (entry != null)
                                        batch.add(entry);

                                    if (batch.size() == cfg.getBatchSize()) {
                                        batches.put(new Batch(part.destinationCache(), batch));
                                        batch = new ArrayList<>(cfg.getBatchSize());
                                    }
                                }
                            }

                            if (!batch.isEmpty())
                                batches.put(new Batch(part.destinationCache(), batch));

                            info(log, "Finished partition [cache=" + part.sourceCache() + ", partition=" + part.partition() +
                                ", entries=" + cnt + ", elapsedMs=" + (System.currentTimeMillis() - start) +
                                ", completed=" + completed.incrementAndGet() + ", total=" + parts.size() + ']');
                        }

                        return null;
                    }
                    finally {
                        readers.decrementAndGet();
                    }
                });
            }

            for (int i = 0; i < threads; i++)
                completion.take().get();

            info(log, "Migration records [read=" + read.get() + ", written=" + written.get() + ']');
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
    private static void info(IgniteLogger log, String msg) {
        if (log.isInfoEnabled())
            log.info(msg);
    }

    /** A bounded-queue item, exclusively owned by a writer after publication. */
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

    /** One interruptible limiter shared by all workers on a side. */
    private static class RateLimiter {
        /** */
        private final int rate;

        /** */
        private long next = System.nanoTime();

        /** */
        RateLimiter(int rate) {
            this.rate = rate;
        }

        /** */
        synchronized void acquire(int permits) throws InterruptedException {
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();

            if (rate == 0)
                return;

            long now = System.nanoTime();

            next = Math.max(now, next) + TimeUnit.SECONDS.toNanos(1) * permits / rate;
            TimeUnit.NANOSECONDS.sleep(next - now);
        }
    }
}
