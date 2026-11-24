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

package org.apache.ignite.util.migration.category.cache.data;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;

/** Transfers cache records through partition readers and a bounded queue of write batches. */
public class CacheDataTransferrer extends AbstractCacheDataTransferrer<IgniteEx> {
    /** Partitions not yet assigned to a reader; consumed round-robin by primary node. */
    private final ConcurrentLinkedQueue<T2<String, Integer>> partitionsToRead = new ConcurrentLinkedQueue<>();

    /** Total source partitions selected for transfer. */
    private int partsCnt;

    /** Completed partition scans. */
    private final AtomicInteger completed = new AtomicInteger();

    /**
     * @param src Connected thick source.
     * @param log Logger.
     * @param cfg Data transfer knobs.
     * @param cacheNameMap Source cache name to destination cache name.
     */
    public CacheDataTransferrer(
        IgniteEx src,
        IgniteLogger log,
        CacheDataMigrationConfiguration cfg,
        Map<String, String> cacheNameMap
    ) {
        super(src, log, cfg, cacheNameMap);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTransfer(
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    ) {
        List<T2<String, Integer>> parts = partitions(src, cacheNameMap.keySet());

        partitionsToRead.addAll(parts);
        partsCnt = parts.size();
    }

    /** {@inheritDoc} */
    @Override protected int readWorkerCount(int readerThreads) {
        return readerThreads;
    }

    /** {@inheritDoc} */
    @Override protected void submitReadTasks(
        ExecutorCompletionService<Void> exec,
        int readWorkers,
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    ) {
        for (int i = 0; i < readWorkers; i++)
            exec.submit(() -> read(transformer));
    }

    /** Reads partitions from the shared queue until it is exhausted. */
    private Void read(@Nullable MigrationTransformer<CacheMigrationEntry> transformer) throws InterruptedException {
        try {
            T2<String, Integer> task;

            while ((task = partitionsToRead.poll()) != null) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                readPartition(task, transformer);
            }

            return null;
        }
        finally {
            readers.decrementAndGet();
        }
    }

    /** Scans one partition into the bounded writer queue. */
    private void readPartition(
        T2<String, Integer> cacheAndPart,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    ) throws InterruptedException {
        long start = System.nanoTime();

        String cache = cacheAndPart.get1();
        int part = cacheAndPart.get2();

        long cnt = 0;
        List<CacheMigrationEntry> batch = new ArrayList<>(cfg.batchSize());

        String destCache = cacheNameMap.get(cache);

        info("Reading partition [cache=" + cache + ", partition=" + part + "].");

        QueryCursor<CacheMigrationEntry> cursor = readCursor(src, cache, part);

        cursors.add(cursor);

        try (cursor) {
            for (CacheMigrationEntry item : cursor) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                if (item == null)
                    continue;

                acquireReadPermit();

                read.incrementAndGet();
                cnt++;

                if (transformer != null)
                    item = transformer.transform(item);

                if (item == null)
                    continue;

                batch = addBatchEntry(destCache, item, batch);
            }
        }
        finally {
            cursors.remove(cursor);
        }

        flushBatch(destCache, batch);

        info("Finished partition [cache=" + cache + ", partition=" + part +
            ", entries=" + cnt + ", elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) +
            ", completed=" + completed.incrementAndGet() + ", total=" + partsCnt + "].");
    }

    /**
     * Partitions interleaved round-robin by primary node so that concurrent readers
     * naturally spread across different servers.
     */
    private static List<T2<String, Integer>> partitions(IgniteEx ig, Collection<String> cacheNames) {
        Map<UUID, Deque<T2<String, Integer>>> byNode = new LinkedHashMap<>();

        for (String cache : cacheNames) {
            Affinity<?> aff = ig.affinity(cache);

            for (int p = 0; p < aff.partitions(); p++)
                byNode.computeIfAbsent(aff.mapPartitionToNode(p).id(), k -> new ArrayDeque<>())
                    .add(new T2<>(cache, p));
        }

        List<T2<String, Integer>> res = new ArrayList<>();
        boolean added = true;

        while (added) {
            added = false;

            for (Deque<T2<String, Integer>> parts : byNode.values()) {
                if (!parts.isEmpty()) {
                    res.add(parts.removeFirst());
                    added = true;
                }
            }
        }

        return res;
    }

    /** */
    @SuppressWarnings("unchecked")
    QueryCursor<CacheMigrationEntry> readCursor(IgniteEx ig, String cache, int part) {
        openSourceCache(ig, cache);

        GridCacheContext<Object, Object> ctx = ig.cachex(cache).context();

        return new QueryCursorImpl<>(() -> {
            try {
                ClusterNode primaryNode = ig.affinity(cache).mapPartitionToNode(part);

                CacheQuery<CacheMigrationEntry> qry = ctx.queries().createScanQuery(
                    null,
                    new CaccheMigrationEntryReader(cache, cfg.skipExpireTime()),
                    part,
                    true,
                    false,
                    Collections.emptySet()
                );

                qry.pageSize(cfg.batchSize());

                return ctx.kernalContext().query().executeQuery(SCAN, cache, ctx,
                    new IgniteOutClosureX<GridCloseableIterator<CacheMigrationEntry>>() {
                        @Override public GridCloseableIterator<CacheMigrationEntry> applyx() throws IgniteCheckedException {
                            return ctx.queries().scanQueryDistributed(qry, Collections.singleton(primaryNode));
                        }
                    }, true);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to scan source partition [cache=" + cache + "].", e);
            }
        });
    }

    /** Ensures the source cache is open and started before scanning it. */
    private static void openSourceCache(IgniteEx ig, String name) {
        try {
            ig.context().cache().getOrStartCache(name);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to open source cache [cache=" + name + "].", e);
        }
    }
}
