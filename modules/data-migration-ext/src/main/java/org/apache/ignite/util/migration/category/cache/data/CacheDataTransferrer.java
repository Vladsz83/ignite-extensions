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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastructures.GridCacheCountDownLatchValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheLockState;
import org.apache.ignite.internal.processors.datastructures.GridCacheSemaphoreState;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCachePlan;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;

/** Transfers cache records through partition readers and a bounded queue of write batches. */
public class CacheDataTransferrer extends AbstractCategoryTransferrer<Object, MigrationEntry> {
    /** Destination connection. */
    private final Object to;

    /** */
    private final CacheDataMigrationConfiguration cfg;

    /** */
    private final MigrationCachePlan cachePlan;

    /** Source cache to partition number. */
    private final Queue<Map.Entry<String, Integer>> pending = new ConcurrentLinkedQueue<>();

    /** */
    private final BlockingQueue<Batch> batches;

    /** */
    private final Set<QueryCursor<MigrationEntry>> cursors = ConcurrentHashMap.newKeySet();

    /** */
    private final Map<String, IgniteDataStreamer<Object, Object>> streamers = new ConcurrentHashMap<>();

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

    /** Thick source connection for this transfer. */
    private IgniteEx srcIg;

    /** Thick destination connection for this transfer. */
    private IgniteEx destIg;

    /** Whether binary metadata creation is disabled. */
    private final boolean noBinaryMetadataCreate;

    /** Whether platform type mapping registration is disabled. */
    private final boolean noMappingsCreate;

    /**
     * @param from Connected source.
     * @param to Connected destination.
     * @param log Logger.
     * @param cfg Data transfer knobs.
     * @param cachePlan Cache selection for this migration.
     */
    public CacheDataTransferrer(
        Object from,
        Object to,
        IgniteLogger log,
        CacheDataMigrationConfiguration cfg,
        MigrationCachePlan cachePlan
    ) {
        this(from, to, log, cfg, cachePlan, false, false);
    }

    /**
     * @param from Connected source.
     * @param to Connected destination.
     * @param log Logger.
     * @param cfg Data transfer knobs.
     * @param cachePlan Cache selection for this migration.
     * @param noBinaryMetadataCreate Whether binary metadata creation is disabled.
     * @param noMappingsCreate Whether mapping registration is disabled.
     */
    public CacheDataTransferrer(
        Object from,
        Object to,
        IgniteLogger log,
        CacheDataMigrationConfiguration cfg,
        MigrationCachePlan cachePlan,
        boolean noBinaryMetadataCreate,
        boolean noMappingsCreate
    ) {
        super(from, MigrationCategory.CACHE_ENTRY, MigrationEntry.class, log);

        this.to = to;
        this.cfg = cfg;
        this.cachePlan = cachePlan;
        this.noBinaryMetadataCreate = noBinaryMetadataCreate;
        this.noMappingsCreate = noMappingsCreate;
        batches = new ArrayBlockingQueue<>(cfg.queueCapacity());
        readers = new AtomicInteger();
        readLimiter = new BasicRateLimiter(cfg.readLimitInSec());
        writeLimiter = new BasicRateLimiter(cfg.writeLimitInSec());
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(
        MigrationDestination dest,
        MigrationTransformer<MigrationEntry> transformer
    ) {
        Object src = this.src;
        Object destOrigin = to;

        if (!(src instanceof IgniteEx) || !(destOrigin instanceof IgniteEx)) {
            new ThinClientCacheDataTransferrer(src, destOrigin, dest, log, cfg, cachePlan, this,
                noBinaryMetadataCreate, noMappingsCreate).transfer(transformer);

            return;
        }

        srcIg = (IgniteEx)src;
        destIg = (IgniteEx)destOrigin;

        Map<String, String> cacheNames = cachePlan.cacheNames();
        List<Map.Entry<String, Integer>> parts = partitions(srcIg, cacheNames.keySet());

        pending.addAll(parts);
        partsCnt = parts.size();

        CacheDataMigrationThreadPools workers = new CacheDataMigrationThreadPools(cfg);
        readers.set(workers.readerThreads());

        info("Cache data worker pools [readers=" + workers.readerThreads() + ", readerPool=dedicated, writers=" +
            workers.writerThreads() + ", writerPool=dedicated].");

        int threads = workers.readerThreads() + workers.writerThreads();
        BlockingQueue<Future<Void>> completed = new LinkedBlockingQueue<>();
        ExecutorCompletionService<Void> readerCompletion =
            new ExecutorCompletionService<>(workers.readerPool(), completed);
        ExecutorCompletionService<Void> writerCompletion =
            new ExecutorCompletionService<>(workers.writerPool(), completed);
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(
            new IgniteThreadFactory(null, "data-migration-log"));
        boolean success = false;

        try {
            reporter.scheduleAtFixedRate(this::logProgress, 1, 1, TimeUnit.MINUTES);

            for (int i = 0; i < workers.writerThreads(); i++)
                writerCompletion.submit(this::write);

            for (int i = 0; i < workers.readerThreads(); i++)
                readerCompletion.submit(() -> read(transformer));

            for (int i = 0; i < threads; i++)
                completed.take().get();

            success = true;
            logProgress();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException("Migration interrupted.", e);
        }
        catch (ExecutionException e) {
            throw new IgniteException("Migration worker failed.", e.getCause());
        }
        finally {
            reporter.shutdownNow();

            cursors.forEach(cursor -> U.close(cursor, log));

            try {
                closeStreamers(success);
            }
            finally {
                workers.stop(success);
            }
        }
    }

    /** */
    private Void write() throws IgniteInterruptedCheckedException {
        try {
            while (readers.get() != 0 || !batches.isEmpty()) {
                Batch batch = batches.poll(100, TimeUnit.MILLISECONDS);

                if (batch != null) {
                    writeLimiter.acquire(batch.entries.size());
                    writeBatch(destIg, batch.cache, batch.entries);
                    written.addAndGet(batch.entries.size());
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw e;
        }

        return null;
    }

    /** Flushes streamers once every writer has finished. */
    private void closeStreamers(boolean success) {
        IgniteException error = null;

        for (IgniteDataStreamer<Object, Object> streamer : streamers.values()) {
            try {
                if (success)
                    streamer.close();
                else
                    streamer.close(true);
            }
            catch (RuntimeException e) {
                if (success && error == null)
                    error = new IgniteException("Failed to flush DataStreamer migration buffers.", e);
                else if (error != null)
                    error.addSuppressed(e);
                else
                    log.warning("Failed to cancel DataStreamer after migration failure.", e);
            }
        }

        streamers.clear();

        if (error != null)
            throw error;
    }

    /** */
    private Void read(MigrationTransformer<MigrationEntry> transformer) throws InterruptedException {
        try {
            Map.Entry<String, Integer> task;

            while ((task = pending.poll()) != null) {
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

    /** */
    private void readPartition(
        Map.Entry<String, Integer> task,
        MigrationTransformer<MigrationEntry> transformer
    ) throws InterruptedException {
        long start = System.nanoTime();
        long cnt = 0;
        List<MigrationEntry> batch = new ArrayList<>(cfg.batchSize());
        String cache = task.getKey();
        int part = task.getValue();
        String destCache = cachePlan.cacheNames().get(cache);

        info("Reading partition [cache=" + cache + ", partition=" + part + "].");

        QueryCursor<MigrationEntry> cursor = readCursor(srcIg, cache, part);

        cursors.add(cursor);

        try (cursor) {
            for (MigrationEntry item : cursor) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                if (item == null)
                    continue;

                try {
                    readLimiter.acquire(1);
                }
                catch (IgniteInterruptedCheckedException e) {
                    Thread.currentThread().interrupt();

                    throw new InterruptedException();
                }

                read.incrementAndGet();
                cnt++;

                if (transformer != null)
                    item = transformer.transform(item);

                if (item == null)
                    continue;

                batch.add(item);

                if (batch.size() == cfg.batchSize()) {
                    batches.put(new Batch(destCache, batch));
                    batch = new ArrayList<>(cfg.batchSize());
                }
            }
        }
        finally {
            cursors.remove(cursor);
        }

        if (!batch.isEmpty())
            batches.put(new Batch(destCache, batch));

        info("Finished partition [cache=" + cache + ", partition=" + part +
            ", entries=" + cnt + ", elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) +
            ", completed=" + completed.incrementAndGet() + ", total=" + partsCnt + "].");
    }

    /** */
    private void logProgress() {
        info("Migration records [read=" + read.get() + ", written=" + written.get() + "].");
    }

    // --- Source-side helpers ---

    /** Partitions interleaved by primary server. */
    static List<Map.Entry<String, Integer>> partitions(IgniteEx ig, Collection<String> cacheNames) {
        Map<UUID, Queue<Map.Entry<String, Integer>>> perNode = new LinkedHashMap<>();

        for (String cache : cacheNames) {
            for (int part = 0; part < ig.affinity(cache).partitions(); part++) {
                UUID primary = ig.affinity(cache).mapPartitionToNode(part).id();

                perNode.computeIfAbsent(primary, key -> new ArrayDeque<>())
                    .add(new SimpleImmutableEntry<>(cache, part));
            }
        }

        List<Map.Entry<String, Integer>> res = new ArrayList<>();
        boolean more;

        do {
            more = false;

            for (Queue<Map.Entry<String, Integer>> queue : perNode.values()) {
                Map.Entry<String, Integer> task = queue.poll();

                if (task != null) {
                    res.add(task);
                    more = true;
                }
            }
        }
        while (more);

        return res;
    }

    /** */
    @SuppressWarnings("unchecked")
    QueryCursor<MigrationEntry> readCursor(IgniteEx ig, String cache, int part) {
        openCache(ig, cache);

        GridCacheContext<Object, Object> ctx = ig.cachex(cache).context();
        int pageSize = cfg.batchSize();

        return new QueryCursorImpl<>(() -> {
            CacheOperationContext prev = ctx.gate().enter(null);

            try {
                ctx.checkSecurity(CACHE_READ);

                if (ctx.topology().lostPartitions().contains(part))
                    throw new IgniteException("Source partition is lost: " + part + '.');

                ClusterNode primary = ig.affinity(cache).mapPartitionToNode(part);
                CacheQuery<MigrationEntry> qry = ctx.queries().createScanQuery(null,
                    new MigrationEntryReader(cache), part, true, false, null,
                    Collections.emptySet());

                qry.pageSize(pageSize);

                // Public partition ScanQuery can select a backup. Route the same scan engine to the primary explicitly.
                return ctx.kernalContext().query().executeQuery(SCAN, cache, ctx,
                    new IgniteOutClosureX<GridCloseableIterator<MigrationEntry>>() {
                        @Override public GridCloseableIterator<MigrationEntry> applyx() throws IgniteCheckedException {
                            return ctx.queries().scanQueryDistributed(qry, Collections.singleton(primary));
                        }
                    }, true);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to scan source partition: " + cache + '.', e);
            }
            finally {
                ctx.gate().leave(prev);
            }
        });
    }

    // --- Destination-side helpers ---

    /** */
    private void writeBatch(IgniteEx ig, String name, List<MigrationEntry> entries) {
        IgniteCache<Object, Object> cache = openPublicCache(ig, name).withKeepBinary().withSkipStore();
        IgniteInternalCache<Object, Object> internal = ig.cachex(name).keepBinary().withSkipStore();
        GridCacheContext<Object, Object> ctx = internal.context();
        boolean keepVer = !cfg.skipConflictVersion();

        if (cfg.useDataStreamer()) {
            if (entries.stream().anyMatch(e -> keepVer && e.version() != null ||
                !cfg.skipExpireTime() && e.expireTime() != 0)) {
                throw new IgniteException("DataStreamer cannot preserve entry metadata. " +
                    "Use --skip-conflict-version and --skip-expire-time, or disable DataStreamer.");
            }

            IgniteDataStreamer<Object, Object> streamer = streamers.computeIfAbsent(name, key -> {
                IgniteDataStreamer<Object, Object> res = ig.dataStreamer(key);

                try {
                    res.skipStore(true);
                    cfg.dataStreamerConfiguration().accept(res);

                    return res;
                }
                catch (RuntimeException e) {
                    res.close(true);

                    throw e;
                }
            });

            for (MigrationEntry entry : entries)
                streamer.addData(destinationKey(entry.key()), destinationValue(ctx, entry));

            return;
        }

        if (keepVer && !ctx.conflictNeedResolve()) {
            throw new IgniteException("Conflict resolution must be configured on the destination servers " +
                "and client node to preserve conflict versions [cache=" + name +
                "]. Use --skip-conflict-version to ignore source versions.");
        }

        if (keepVer || !cfg.skipExpireTime() && entries.stream().anyMatch(e -> e.expireTime() != 0)) {
            Map<KeyCacheObject, GridCacheDrInfo> batch = new LinkedHashMap<>();

            for (MigrationEntry entry : entries) {
                long expire = cfg.skipExpireTime() ? 0 : entry.expireTime();

                if (expire > 0 && expire <= System.currentTimeMillis())
                    continue;

                GridCacheVersion ver = keepVer ? entry.version() :
                    ctx.versions().next(ctx.affinity().affinityTopologyVersion().topologyVersion());

                if (ver == null)
                    throw new IgniteException("Missing source conflict version for cache: " + name + '.');

                ver = ver.conflictVersion();

                // Recalculate affinity: source and destination partition counts may differ.
                Object key = destinationKey(entry.key());
                KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

                Object val = destinationValue(ctx, entry);

                batch.put(cacheKey, expire == 0 ? new GridCacheDrInfo(ctx.toCacheObject(val), ver) :
                    new GridCacheDrExpirationInfo(ctx.toCacheObject(val), ver, 0, expire));
            }

            if (!batch.isEmpty()) {
                try {
                    internal.putAllConflict(batch);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to write conflict-aware batch [cache=" + name + "].", e);
                }
            }
        }
        else {
            Map<Object, Object> batch = new LinkedHashMap<>();

            for (MigrationEntry entry : entries)
                batch.put(destinationKey(entry.key()), destinationValue(ctx, entry));

            if (!cfg.skipExpireTime())
                cache = cache.withExpiryPolicy(new ModifiedExpiryPolicy(Duration.ETERNAL));

            if (batch.size() == 1) {
                Map.Entry<Object, Object> entry = batch.entrySet().iterator().next();

                cache.put(entry.getKey(), entry.getValue());
            }
            else if (!batch.isEmpty())
                cache.putAll(batch);
        }
    }

    /** Discards any source-side cached affinity partition. */
    private static Object destinationKey(Object key) {
        return key instanceof KeyCacheObject ? ((KeyCacheObject)key).copy(-1) : key;
    }

    /** */
    private static Object destinationValue(GridCacheContext<?, ?> ctx, MigrationEntry entry) {
        return ctx.dataStructuresCache() ? dataStructureValue(ctx, entry.value()) : entry.value();
    }

    /** Rebase cluster timestamps without transferring source node/thread ownership. */
    private static Object dataStructureValue(GridCacheContext<?, ?> ctx, Object val) {
        long gridStartTime = ctx.discovery().gridStartTime();

        if (val instanceof GridCacheCountDownLatchValue) {
            GridCacheCountDownLatchValue latch = (GridCacheCountDownLatchValue)val;
            GridCacheCountDownLatchValue res = new GridCacheCountDownLatchValue(latch.initialCount(),
                latch.autoDelete(), gridStartTime);

            res.set(latch.get());

            return res;
        }

        if (val instanceof GridCacheSemaphoreState) {
            GridCacheSemaphoreState sem = (GridCacheSemaphoreState)val;

            if (sem.getWaiters() != null && sem.getWaiters().values().stream().anyMatch(cnt -> cnt != 0))
                throw new IgniteException("Cannot migrate a semaphore with acquired permits.");

            GridCacheSemaphoreState res = new GridCacheSemaphoreState(sem.getCount(), new HashMap<>(),
                sem.isFailoverSafe(), gridStartTime);

            res.setBroken(sem.isBroken());

            return res;
        }

        if (val instanceof GridCacheLockState) {
            GridCacheLockState lock = (GridCacheLockState)val;

            if (lock.get() != 0 || !lock.getNodes().isEmpty() ||
                lock.getConditionMap().values().stream().anyMatch(nodes -> !nodes.isEmpty()) ||
                lock.getSignals() != null && lock.getSignals().values().stream().anyMatch(signals -> !signals.isEmpty()))
                throw new IgniteException("Cannot migrate a lock with an owner, waiters or pending signals.");

            GridCacheLockState res = new GridCacheLockState(0, null, 0, lock.isFailoverSafe(), lock.isFair(), gridStartTime);

            res.setConditionMap(new HashMap<>(lock.getConditionMap()));

            return res;
        }

        return val;
    }

    // --- Shared helpers ---

    /** */
    private static void openCache(IgniteEx ig, String name) {
        try {
            ig.context().cache().getOrStartCache(name);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to open migration cache: " + name + '.', e);
        }
    }

    /** Opens user or data structure caches without exposing system caches through the public API. */
    private static IgniteCache<Object, Object> openPublicCache(IgniteEx ig, String name) {
        openCache(ig, name);

        return ((IgniteCacheProxyImpl<Object, Object>)ig.context().cache().<Object, Object>jcache(name))
            .gatewayWrapper();
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
