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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastructures.GridCacheCountDownLatchValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheLockState;
import org.apache.ignite.internal.processors.datastructures.GridCacheSemaphoreState;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.jetbrains.annotations.Nullable;

/**
 * Shared reader/writer pipeline for cache-data transferrers: a bounded queue of write batches, rate limits,
 * a progress reporter and the worker-pool lifecycle. Batches are written with the best API exposed by the
 * destination connection; an unknown destination type is written through the generic
 * {@link MigrationDestination} accept contract. Subclasses provide the source-specific read tasks.
 *
 * @param <SRC> Type of the connection used to read from the source cluster.
 */
public abstract class AbstractCacheDataTransferrer<SRC> extends AbstractCategoryTransferrer<SRC, CacheMigrationEntry> {
    /** Progress reporter period in seconds. */
    private static final int PROGRESS_PERIOD = 20;

    /** Progress reporter thread name. */
    private static final String REPORTER_THREAD_NAME = "migration-cache-log";

    /** Data transfer knobs. */
    protected final CacheDataMigrationConfiguration cfg;

    /** Source cache name to destination cache name. */
    protected final Map<String, String> cacheNameMap;

    /** Bounded queue of writer batches. */
    private final BlockingQueue<Batch> batches;

    /** Active source cursors. */
    protected final Set<QueryCursor<?>> cursors = ConcurrentHashMap.newKeySet();

    /** Remaining reader workers. */
    protected final AtomicInteger readers;

    /** Read entry count. */
    protected final AtomicLong read = new AtomicLong();

    /** Written entry count. */
    protected final AtomicLong written = new AtomicLong();

    /** Global read limiter. */
    protected final BasicRateLimiter readLimiter;

    /** Global write limiter. */
    protected final BasicRateLimiter writeLimiter;

    /** Destination streamers; closed after all writer threads have finished. */
    private final Map<String, IgniteDataStreamer<Object, Object>> streamers = new ConcurrentHashMap<>();

    /** Caches already reported as thin conflict-aware writes. */
    private final Set<String> thinConflictWarned = ConcurrentHashMap.newKeySet();

    /**
     * @param src Connected source.
     * @param log Logger.
     * @param cfg Data transfer settings.
     * @param cacheNameMap Source cache name to destination cache name.
     */
    protected AbstractCacheDataTransferrer(
        SRC src,
        IgniteLogger log,
        CacheDataMigrationConfiguration cfg,
        Map<String, String> cacheNameMap
    ) {
        super(src, MigrationCategory.CACHE_ENTRY, CacheMigrationEntry.class, log);

        this.cfg = cfg;
        this.cacheNameMap = Collections.unmodifiableMap(cacheNameMap);
        this.batches = new ArrayBlockingQueue<>(cfg.queueCapacity());
        this.readers = new AtomicInteger();
        this.readLimiter = new BasicRateLimiter(cfg.readLimitInSec());
        this.writeLimiter = new BasicRateLimiter(cfg.writeLimitInSec());
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(MigrationDestination<?> dest, @Nullable MigrationTransformer<CacheMigrationEntry> transformer) {
        if (cacheNameMap.isEmpty()) {
            info("No caches selected for cache data transfer.");

            return;
        }

        if (!(dest.origin() instanceof IgniteEx) && !(dest.origin() instanceof IgniteClient)) {
            warn("Destination connection type is not recognized for optimized cache writes " +
                "[type=" + (dest.origin() == null ? "null" : dest.origin().getClass().getSimpleName()) +
                "]. Conflict versions, expiration metadata and DataStreamer are not available.");
        }

        beforeTransfer(dest, transformer);

        int readThreads = cfg.readThreads();
        int writeThreads = cfg.writeThreads();
        int readWorkers = readWorkerCount(readThreads);
        readers.set(readWorkers);

        info("Cache data worker pools [readers=" + readWorkers + ", writers=" + writeThreads + "].");

        boolean success = false;
        BlockingQueue<Future<Void>> futures = new LinkedBlockingQueue<>();

        ExecutorService tPool = Executors.newFixedThreadPool(
            readWorkers + writeThreads,
            new IgniteThreadFactory(null, "migration-cache-worker")
        );

        ExecutorCompletionService<Void> exec = new ExecutorCompletionService<>(tPool, futures);

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(
            new IgniteThreadFactory(null, REPORTER_THREAD_NAME));

        try {
            reporter.scheduleAtFixedRate(this::logProgress, PROGRESS_PERIOD, PROGRESS_PERIOD, TimeUnit.SECONDS);

            submitReadTasks(exec, readWorkers, dest, transformer);

            for (int i = 0; i < writeThreads; i++)
                exec.submit(() -> write(dest));

            for (int i = 0; i < readWorkers + writeThreads; i++)
                futures.take().get();

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
                if (success)
                    flushStreamers();
                else
                    discardStreamers();
            }
            finally {
                stopPool(tPool, success);
            }
        }
    }

    /** Stops a transferrer-owned executor. */
    private static void stopPool(ExecutorService pool, boolean success) {
        if (success)
            pool.shutdown();
        else
            pool.shutdownNow();
    }

    /**
     * Hook run once, after the empty-selection check and before the worker pools are created.
     * Default: no-op.
     */
    protected void beforeTransfer(MigrationDestination<?> dest, @Nullable MigrationTransformer<CacheMigrationEntry> transformer) {
        // No-op.
    }

    /** @param readerThreads Configured reader worker count. @return Number of reader workers to start. */
    protected abstract int readWorkerCount(int readerThreads);

    /**
     * Submits one read task per reader worker.
     *
     * @param exec Worker completion service.
     * @param readWorkers Number of reader workers.
     * @param dest Migration destination.
     * @param transformer Entry transformer.
     */
    protected abstract void submitReadTasks(
        ExecutorCompletionService<Void> exec,
        int readWorkers,
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    );

    /** Consumes writer batches until every reader has finished. */
    private Void write(MigrationDestination<?> dest) throws IgniteInterruptedCheckedException {
        try {
            while (readers.get() != 0 || !batches.isEmpty()) {
                Batch batch = batches.poll(100, TimeUnit.MILLISECONDS);

                if (batch != null) {
                    writeLimiter.acquire(batch.entries.size());

                    writeBatch(dest, batch.cache, batch.entries);

                    written.addAndGet(batch.entries.size());
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }

        return null;
    }

    /** Writes a batch using the best API exposed by the destination connection. */
    void writeBatch(MigrationDestination<?> dest, String cache, List<CacheMigrationEntry> entries) {
        Object origin = dest.origin();

        if (origin instanceof IgniteEx ig)
            writeThickBatch(ig, cache, entries);
        else if (origin instanceof IgniteClient client)
            writeThinBatch(client, cache, entries);
        else
            dest.acceptAll(MigrationCategory.CACHE_ENTRY, new ArrayList<>(entries));
    }

    /** Logs read/write totals. */
    protected void logProgress() {
        info("Migration records [read=" + read.get() + ", written=" + written.get() + "].");
    }

    /**
     * Acquires a single read permit, translating an interrupt into {@link InterruptedException}.
     *
     * @throws InterruptedException If the reader is interrupted while waiting for the read rate limit.
     */
    protected final void acquireReadPermit() throws InterruptedException {
        try {
            readLimiter.acquire(1);
        }
        catch (IgniteInterruptedCheckedException e) {
            Thread.currentThread().interrupt();

            throw new InterruptedException();
        }
    }

    /**
     * Adds an entry to the in-progress batch, flushing and resetting it once the batch is full.
     *
     * @param destCache Destination cache.
     * @param item Entry.
     * @param batch Current batch buffer.
     * @return The buffer to keep appending to (a fresh one if the previous was flushed).
     * @throws InterruptedException If the writer queue is full and the reader is interrupted while blocking.
     */
    protected final List<CacheMigrationEntry> addBatchEntry(
        String destCache,
        CacheMigrationEntry item,
        List<CacheMigrationEntry> batch
    ) throws InterruptedException {
        batch.add(item);

        if (batch.size() == cfg.batchSize()) {
            batches.put(new Batch(destCache, batch));

            return new ArrayList<>(cfg.batchSize());
        }

        return batch;
    }

    /**
     * Publishes a final partial batch.
     *
     * @param destCache Destination cache.
     * @param batch Batch buffer.
     * @throws InterruptedException If the writer queue is full and the reader is interrupted while blocking.
     */
    protected final void flushBatch(String destCache, List<CacheMigrationEntry> batch) throws InterruptedException {
        if (!batch.isEmpty())
            batches.put(new Batch(destCache, batch));
    }

    /**
     * Flushes the DataStreamer buffers once every writer thread has finished.
     *
     * @throws IgniteException If a buffer cannot be flushed.
     */
    private void flushStreamers() {
        IgniteException error = null;

        for (IgniteDataStreamer<Object, Object> streamer : streamers.values()) {
            try {
                streamer.close();
            }
            catch (RuntimeException e) {
                if (error == null)
                    error = new IgniteException("Failed to flush DataStreamer migration buffers.", e);
                else
                    error.addSuppressed(e);
            }
        }

        streamers.clear();

        if (error != null)
            throw error;
    }

    /** Discards unflushed DataStreamer buffers after a failed migration. */
    private void discardStreamers() {
        for (IgniteDataStreamer<Object, Object> streamer : streamers.values()) {
            try {
                streamer.close(true);
            }
            catch (RuntimeException e) {
                log.warning("Failed to cancel DataStreamer after migration failure.", e);
            }
        }

        streamers.clear();
    }

    /** Writes a batch to a thick destination with internal, conflict- and expiry-aware operations. */
    private void writeThickBatch(IgniteEx ig, String name, List<CacheMigrationEntry> entries) {
        IgniteInternalCache<Object, Object> internal = ig.cachex(name).keepBinary().withSkipStore();
        GridCacheContext<Object, Object> ctx = internal.context();
        boolean keepVer = !cfg.skipConflictVersion();

        if (cfg.useDataStreamer()) {
            if (entries.stream().anyMatch(e -> keepVer && e.version() != null ||
                !cfg.skipExpireTime() && e.expireTime() != null)) {
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

            for (CacheMigrationEntry entry : entries)
                streamer.addData(destinationKey(entry.key()), destinationValue(ctx, entry));

            return;
        }

        if (keepVer && !ctx.conflictNeedResolve()) {
            throw new IgniteException("Conflict resolution must be configured on the destination servers " +
                "and client node to preserve conflict versions [cache=" + name +
                "]. Use --skip-conflict-version to ignore source versions.");
        }

        if (keepVer || !cfg.skipExpireTime() && entries.stream().anyMatch(e -> e.expireTime() != null)) {
            Map<KeyCacheObject, GridCacheDrInfo> batch = new LinkedHashMap<>();

            for (CacheMigrationEntry entry : entries) {
                long expire = cfg.skipExpireTime() || entry.expireTime() == null ? 0 : entry.expireTime();

                if (expire > 0 && expire <= System.currentTimeMillis())
                    continue;

                GridCacheVersion ver = keepVer ? entry.version() :
                    ctx.versions().next(ctx.affinity().affinityTopologyVersion().topologyVersion());

                if (ver == null)
                    throw new IgniteException("Missing source conflict version [cache=" + name + "].");

                ver = ver.conflictVersion();

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

            for (CacheMigrationEntry entry : entries)
                batch.put(destinationKey(entry.key()), destinationValue(ctx, entry));

            if (!cfg.skipExpireTime())
                internal = internal.withExpiryPolicy(new ModifiedExpiryPolicy(Duration.ETERNAL));

            try {
                if (batch.size() == 1) {
                    Map.Entry<Object, Object> entry = batch.entrySet().iterator().next();

                    internal.put(entry.getKey(), entry.getValue());
                }
                else if (!batch.isEmpty())
                    internal.putAll(batch);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to write batch [cache=" + name + "].", e);
            }
        }
    }

    /** Writes a batch to a thin-client destination with the public client cache APIs. */
    @SuppressWarnings("unchecked")
    private void writeThinBatch(IgniteClient client, String name, List<CacheMigrationEntry> entries) {
        ClientCache<Object, Object> cache = client.<Object, Object>cache(name).withKeepBinary();
        boolean keepVer = !cfg.skipConflictVersion();
        boolean hasVersions = entries.stream().anyMatch(e -> e.version() != null);
        long now = System.currentTimeMillis();

        if (keepVer && hasVersions) {
            if (entries.stream().anyMatch(e -> e.version() == null))
                throw new IgniteException("Thin conflict-aware batch contains entries without versions [cache=" +
                    name + "].");

            if (thinConflictWarned.add(name))
                log.warning("Preserving source conflict versions through a thin destination uses Ignite's " +
                    "internal conflict-aware client operation; the destination cluster must have conflict " +
                    "resolution enabled [cache=" + name + "].");

            Map<Object, T3<Object, GridCacheVersion, Long>> conflictBatch = new LinkedHashMap<>();

            for (CacheMigrationEntry entry : entries) {
                long expire = cfg.skipExpireTime() || entry.expireTime() == null ? 0 : entry.expireTime();

                if (expire > 0 && expire <= now)
                    continue;

                conflictBatch.put(entry.key(), new T3<>(entry.value(), entry.version().conflictVersion(), expire));
            }

            if (!conflictBatch.isEmpty())
                ((TcpClientCache<Object, Object>)cache).putAllConflict(conflictBatch);

            return;
        }

        boolean hasExpiry = !cfg.skipExpireTime() && entries.stream()
            .anyMatch(e -> e.expireTime() != null && e.expireTime() > now);

        if (!hasExpiry) {
            Map<Object, Object> values = new LinkedHashMap<>();

            for (CacheMigrationEntry entry : entries)
                values.put(entry.key(), entry.value());

            if (!values.isEmpty())
                cache.putAll(values);

            return;
        }

        // Absolute source expiry is converted to a remaining TTL immediately before the operation.
        for (CacheMigrationEntry entry : entries) {
            long expire = entry.expireTime() == null ? 0 : entry.expireTime();

            if (expire > 0 && expire <= now)
                continue;

            Duration ttl = expire == 0 ? Duration.ETERNAL :
                new Duration(TimeUnit.MILLISECONDS, expire - now);

            cache.withExpirePolicy(new ModifiedExpiryPolicy(ttl)).put(entry.key(), entry.value());
        }
    }

    /** Discards any source-side cached affinity partition. */
    private static Object destinationKey(Object key) {
        return key instanceof KeyCacheObject ? ((KeyCacheObject)key).copy(-1) : key;
    }

    /** */
    private static Object destinationValue(GridCacheContext<?, ?> ctx, CacheMigrationEntry entry) {
        return ctx.dataStructuresCache() ? dataStructureValue(ctx, entry.value()) : entry.value();
    }

    /** Rebase cluster timestamps without transferring source node/thread ownership. */
    private static Object dataStructureValue(GridCacheContext<?, ?> ctx, Object val) {
        long gridStartTime = ctx.discovery().gridStartTime();

        if (val instanceof GridCacheCountDownLatchValue latch) {
            GridCacheCountDownLatchValue res = new GridCacheCountDownLatchValue(latch.initialCount(),
                latch.autoDelete(), gridStartTime);

            res.set(latch.get());

            return res;
        }

        if (val instanceof GridCacheSemaphoreState sem) {
            if (sem.getWaiters() != null && sem.getWaiters().values().stream().anyMatch(cnt -> cnt != 0))
                throw new IgniteException("Cannot migrate a semaphore with acquired permits.");

            GridCacheSemaphoreState res = new GridCacheSemaphoreState(sem.getCount(), new HashMap<>(),
                sem.isFailoverSafe(), gridStartTime);

            res.setBroken(sem.isBroken());

            return res;
        }

        if (val instanceof GridCacheLockState lock) {
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

    /** A batch of entries bound to a single destination cache; exclusively owned by a writer after publication. */
    private static final class Batch {
        /** Destination cache name. */
        private final String cache;

        /** Batch entries. */
        private final List<CacheMigrationEntry> entries;

        /**
         * @param cache Destination cache.
         * @param entries Batch entries.
         */
        Batch(String cache, List<CacheMigrationEntry> entries) {
            this.cache = cache;
            this.entries = entries;
        }
    }
}
