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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCache;
import org.apache.ignite.util.migration.category.type.MigrationTypeMapping;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/** Internal mixed-client data-transfer strategy used by {@link CacheDataTransferrer}. */
final class ThinClientCacheDataTransferrer {
    /** Source connection. */
    private final Object from;

    /** Destination connection. */
    private final Object to;

    /** Destination endpoint for category-aware metadata and mapping registration. */
    private final MigrationDestination toEndpoint;

    /** Logger. */
    private final IgniteLogger log;

    /** Data transfer settings. */
    private final CacheDataMigrationConfiguration cfg;

    /** Cache selection and name mapping. */
    private final List<MigrationCache> caches;

    /** Thick cache transferrer supplying the primary-partition scan implementation. */
    private final CacheDataTransferrer thickProvider;

    /** Whether binary metadata creation is disabled. */
    private final boolean noBinaryMetadataCreate;

    /** Whether type mapping registration is disabled. */
    private final boolean noMappingsCreate;

    /** Bounded queue of writer batches. */
    private final BlockingQueue<Batch> batches;

    /** Active scan cursors. */
    private final java.util.Set<QueryCursor<?>> cursors = ConcurrentHashMap.newKeySet();

    /** Destination streamers; closed by the transferrer after all writers finish. */
    private final Map<String, IgniteDataStreamer<Object, Object>> streamers = new ConcurrentHashMap<>();

    /** Binary metadata already registered at the destination, by type ID. */
    private final Set<Integer> registeredMetadata = ConcurrentHashMap.newKeySet();

    /** Per-type registration locks. */
    private final Map<Integer, Object> metadataLocks = new ConcurrentHashMap<>();

    /** Remaining reader workers. */
    private final AtomicInteger readers;

    /** Completed cache/partition scans. */
    private final AtomicInteger completedTasks = new AtomicInteger();

    /** Read entry count. */
    private final AtomicLong read = new AtomicLong();

    /** Written entry count. */
    private final AtomicLong written = new AtomicLong();

    /** Global read limiter. */
    private final BasicRateLimiter readLimiter;

    /** Global write limiter. */
    private final BasicRateLimiter writeLimiter;

    /**
     * @param from Source.
     * @param to Destination.
     * @param toEndpoint Destination endpoint.
     * @param log Logger.
     * @param cfg Data transfer settings.
     * @param caches Selected and transformed cache definitions.
     * @param thickProvider Existing cache-data provider.
     * @param noBinaryMetadataCreate Whether binary metadata creation is disabled.
     * @param noMappingsCreate Whether mapping registration is disabled.
     */
    ThinClientCacheDataTransferrer(
        Object from,
        Object to,
        MigrationDestination toEndpoint,
        IgniteLogger log,
        CacheDataMigrationConfiguration cfg,
        List<MigrationCache> caches,
        CacheDataTransferrer thickProvider,
        boolean noBinaryMetadataCreate,
        boolean noMappingsCreate
    ) {
        this.from = from;
        this.to = to;
        this.toEndpoint = toEndpoint;
        this.log = log;
        this.cfg = cfg;
        this.caches = caches;
        this.thickProvider = thickProvider;
        this.noBinaryMetadataCreate = noBinaryMetadataCreate;
        this.noMappingsCreate = noMappingsCreate;
        batches = new ArrayBlockingQueue<>(cfg.queueCapacity());
        readers = new AtomicInteger();
        readLimiter = new BasicRateLimiter(cfg.readLimitInSec());
        writeLimiter = new BasicRateLimiter(cfg.writeLimitInSec());
    }

    /** Transfers cache records using the public cache APIs of thick or thin clients. */
    void transfer(@Nullable MigrationTransformer<MigrationEntry> transformer) {
        Map<String, String> names = new LinkedHashMap<>(caches.size());

        for (MigrationCache c : caches)
            names.put(c.sourceName(), c.configuration().getName());

        Object src = from;
        Object dest = to;

        if (caches.isEmpty()) {
            info("No caches selected for thin-client data transfer.");

            return;
        }

        if (dest instanceof IgniteClient && cfg.useDataStreamer())
            throw new IgniteException("DataStreamer is not available through the thin-client API.");

        if (src instanceof IgniteClient)
            warn("Thin source scans do not expose entry versions or expiry timestamps; source entry metadata " +
                "cannot be preserved.");
        else if (dest instanceof IgniteClient && !cfg.skipConflictVersion()) {
            warn("Preserving source conflict versions through a thin destination uses Ignite's internal " +
                "conflict-aware client operation; the destination cluster must have conflict resolution enabled.");
        }

        ConcurrentLinkedQueue<ReadTask> pending = new ConcurrentLinkedQueue<>();

        if (src instanceof IgniteEx ig) {
            for (Map.Entry<String, Integer> part : CacheDataTransferrer.partitions(
                ig, names.keySet()))
                pending.add(new ReadTask(part.getKey(), names.get(part.getKey()), part.getValue()));
        }
        else {
            for (MigrationCache cache : caches)
                pending.add(new ReadTask(cache.sourceName(), names.get(cache.sourceName()), null));
        }

        int totalTasks = pending.size();
        CacheDataMigrationThreadPools workers = new CacheDataMigrationThreadPools(cfg);
        int readWorkers = Math.min(workers.readerThreads(), pending.size());
        readers.set(readWorkers);

        info("Thin cache data worker pools [readers=" + readWorkers + ", readerPool=dedicated, writers=" +
            workers.writerThreads() + ", writerPool=dedicated].");

        BlockingQueue<Future<Void>> completed = new LinkedBlockingQueue<>();
        ExecutorCompletionService<Void> readerCompletion =
            new ExecutorCompletionService<>(workers.readerPool(), completed);
        ExecutorCompletionService<Void> writerCompletion =
            new ExecutorCompletionService<>(workers.writerPool(), completed);
        ScheduledExecutorService reporter = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
            new IgniteThreadFactory(null, "thin-migration-log"));
        int tasksCnt = readWorkers + workers.writerThreads();
        boolean success = false;

        try {
            reporter.scheduleAtFixedRate(this::logProgress, 1, 1, TimeUnit.MINUTES);

            for (int i = 0; i < workers.writerThreads(); i++)
                writerCompletion.submit(this::write);

            for (int i = 0; i < readWorkers; i++)
                readerCompletion.submit(() -> read(pending, transformer, totalTasks));

            for (int i = 0; i < tasksCnt; i++)
                completed.take().get();

            success = true;
            logProgress();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException("Thin-client migration interrupted.", e);
        }
        catch (ExecutionException e) {
            throw new IgniteException("Thin-client migration worker failed.", e.getCause());
        }
        finally {
            reporter.shutdownNow();

            cursors.forEach(this::close);

            closeStreamers(success);

            workers.stop(success);
        }
    }

    /** Reads assigned caches into the bounded writer queue. */
    private Void read(
        ConcurrentLinkedQueue<ReadTask> pending,
        @Nullable MigrationTransformer<MigrationEntry> transformer,
        int totalTasks
    ) throws IgniteCheckedException, InterruptedException {
        try {
            ReadTask task;

            while ((task = pending.poll()) != null) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                readTask(task, transformer, totalTasks);
            }

            return null;
        }
        finally {
            readers.decrementAndGet();
        }
    }

    /** Scans one cache (or source partition) through the best available endpoint API. */
    private void readTask(ReadTask task,
        @Nullable MigrationTransformer<MigrationEntry> transformer,
        int totalTasks
    ) throws IgniteCheckedException, InterruptedException {
        long start = System.nanoTime();
        long cnt = 0;
        List<MigrationEntry> batch = new ArrayList<>(cfg.batchSize());
        QueryCursor<?> cursor = task.partition == null ? sourceCursor(task.sourceCache) :
            thickProvider.readCursor((IgniteEx)from, task.sourceCache, task.partition);

        cursors.add(cursor);

        info("Reading cache data [cache=" + task.sourceCache + ", partition=" + task.partition + ", source=" +
            (task.partition == null ? "thin-client-compatible ScanQuery" : "thick-client primary scan") + "].");

        try (QueryCursor<?> ignored = cursor) {
            for (Object rawEntry : cursor) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                readLimiter.acquire(1);

                MigrationEntry item;

                if (rawEntry instanceof MigrationEntry entry)
                    item = entry;
                else {
                    @SuppressWarnings("unchecked")
                    Cache.Entry<Object, Object> cacheEntry = (Cache.Entry<Object, Object>)rawEntry;

                    item = new MigrationEntry(cacheEntry.getKey(), cacheEntry.getValue(), 0, null);
                }

                if (transformer != null)
                    item = transformer.transform(item);

                if (item == null)
                    continue;

                registerBinaryMetadata(item);

                batch.add(item);
                read.incrementAndGet();
                cnt++;

                if (batch.size() == cfg.batchSize()) {
                    batches.put(new Batch(task.destinationCache, batch));
                    batch = new ArrayList<>(cfg.batchSize());
                }
            }
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException("Failed to scan source cache [cache=" + task.sourceCache +
                ", partition=" + task.partition + "].", e);
        }
        finally {
            cursors.remove(cursor);
        }

        if (!batch.isEmpty())
            batches.put(new Batch(task.destinationCache, batch));

        int completed = completedTasks.incrementAndGet();
        String progress = task.partition == null ? "completedCacheScans=" + completed + ", totalCacheScans=" + totalTasks :
            "completedPartitions=" + completed + ", totalPartitions=" + totalTasks;

        info("Finished cache scan [cache=" + task.sourceCache + ", partition=" + task.partition +
            ", entries=" + cnt + ", elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) +
            ", " + progress + "].");
    }

    /** Registers binary metadata encountered in the current entry on the destination before writing it. */
    private void registerBinaryMetadata(MigrationEntry entry) throws IgniteCheckedException {
        Set<Object> visited = java.util.Collections.newSetFromMap(new IdentityHashMap<>());

        registerBinaryValue(entry.key(), visited);
        registerBinaryValue(entry.value(), visited);
    }

    /** Registers metadata recursively for a binary object and any nested objects in its fields. */
    private void registerBinaryValue(Object val, Set<Object> visited) throws IgniteCheckedException {
        if (val == null || !visited.add(val))
            return;

        if (val instanceof BinaryObject binary) {
            BinaryType type = binary instanceof BinaryObjectEx ? ((BinaryObjectEx)binary).rawType() : binary.type();

            if (type == null)
                return;

            if (type instanceof BinaryTypeImpl impl)
                registerBinaryType(impl);

            for (String field : type.fieldNames())
                registerBinaryValue(binary.field(field), visited);
        }
        else if (val instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                registerBinaryValue(entry.getKey(), visited);
                registerBinaryValue(entry.getValue(), visited);
            }
        }
        else if (val instanceof Iterable<?> iterable) {
            for (Object item : iterable)
                registerBinaryValue(item, visited);
        }
        else if (val.getClass().isArray() && !val.getClass().getComponentType().isPrimitive()) {
            for (int i = 0; i < Array.getLength(val); i++)
                registerBinaryValue(Array.get(val, i), visited);
        }
    }

    /** Registers one type's binary schema and any Java class mapping available from the source. */
    private void registerBinaryType(BinaryTypeImpl type) throws IgniteCheckedException {
        BinaryMetadata metadata = type.metadata();
        int typeId = metadata.typeId();
        Object mux = metadataLocks.computeIfAbsent(typeId, id -> new Object());

        synchronized (mux) {
            if (registeredMetadata.contains(typeId))
                return;

            try {
                Object dest = to;

                if (!noBinaryMetadataCreate) {
                    toEndpoint.accept(MigrationCategory.BINARY_METADATA, metadata);

                    info("Transferred binary metadata [name='" + metadata.typeName() + "', typeId=" + typeId +
                        ", destination=" + dest.getClass().getSimpleName() + "].");
                }

                MigrationTypeMapping mapping = noMappingsCreate ? null : sourceTypeMapping(typeId, type.typeName());

                if (mapping != null)
                    registerTypeMapping(mapping);

                registeredMetadata.add(typeId);
            }
            catch (RuntimeException e) {
                throw new IgniteCheckedException("Failed to register thin-path binary metadata [typeName=" +
                    type.typeName() + ", typeId=" + typeId + "].", e);
            }
        }
    }

    /** Finds a source type mapping when thick-client mapping data or a local Java class is available. */
    private MigrationTypeMapping sourceTypeMapping(int typeId, String typeName) {
        Object src = from;

        if (src instanceof IgniteEx ig) {
            List<Map<Integer, MappedName>> mappings = ig.context().marshallerContext().getCachedMappings();

            for (int platform = 0; platform < mappings.size(); platform++) {
                MappedName name = mappings.get(platform).get(typeId);

                if (name != null)
                    return new MigrationTypeMapping((byte)platform, typeId, name.className());
            }
        }
        else {
            try {
                Class.forName(typeName, false, Thread.currentThread().getContextClassLoader());

                return new MigrationTypeMapping(JAVA_ID, typeId, typeName);
            }
            catch (ClassNotFoundException ignored) {
                // BinaryObject data can still be copied without a Java class mapping.
            }
        }

        return null;
    }

    /** Registers a source mapping through the destination receiver contract. */
    private void registerTypeMapping(MigrationTypeMapping mapping) throws IgniteCheckedException {
        try {
            if (to instanceof IgniteClient && mapping.platformId() != JAVA_ID) {
                log.warning("Thin client cannot register a non-Java platform mapping; leaving it unmapped " +
                    "[platformId=" + mapping.platformId() + ", typeName=" + mapping.typeName() + "].");

                return;
            }

            toEndpoint.accept(MigrationCategory.TYPE_MAPPING, mapping);

            info("Transferred type mapping [name='" + mapping.typeName() + "', typeId=" + mapping.typeId() +
                ", platformId=" + mapping.platformId() + ", destination=" +
                to.getClass().getSimpleName() + "].");
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to register type mapping in thin-client cache transfer " +
                "[typeName=" + mapping.typeName() + ", typeId=" + mapping.typeId() + "].", e);
        }
    }

    /** Opens a source cursor through IgniteCache or ClientCache. */
    private QueryCursor<?> sourceCursor(String name) throws IgniteCheckedException {
        Object src = from;
        ScanQuery<Object, Object> qry = new ScanQuery<>().setPageSize(cfg.batchSize());

        try {
            if (src instanceof IgniteEx ig)
                return ig.<Object, Object>cache(name).withKeepBinary().query(qry);

            if (src instanceof IgniteClient client)
                return client.<Object, Object>cache(name).withKeepBinary().query(qry);
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to open source cache cursor [cache=" + name + "].", e);
        }

        throw new IgniteCheckedException("Unsupported thin-client migration source: " + src.getClass().getName());
    }

    /** Consumes writer batches. Streamers remain open until all workers have stopped. */
    private Void write() throws IgniteCheckedException, InterruptedException {
        while (readers.get() != 0 || !batches.isEmpty()) {
            Batch batch = batches.poll(100, TimeUnit.MILLISECONDS);

            if (batch != null) {
                writeLimiter.acquire(batch.entries.size());
                writeBatch(batch.cache, batch.entries);
                written.addAndGet(batch.entries.size());
            }
        }

        return null;
    }

    /** Writes a batch with APIs exposed by the destination endpoint. */
    private void writeBatch(String name, List<MigrationEntry> entries) throws IgniteCheckedException {
        Object dest = to;

        try {
            Map<Object, Object> values = new LinkedHashMap<>();

            for (MigrationEntry entry : entries)
                values.put(entry.key(), entry.value());

            if (dest instanceof IgniteClient client) {
                writeThinClientBatch(client, name, entries, values);

                return;
            }

            if (!(dest instanceof IgniteEx))
                throw new IgniteCheckedException("Unsupported thin-client migration destination: " +
                    dest.getClass().getName());

            IgniteEx ig = (IgniteEx)dest;

            if (cfg.useDataStreamer()) {
                if (entries.stream().anyMatch(entry -> !cfg.skipConflictVersion() && entry.version() != null ||
                    !cfg.skipExpireTime() && entry.expireTime() != 0)) {
                    throw new IgniteCheckedException("DataStreamer cannot preserve entry metadata. " +
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

                for (Map.Entry<Object, Object> entry : values.entrySet())
                    streamer.addData(entry.getKey(), entry.getValue());
            }
            else {
                IgniteCache<Object, Object> cache = ig.<Object, Object>cache(name).withKeepBinary().withSkipStore();

                if (values.size() == 1) {
                    Map.Entry<Object, Object> entry = values.entrySet().iterator().next();

                    cache.put(entry.getKey(), entry.getValue());
                }
                else if (!values.isEmpty())
                    cache.putAll(values);
            }
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to write thin-client migration batch [cache=" + name + "].", e);
        }
    }

    /** Uses the thin client's conflict-aware protocol when the source supplied row metadata. */
    @SuppressWarnings("unchecked")
    private void writeThinClientBatch(
        IgniteClient client,
        String name,
        List<MigrationEntry> entries,
        Map<Object, Object> values
    ) throws IgniteCheckedException {
        ClientCache<Object, Object> cache = client.<Object, Object>cache(name).withKeepBinary();
        boolean hasSrcVersions = entries.stream().anyMatch(entry -> entry.version() != null);

        if (!cfg.skipConflictVersion() && hasSrcVersions) {
            if (entries.stream().anyMatch(entry -> entry.version() == null))
                throw new IgniteCheckedException("Thin conflict-aware batch contains entries without versions [cache=" +
                    name + "].");

            Map<Object, T3<Object, GridCacheVersion, Long>> conflictBatch = new LinkedHashMap<>();

            for (MigrationEntry entry : entries) {
                long expireTime = cfg.skipExpireTime() ? 0 : entry.expireTime();

                if (expireTime > 0 && expireTime <= System.currentTimeMillis())
                    continue;

                conflictBatch.put(entry.key(), new T3<>(entry.value(), entry.version().conflictVersion(), expireTime));
            }

            if (!conflictBatch.isEmpty())
                ((TcpClientCache<Object, Object>)cache).putAllConflict(conflictBatch);

            return;
        }

        // A thin source does not expose expiry metadata. Keep the old public putAll path in this case.
        if (!(from instanceof IgniteEx) || cfg.skipExpireTime()) {
            if (!values.isEmpty())
                cache.putAll(values);

            return;
        }

        // When only source versions are skipped, public thin writes can still retain an absolute source expiry by
        // converting it to a remaining TTL immediately before the operation.
        for (MigrationEntry entry : entries) {
            long expireTime = entry.expireTime();

            if (expireTime > 0 && expireTime <= System.currentTimeMillis())
                continue;

            Duration ttl = expireTime == 0 ? Duration.ETERNAL :
                new Duration(TimeUnit.MILLISECONDS, expireTime - System.currentTimeMillis());

            cache.withExpirePolicy(new ModifiedExpiryPolicy(ttl)).put(entry.key(), entry.value());
        }
    }

    /** Closes all active streamers after writer completion. */
    private void closeStreamers(boolean success) {
        for (IgniteDataStreamer<Object, Object> streamer : streamers.values()) {
            try {
                if (success)
                    streamer.close();
                else
                    streamer.close(true);
            } catch (Exception e) {
                log.warning("Error while closing streamer [" + streamer + "]", e);
            }
        }

        streamers.clear();
    }

    /** Closes a cursor quietly while the transferrer is unwinding. */
    private void close(QueryCursor<?> cursor) {
        try {
            cursor.close();
        }
        catch (RuntimeException e) {
            log.warning("Failed to close thin-client migration cursor.", e);
        }
    }

    /** Logs read/write totals. */
    private void logProgress() {
        info("Thin-client migration records [read=" + read.get() + ", written=" + written.get() + "].");
    }

    /** Logs an informational message. */
    private void info(String msg) {
        if (log.isInfoEnabled())
            log.info(msg);
    }

    /** Logs a warning. */
    private void warn(String msg) {
        log.warning(msg);
    }

    /** Source cache scan task; a {@code null} partition selects a thin-client whole-cache scan. */
    private static class ReadTask {
        /** Source cache name. */
        private final String sourceCache;

        /** Destination cache name. */
        private final String destinationCache;

        /** Source partition, or {@code null} for whole-cache scan. */
        private final Integer partition;

        /**
         * @param sourceCache Source cache.
         * @param destinationCache Destination cache.
         * @param partition Partition, or {@code null} for whole-cache scan.
         */
        private ReadTask(String sourceCache, String destinationCache, Integer partition) {
            this.sourceCache = sourceCache;
            this.destinationCache = destinationCache;
            this.partition = partition;
        }
    }

    /** Writer queue item. */
    private static class Batch {
        /** Destination cache name. */
        private final String cache;

        /** Entries. */
        private final List<MigrationEntry> entries;

        /**
         * @param cache Destination cache.
         * @param entries Entries.
         */
        private Batch(String cache, List<MigrationEntry> entries) {
            this.cache = cache;
            this.entries = entries;
        }
    }
}
