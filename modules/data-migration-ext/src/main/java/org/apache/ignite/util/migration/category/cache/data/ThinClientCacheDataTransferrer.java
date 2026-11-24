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
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.type.TypeMappingMigrationEntity;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/** Transfers cache records scanned through the thin-client public cache API. */
public class ThinClientCacheDataTransferrer extends AbstractCacheDataTransferrer<IgniteClient> {
    /** Whether binary metadata creation is disabled. */
    private final boolean skipBinaryMeta;

    /** Whether type mapping registration is disabled. */
    private final boolean skipTypeMappings;

    /** Binary type IDs already known at the destination, to be skipped on registration. */
    private final Set<Integer> registeredTypeIds = ConcurrentHashMap.newKeySet();

    /** Per-type registration locks. */
    private final Map<Integer, Object> metadataLocks = new ConcurrentHashMap<>();

    /** Completed cache scans. */
    private final AtomicInteger completedTasks = new AtomicInteger();

    /**
     * @param src Connected thin source.
     * @param log Logger.
     * @param cfg Data transfer settings.
     * @param cacheNameMap Source cache name to destination cache name.
     * @param skipBinaryMeta Whether binary metadata creation is disabled.
     * @param skipTypeMappings Whether mapping registration is disabled.
     * @param alreadyTransferredTypeIds Binary type IDs already registered at the destination by prior channels.
     */
    public ThinClientCacheDataTransferrer(
        IgniteClient src,
        IgniteLogger log,
        CacheDataMigrationConfiguration cfg,
        Map<String, String> cacheNameMap,
        boolean skipBinaryMeta,
        boolean skipTypeMappings,
        Collection<Integer> alreadyTransferredTypeIds
    ) {
        super(src, log, cfg, cacheNameMap);

        this.skipBinaryMeta = skipBinaryMeta;
        this.skipTypeMappings = skipTypeMappings;
        this.registeredTypeIds.addAll(alreadyTransferredTypeIds);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTransfer(
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    ) {
        warn("Thin client source scans do not expose entry versions or expiry timestamps; source entry metadata " +
            "cannot be preserved.");
    }

    /** {@inheritDoc} */
    @Override protected int readWorkerCount(int readerThreads) {
        return Math.min(readerThreads, cacheNameMap.size());
    }

    /** {@inheritDoc} */
    @Override protected void submitReadTasks(
        ExecutorCompletionService<Void> exec,
        int readWorkers,
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    ) {
        List<String> caches = new ArrayList<>(cacheNameMap.keySet());

        for (int i = 0; i < readWorkers; i++)
            exec.submit(() -> read(caches, dest, transformer));
    }

    /** Reads assigned caches into the bounded writer queue. */
    private Void read(
        List<String> caches,
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    ) throws InterruptedException {
        try {
            for (String cache : caches) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                readCache(cache, caches.size(), dest, transformer);
            }

            return null;
        }
        finally {
            readers.decrementAndGet();
        }
    }

    /** Scans one source cache through the thin-client ScanQuery. */
    private void readCache(
        String srcCache,
        int totalCaches,
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<CacheMigrationEntry> transformer
    ) throws InterruptedException {
        long start = System.nanoTime();

        List<CacheMigrationEntry> batch = new ArrayList<>(cfg.batchSize());

        String destCache = cacheNameMap.get(srcCache);

        info("Reading cache data [cache=" + srcCache + "].");

        ScanQuery<Object, Object> qry = new ScanQuery<>().setPageSize(cfg.batchSize());

        QueryCursor<?> cursor = src.cache(srcCache).withKeepBinary().query(qry);

        cursors.add(cursor);

        long curCnt = 0;

        try (QueryCursor<?> ignored = cursor) {
            for (Object rawEntry : cursor) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException();

                @SuppressWarnings("unchecked")
                Cache.Entry<Object, Object> cacheEntry = (Cache.Entry<Object, Object>)rawEntry;

                acquireReadPermit();

                CacheMigrationEntry item = new CacheMigrationEntry(cacheEntry.getKey(), cacheEntry.getValue(), null, null);

                read.incrementAndGet();
                curCnt++;

                if (transformer != null)
                    item = transformer.transform(item);

                if (item == null)
                    continue;

                registerBinaryMetadata(dest, item);

                batch = addBatchEntry(destCache, item, batch);
            }
        }
        catch (IgniteException e) {
            throw new IgniteException("Failed to scan source cache [cache=" + srcCache + "].", e);
        }
        finally {
            cursors.remove(cursor);
        }

        flushBatch(destCache, batch);

        info("Finished cache scan [cache=" + srcCache + ", entries=" + curCnt +
            ", elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) +
            ", completed=" + completedTasks.incrementAndGet() + ", total=" + totalCaches + "].");
    }

    /** Registers binary metadata encountered in the current entry on the destination before writing it. */
    private void registerBinaryMetadata(MigrationDestination<?> dest, CacheMigrationEntry entry) {
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());

        registerBinaryValue(dest, entry.key(), visited);
        registerBinaryValue(dest, entry.value(), visited);
    }

    /** Registers metadata recursively for a binary object and any nested objects in its fields. */
    private void registerBinaryValue(MigrationDestination<?> dest, Object val, Set<Object> visited)
        {
        if (val == null || !visited.add(val))
            return;

        if (val instanceof BinaryObject binary) {
            BinaryType type = binary instanceof BinaryObjectEx ? ((BinaryObjectEx)binary).rawType() : binary.type();

            if (type == null)
                return;

            if (type instanceof BinaryTypeImpl impl)
                registerBinaryType(dest, impl);

            for (String field : type.fieldNames())
                registerBinaryValue(dest, binary.field(field), visited);
        }
        else if (val instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                registerBinaryValue(dest, entry.getKey(), visited);
                registerBinaryValue(dest, entry.getValue(), visited);
            }
        }
        else if (val instanceof Iterable<?> iterable) {
            for (Object item : iterable)
                registerBinaryValue(dest, item, visited);
        }
        else if (val.getClass().isArray() && !val.getClass().getComponentType().isPrimitive()) {
            for (int i = 0; i < Array.getLength(val); i++)
                registerBinaryValue(dest, Array.get(val, i), visited);
        }
    }

    /** Registers one type's binary schema and any Java class mapping available to the migration process. */
    private void registerBinaryType(MigrationDestination<?> dest, BinaryTypeImpl type) {
        BinaryMetadata metadata = type.metadata();
        int typeId = metadata.typeId();
        Object mux = metadataLocks.computeIfAbsent(typeId, id -> new Object());

        synchronized (mux) {
            if (registeredTypeIds.contains(typeId))
                return;

            if (!skipBinaryMeta) {
                dest.accept(MigrationCategory.BINARY_METADATA, metadata);

                info("Transferred binary metadata [name='" + metadata.typeName() + "', typeId=" + typeId +
                    ", destination=" + dest.origin().getClass().getSimpleName() + "].");
            }

            TypeMappingMigrationEntity mapping = skipTypeMappings ? null : sourceTypeMapping(typeId, type.typeName());

            if (mapping != null)
                registerTypeMapping(dest, mapping);

            registeredTypeIds.add(typeId);
        }
    }

    /** Finds a Java class mapping available to the migration process. */
    private TypeMappingMigrationEntity sourceTypeMapping(int typeId, String typeName) {
        try {
            Class.forName(typeName, false, Thread.currentThread().getContextClassLoader());

            return new TypeMappingMigrationEntity(JAVA_ID, typeId, typeName);
        }
        catch (ClassNotFoundException ignored) {
            // BinaryObject data can still be copied without a Java class mapping.
        }

        return null;
    }

    /** Registers a source mapping through the destination receiver contract. */
    private void registerTypeMapping(MigrationDestination<?> dest, TypeMappingMigrationEntity mapping) {
        if (dest.origin() instanceof IgniteClient && mapping.platformId() != JAVA_ID) {
            log.warning("Thin client cannot register a non-Java platform mapping; leaving it unmapped " +
                "[platformId=" + mapping.platformId() + ", typeName=" + mapping.typeName() + "].");

            return;
        }

        dest.accept(MigrationCategory.TYPE_MAPPING, mapping);

        info("Transferred type mapping [name='" + mapping.typeName() + "', typeId=" + mapping.typeId() +
            ", platformId=" + mapping.platformId() + ", destination=" +
            dest.origin().getClass().getSimpleName() + "].");
    }
}
