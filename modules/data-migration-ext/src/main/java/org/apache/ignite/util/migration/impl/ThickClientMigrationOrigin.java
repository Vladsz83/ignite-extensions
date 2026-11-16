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

package org.apache.ignite.util.migration.impl;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationCache;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationEntry;
import org.apache.ignite.util.migration.api.MigrationPartition;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.api.MigrationTypeMapping;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;

/** Client-node connection, usable as either endpoint. */
public class ThickClientMigrationOrigin implements MigrationSource, MigrationDestination {
    /** */
    private static final String SQL_VIEW_KEY_PREFIX = "ignite.internal.sql.view.";

    /** */
    private final IgniteConfiguration cfg;

    /** */
    private IgniteEx ig;

    /** */
    private boolean closed;

    /** */
    private final Map<String, IgniteDataStreamer<Object, Object>> streamers = new ConcurrentHashMap<>();

    /** */
    private ThickClientMigrationOrigin(IgniteConfiguration cfg) {
        this.cfg = new IgniteConfiguration(cfg);
    }

    /** {@inheritDoc} */
    @Override public synchronized void connect() throws IgniteCheckedException {
        if (closed)
            throw new IgniteCheckedException("Migration client node is already closed");

        if (ig != null)
            throw new IgniteCheckedException("Migration client node is already connected");

        try {
            ig = (IgniteEx)Ignition.start(cfg);

            if (!ig.cluster().state().active())
                throw new IgniteCheckedException("Migration requires an active cluster");
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to start migration client node", e);
        }
    }

    /**
     * @param cfg Client node configuration.
     * @return Migration endpoint.
     */
    public static ThickClientMigrationOrigin create(IgniteConfiguration cfg) throws IgniteCheckedException {
        if (!Boolean.TRUE.equals(cfg.isClientMode()))
            throw new IgniteCheckedException("Migration requires a client-mode IgniteConfiguration");

        return new ThickClientMigrationOrigin(cfg);
    }

    /** @return Connected cluster ID. */
    public UUID clusterId() {
        return ig.cluster().id();
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryMetadata> binaryMetadata() {
        return ((CacheObjectBinaryProcessorImpl)ig.context().cacheObjects()).binaryMetadata();
    }

    /** {@inheritDoc} */
    @Override public Collection<MigrationTypeMapping> typeMappings() {
        List<MigrationTypeMapping> res = new ArrayList<>();
        List<Map<Integer, MappedName>> mappings = ig.context().marshallerContext().getCachedMappings();

        for (int platform = 0; platform < mappings.size(); platform++) {
            for (Map.Entry<Integer, MappedName> entry : mappings.get(platform).entrySet())
                res.add(new MigrationTypeMapping((byte)platform, entry.getKey(), entry.getValue().className()));
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<MigrationCache> caches() {
        List<MigrationCache> res = new ArrayList<>();

        for (DynamicCacheDescriptor desc : ig.context().cache().cacheDescriptors().values()) {
            if (desc.cacheType() != CacheType.USER && desc.cacheType() != CacheType.DATA_STRUCTURES)
                continue;

            // The descriptor schema contains ALTER TABLE/CREATE INDEX changes absent from the original configuration.
            CacheConfiguration<?, ?> cfg = new CacheConfiguration<>(desc.cacheConfiguration());

            cfg.clearQueryEntities();
            cfg.setQueryEntities(desc.schema().entities());

            try {
                // Transformers must never mutate a live descriptor or its nested affinity/query objects.
                cfg = U.unmarshal(ig.context(), U.marshal(ig.context(), cfg), getClass().getClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to copy cache configuration: " + cfg.getName(), e);
            }

            for (QueryEntity entity : cfg.getQueryEntities()) {
                if (entity instanceof QueryEntityEx) {
                    QueryEntityEx ex = (QueryEntityEx)entity;

                    if (Integer.valueOf(-1).equals(ex.getPrimaryKeyInlineSize()))
                        ex.setPrimaryKeyInlineSize(null);

                    if (Integer.valueOf(-1).equals(ex.getAffinityKeyInlineSize()))
                        ex.setAffinityKeyInlineSize(null);
                }
            }

            res.add(new MigrationCache(cfg.getName(), cfg, desc.sql(), desc.cacheType()));
        }

        res.sort(Comparator.comparing(MigrationCache::sourceName));

        return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Serializable> metastorage(IgniteToIgniteMigrationConfiguration cfg)
        throws IgniteCheckedException {
        return metastorage("", key -> !key.startsWith(SQL_VIEW_KEY_PREFIX) && cfg.getMetastorageFilter().test(key));
    }

    /** {@inheritDoc} */
    @Override public Map<String, Serializable> sqlViews(IgniteToIgniteMigrationConfiguration cfg)
        throws IgniteCheckedException {
        return metastorage(SQL_VIEW_KEY_PREFIX, cfg.getMetastorageFilter());
    }

    /** */
    private Map<String, Serializable> metastorage(String prefix, Predicate<String> filter) throws IgniteCheckedException {
        Map<String, Serializable> res = new LinkedHashMap<>();

        ig.context().distributedMetastorage().iterate(prefix, (key, val) -> {
            if (filter.test(key))
                res.put(key, (Serializable)val);
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override public List<MigrationPartition> partitions(Map<String, String> cacheNames) {
        Map<UUID, Queue<MigrationPartition>> perNode = new LinkedHashMap<>();

        cacheNames.forEach((src, dest) -> {
            for (int part = 0; part < ig.affinity(src).partitions(); part++) {
                UUID primary = ig.affinity(src).mapPartitionToNode(part).id();

                perNode.computeIfAbsent(primary, key -> new ArrayDeque<>()).add(new MigrationPartition(src, dest, part));
            }
        });

        List<MigrationPartition> res = new ArrayList<>();
        boolean more;

        do {
            more = false;

            for (Queue<MigrationPartition> queue : perNode.values()) {
                MigrationPartition part = queue.poll();

                if (part != null) {
                    res.add(part);
                    more = true;
                }
            }
        }
        while (more);

        return res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public QueryCursor<MigrationEntry> read(MigrationPartition part, IgniteToIgniteMigrationConfiguration cfg) {
        cache(part.sourceCache());

        GridCacheContext<Object, Object> ctx = ig.cachex(part.sourceCache()).context();

        return new QueryCursorImpl<>(() -> {
            CacheOperationContext prev = ctx.gate().enter(null);

            try {
                ctx.checkSecurity(CACHE_READ);

                if (ctx.topology().lostPartitions().contains(part.partition()))
                    throw new IgniteException("Source partition is lost: " + part.partition());

                ClusterNode primary = ig.affinity(part.sourceCache()).mapPartitionToNode(part.partition());
                CacheQuery<MigrationEntry> qry = ctx.queries().createScanQuery(null,
                    new MigrationEntryReader(part.sourceCache()), part.partition(), true, false, null,
                    Collections.emptySet());

                qry.pageSize(cfg.getBatchSize());

                // Public partition ScanQuery can select a backup. Route the same scan engine to the primary explicitly.
                return ctx.kernalContext().query().executeQuery(SCAN, part.sourceCache(), ctx,
                    new IgniteOutClosureX<GridCloseableIterator<MigrationEntry>>() {
                        @Override public GridCloseableIterator<MigrationEntry> applyx() throws IgniteCheckedException {
                            return ctx.queries().scanQueryDistributed(qry, Collections.singleton(primary));
                        }
                    }, true);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to scan source partition: " + part.sourceCache(), e);
            }
            finally {
                ctx.gate().leave(prev);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void binaryMetadata(BinaryMetadata meta) {
        binaryContext().updateMetadata(meta.typeId(), meta, false);
    }

    /** {@inheritDoc} */
    @Override public void typeMapping(MigrationTypeMapping mapping) {
        binaryContext().registerUserClassName(mapping.typeId(), mapping.typeName(), false, false, mapping.platformId());
    }

    /** {@inheritDoc} */
    @Override public void cache(MigrationCache cache, IgniteToIgniteMigrationConfiguration cfg) {
        String name = cache.configuration().getName();
        DynamicCacheDescriptor desc = ig.context().cache().cacheDescriptor(name);

        if (desc != null && desc.cacheType() != cache.type())
            throw new IgniteException("Destination cache type differs from source: " + name);

        if (cfg.isNoCacheCreate()) {
            if (desc == null)
                throw new IgniteException("Destination cache does not exist: " + name);

            cache(name);

            return;
        }

        boolean created;

        try {
            created = ig.context().cache().dynamicStartCache(cache.configuration(), name, null, cache.type(),
                cache.sql(), false, true, true).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to create destination cache: " + name, e);
        }

        if (!created && !cfg.isCreateCacheIfNotExists())
            throw new CacheExistsException("Destination cache already exists: " + name);
    }

    /** {@inheritDoc} */
    @Override public void metastorage(String key, Serializable val) throws IgniteCheckedException {
        ig.context().distributedMetastorage().write(key, val);
    }

    /** {@inheritDoc} */
    @Override public void sqlView(String key, Serializable val) throws IgniteCheckedException {
        ig.context().distributedMetastorage().write(key, val);
    }

    /** {@inheritDoc} */
    @Override public void write(String name, List<MigrationEntry> entries, IgniteToIgniteMigrationConfiguration cfg)
        throws IgniteCheckedException {
        IgniteCache<Object, Object> cache = cache(name).withKeepBinary().withSkipStore();
        IgniteInternalCache<Object, Object> internal = ig.cachex(name).keepBinary().withSkipStore();
        GridCacheContext<Object, Object> ctx = internal.context();
        boolean keepVer = !cfg.isSkipConflictVersion();

        if (cfg.isUseDataStreamer()) {
            if (entries.stream().anyMatch(e -> keepVer && e.version() != null ||
                !cfg.isSkipExpireTime() && e.expireTime() != 0)) {
                throw new IgniteCheckedException("DataStreamer cannot preserve entry metadata. " +
                    "Use --skip-conflict-version and --skip-expire-time, or disable DataStreamer.");
            }

            IgniteDataStreamer<Object, Object> streamer = streamers.computeIfAbsent(name, key -> {
                IgniteDataStreamer<Object, Object> res = ig.dataStreamer(key);

                try {
                    res.skipStore(true);
                    cfg.getDataStreamerConfiguration().accept(res);

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
            throw new IgniteCheckedException("Conflict resolution must be configured on the destination servers " +
                "and client node to preserve conflict versions [cache=" + name +
                "]. Use --skip-conflict-version to ignore source versions.");
        }

        if (keepVer || !cfg.isSkipExpireTime() && entries.stream().anyMatch(e -> e.expireTime() != 0)) {
            Map<KeyCacheObject, GridCacheDrInfo> batch = new LinkedHashMap<>();

            for (MigrationEntry entry : entries) {
                long expire = cfg.isSkipExpireTime() ? 0 : entry.expireTime();

                if (expire > 0 && expire <= System.currentTimeMillis())
                    continue;

                GridCacheVersion ver = keepVer ? entry.version() :
                    ctx.versions().next(ctx.affinity().affinityTopologyVersion().topologyVersion());

                if (ver == null)
                    throw new IgniteCheckedException("Missing source conflict version for cache: " + name);

                ver = ver.conflictVersion();

                // Recalculate affinity: source and destination partition counts may differ.
                Object key = destinationKey(entry.key());
                KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

                Object val = destinationValue(ctx, entry);

                batch.put(cacheKey, expire == 0 ? new GridCacheDrInfo(ctx.toCacheObject(val), ver) :
                    new GridCacheDrExpirationInfo(ctx.toCacheObject(val), ver, 0, expire));
            }

            if (!batch.isEmpty())
                internal.putAllConflict(batch);
        }
        else {
            Map<Object, Object> batch = new LinkedHashMap<>();

            for (MigrationEntry entry : entries)
                batch.put(destinationKey(entry.key()), destinationValue(ctx, entry));

            if (!cfg.isSkipExpireTime())
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
    private Object destinationValue(GridCacheContext<?, ?> ctx, MigrationEntry entry) throws IgniteCheckedException {
        return ctx.dataStructuresCache() ?
            MigrationDataStructures.value(entry.value(), ig.context().discovery().gridStartTime()) : entry.value();
    }

    /** Opens user or data structure caches without exposing system caches through the public API. */
    private IgniteCache<Object, Object> cache(String name) {
        try {
            ig.context().cache().getOrStartCache(name);

            return ((IgniteCacheProxyImpl<Object, Object>)ig.context().cache().<Object, Object>jcache(name))
                .gatewayWrapper();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to open migration cache: " + name, e);
        }
    }

    /** */
    private BinaryContext binaryContext() {
        return ((CacheObjectBinaryProcessorImpl)ig.context().cacheObjects()).binaryContext();
    }

    /** {@inheritDoc} */
    @Override public void finish() {
        streamers.values().forEach(IgniteDataStreamer::close);
        streamers.clear();
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (!closed) {
            closed = true;

            try {
                streamers.values().forEach(streamer -> streamer.close(true));
            }
            finally {
                if (ig != null)
                    ig.close();
            }
        }
    }
}
