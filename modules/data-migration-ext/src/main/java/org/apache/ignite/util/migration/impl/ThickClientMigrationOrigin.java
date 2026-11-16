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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationCache;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationEntry;
import org.apache.ignite.util.migration.api.MigrationPartition;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.api.MigrationTypeMapping;

/** Client-node connection, usable as either endpoint. */
public class ThickClientMigrationOrigin implements MigrationSource, MigrationDestination {
    /** */
    private final IgniteConfiguration cfg;

    /** */
    private IgniteEx ig;

    /** */
    private boolean closed;

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

    /** */
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
        List<BinaryMetadata> res = new ArrayList<>();

        ig.binary().types().forEach(type -> res.add(((BinaryTypeImpl)type).metadata()));

        return res;
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
            if (desc.cacheType() != CacheType.USER)
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

            res.add(new MigrationCache(cfg.getName(), cfg, desc.sql()));
        }

        res.sort(Comparator.comparing(MigrationCache::sourceName));

        return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Serializable> metastorage(IgniteToIgniteMigrationConfiguration cfg)
        throws IgniteCheckedException {
        Map<String, Serializable> res = new LinkedHashMap<>();

        ig.context().distributedMetastorage().iterate("", (key, val) -> {
            if (cfg.getMetastorageFilter().test(key))
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
    @Override public QueryCursor<MigrationEntry> read(MigrationPartition part, IgniteToIgniteMigrationConfiguration cfg) {
        IgniteCache<Object, Object> cache = ig.cache(part.sourceCache()).withKeepBinary().withSkipStore()
            .withExpiryPolicy(new EternalExpiryPolicy());

        return cache.query(new ScanQuery<>().setPartition(part.partition()).setPageSize(cfg.getBatchSize()),
            new MigrationEntryReader(part.sourceCache()));
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

        if (cfg.isNoCacheCreate()) {
            if (ig.cache(name) == null)
                throw new IgniteException("Destination cache does not exist: " + name);

            return;
        }

        boolean created = ig.getOrCreateCache0(cache.configuration(), cache.sql()).get2();

        if (!created && !cfg.isCreateCacheIfNotExists())
            throw new CacheExistsException("Destination cache already exists: " + name);
    }

    /** {@inheritDoc} */
    @Override public void metastorage(String key, Serializable val) throws IgniteCheckedException {
        ig.context().distributedMetastorage().write(key, val);
    }

    /** {@inheritDoc} */
    @Override public void write(String name, List<MigrationEntry> entries, IgniteToIgniteMigrationConfiguration cfg)
        throws IgniteCheckedException {
        IgniteCache<Object, Object> cache = ig.cache(name).withKeepBinary().withSkipStore();

        if (!cfg.isSkipCdcInfo()) {
            GridCacheContext<Object, Object> ctx = ig.cachex(name).context();
            Map<KeyCacheObject, GridCacheDrInfo> batch = new LinkedHashMap<>();

            for (MigrationEntry entry : entries) {
                long expire = cfg.isSkipExpireTime() ? 0 : entry.expireTime();

                if (expire > 0 && expire <= System.currentTimeMillis())
                    continue;

                GridCacheVersion ver = entry.version();

                if (ver == null)
                    throw new IgniteCheckedException("Missing source conflict version for cache: " + name);

                if (ver.otherClusterVersion() != null)
                    ver = (GridCacheVersion)ver.otherClusterVersion();

                // Recalculate affinity: source and destination partition counts may differ.
                Object key = destinationKey(entry.key());
                KeyCacheObject cacheKey = ctx.toCacheKeyObject(key);

                batch.put(cacheKey, expire == 0 ? new GridCacheDrInfo(ctx.toCacheObject(entry.value()), ver) :
                    new GridCacheDrExpirationInfo(ctx.toCacheObject(entry.value()), ver, 0, expire));
            }

            if (!batch.isEmpty())
                ig.cachex(name).putAllConflict(batch);
        }
        else {
            Map<Object, Object> batch = new LinkedHashMap<>();

            for (MigrationEntry entry : entries) {
                long expire = cfg.isSkipExpireTime() ? 0 : entry.expireTime();

                if (expire > 0) {
                    if (expire > System.currentTimeMillis())
                        cache.withExpiryPolicy(new AbsoluteExpiryPolicy(expire))
                            .put(destinationKey(entry.key()), entry.value());
                }
                else
                    batch.put(destinationKey(entry.key()), entry.value());
            }

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
    private BinaryContext binaryContext() {
        return ((CacheObjectBinaryProcessorImpl)ig.context().cacheObjects()).binaryContext();
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (!closed) {
            closed = true;

            if (ig != null)
                ig.close();
        }
    }

    /** Computes remaining TTL on the destination server, rather than when a batch is enqueued. */
    private static class AbsoluteExpiryPolicy implements ExpiryPolicy, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long expire;

        /** */
        AbsoluteExpiryPolicy(long expire) {
            this.expire = expire;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            return new Duration(TimeUnit.MILLISECONDS, Math.max(0, expire - System.currentTimeMillis()));
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            return getExpiryForCreation();
        }
    }
}
