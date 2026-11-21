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

package org.apache.ignite.util.migration.category.cache.configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;

/** Transfers selected cache configurations from a thick-client source. */
public class CacheConfigurationsTransferrer extends AbstractCategoryTransferrer<IgniteEx, MigrationCache> {
    /** */
    private final CacheMigrationConfiguration cfg;

    /** */
    private final MigrationCachePlan cachePlan;

    /**
     * @param from Source.
     * @param log Logger.
     * @param cfg Destination cache creation policy.
     * @param cachePlan Cache selection for this migration.
     */
    public CacheConfigurationsTransferrer(
        IgniteEx from,
        IgniteLogger log,
        CacheMigrationConfiguration cfg,
        MigrationCachePlan cachePlan
    ) {
        super(from, MigrationCategory.CACHE_CONFIGURATION, MigrationCache.class, log);

        this.cfg = cfg;
        this.cachePlan = cachePlan;
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(MigrationDestination dest, MigrationTransformer<MigrationCache> transformer) {
        List<MigrationCache> caches = cachePlan.caches();

        if (caches.isEmpty()) {
            info("No caches selected for cache configuration migration.");

            return;
        }

        Object destOrigin = dest.connect();

        if (destOrigin instanceof IgniteClient)
            warn("Thin destination supports only ClientCacheConfiguration properties; unsupported source cache " +
                "properties will use destination defaults.");

        for (MigrationCache cache : caches) {
            if (destOrigin instanceof IgniteClient && cache.configuration().getAffinity() != null &&
                !(cache.configuration().getAffinity() instanceof RendezvousAffinityFunction)) {
                warn("Thin destination cannot represent the cache's custom affinity; using default rendezvous affinity " +
                    "[cache=" + cache.configuration().getName() + "].");
            }

            String action;

            if (destOrigin instanceof IgniteEx)
                action = createOrValidateCache((IgniteEx)destOrigin, cache, cfg);
            else if (destOrigin instanceof IgniteClient)
                action = createOrValidateThinCache((IgniteClient)destOrigin, cache, cfg);
            else
                throw new IgniteException("Unsupported cache configuration destination: " +
                    destOrigin.getClass().getName());

            info("Cache " + action + " [sourceName=" + cache.sourceName() + ", destinationName=" +
                cache.configuration().getName() + ", sql=" + cache.sql() + ", configuration=" +
                cache.configuration() + "].");
        }
    }

    /** */
    static String createOrValidateCache(IgniteEx ig, MigrationCache cache,
        CacheMigrationConfiguration cfg) {
        String name = cache.configuration().getName();
        DynamicCacheDescriptor desc = ig.context().cache().cacheDescriptor(name);

        if (desc != null && desc.cacheType() != cache.type())
            throw new IgniteException("Destination cache type differs from source: " + name + '.');

        if (cfg.noCacheCreate()) {
            if (desc == null)
                throw new IgniteException("Destination cache does not exist: " + name + '.');

            openCache(ig, name);

            return "creation skipped; using existing [reason=no-cache-create]";
        }

        try {
            boolean created = ig.context().cache().dynamicStartCache(cache.configuration(), name, null, cache.type(),
                cache.sql(), false, true, true).get();

            if (!created && !cfg.createCacheIfNotExists())
                throw new CacheExistsException("Destination cache already exists: " + name + '.');

            return created ? "created" : "creation skipped; using existing [reason=already-exists]";
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to create destination cache: " + name + '.', e);
        }
    }

    /** */
    private static void openCache(IgniteEx ig, String name) {
        try {
            ig.context().cache().getOrStartCache(name);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to open migration cache: " + name + '.', e);
        }
    }

    /** */
    static String createOrValidateThinCache(IgniteClient client, MigrationCache cache,
        CacheMigrationConfiguration cfg) {
        String name = cache.configuration().getName();
        boolean exists = client.cacheNames().contains(name);

        if (cfg.noCacheCreate()) {
            if (!exists)
                throw new IgniteException("Destination cache does not exist: " + name + '.');

            return "creation skipped; using existing [reason=no-cache-create]";
        }

        if (exists && !cfg.createCacheIfNotExists())
            throw new CacheExistsException("Destination cache already exists: " + name + '.');

        if (exists)
            return "creation skipped; using existing [reason=already-exists]";

        ClientCacheConfiguration clientCfg = ThinClientCacheConfigurationAdapter
            .toClientCacheConfiguration(cache.configuration());

        try {
            if (cfg.createCacheIfNotExists())
                client.getOrCreateCache(clientCfg);
            else
                client.createCache(clientCfg);

            return "created";
        }
        catch (RuntimeException e) {
            throw new IgniteException("Failed to create destination cache through the thin client: " + name + '.', e);
        }
    }

    /** Reads cache definitions from a thick-client source. */
    public static List<MigrationCache> sourceCaches(IgniteEx ig) {
        List<MigrationCache> res = new ArrayList<>();

        for (DynamicCacheDescriptor desc : ig.context().cache().cacheDescriptors().values()) {
            if (desc.cacheType() != CacheType.USER && desc.cacheType() != CacheType.DATA_STRUCTURES)
                continue;

            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(desc.cacheConfiguration());

            cacheCfg.clearQueryEntities();
            cacheCfg.setQueryEntities(desc.schema().entities());

            try {
                cacheCfg = U.unmarshal(ig.context(), U.marshal(ig.context(), cacheCfg),
                    CacheConfigurationsTransferrer.class.getClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to copy cache configuration: " + cacheCfg.getName() + '.', e);
            }

            for (QueryEntity entity : cacheCfg.getQueryEntities()) {
                if (entity instanceof QueryEntityEx) {
                    QueryEntityEx ex = (QueryEntityEx)entity;

                    if (Integer.valueOf(-1).equals(ex.getPrimaryKeyInlineSize()))
                        ex.setPrimaryKeyInlineSize(null);

                    if (Integer.valueOf(-1).equals(ex.getAffinityKeyInlineSize()))
                        ex.setAffinityKeyInlineSize(null);
                }
            }

            res.add(new MigrationCache(cacheCfg.getName(), cacheCfg, desc.sql(), desc.cacheType()));
        }

        res.sort(Comparator.comparing(MigrationCache::sourceName));

        return res;
    }
}
