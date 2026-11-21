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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;

/** Transfers selected cache configurations from a thin-client source. */
public class ThinClientCacheConfigurationsTransferrer extends AbstractCategoryTransferrer<IgniteClient, MigrationCache> {
    /** */
    private final CacheMigrationConfiguration cfg;

    /** */
    private final MigrationCachePlan cachePlan;

    /**
     * @param from Connected thin-client source.
     * @param log Logger.
     * @param cfg Destination cache creation policy.
     * @param cachePlan Cache selection for this migration.
     */
    public ThinClientCacheConfigurationsTransferrer(
        IgniteClient from,
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

        warn("Thin source exposes only ClientCacheConfiguration; SQL-created flags and unsupported cache " +
            "properties cannot be copied exactly.");

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
                action = CacheConfigurationsTransferrer.createOrValidateCache((IgniteEx)destOrigin, cache, cfg);
            else if (destOrigin instanceof IgniteClient)
                action = CacheConfigurationsTransferrer.createOrValidateThinCache((IgniteClient)destOrigin, cache, cfg);
            else
                throw new IgniteException("Unsupported cache configuration destination: " +
                    destOrigin.getClass().getName());

            info("Cache " + action + " [sourceName=" + cache.sourceName() + ", destinationName=" +
                cache.configuration().getName() + ", sql=" + cache.sql() + ", configuration=" +
                cache.configuration() + "].");
        }
    }

    /**
     * Reads cache configuration through the thin-client API. Thin clients do not expose cache type,
     * SQL-created state, data-structure caches, or the full server-side cache configuration.
     */
    public static List<MigrationCache> sourceCaches(IgniteClient client) {
        List<MigrationCache> res = new ArrayList<>();

        try {
            for (String name : client.cacheNames()) {
                ClientCache<Object, Object> cache = client.<Object, Object>cache(name);
                ClientCacheConfiguration clientCfg = cache.getConfiguration();
                org.apache.ignite.configuration.CacheConfiguration<?, ?> cacheCfg =
                    ThinClientCacheConfigurationAdapter.toCacheConfiguration(clientCfg);

                res.add(new MigrationCache(name, cacheCfg, false, CacheType.USER));
            }
        }
        catch (RuntimeException e) {
            throw new IgniteException("Failed to read cache configurations through the thin client.", e);
        }

        res.sort(Comparator.comparing(MigrationCache::sourceName));

        return res;
    }
}
