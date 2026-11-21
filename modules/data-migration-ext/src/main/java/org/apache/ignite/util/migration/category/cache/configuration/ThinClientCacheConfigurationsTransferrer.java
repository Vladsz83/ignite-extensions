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
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;

/** Transfers cache configurations to a thin-client destination. */
public class ThinClientCacheConfigurationsTransferrer extends AbstractCategoryTransferrer<Object, MigrationCache> {
    /** Connected thin-client destination. */
    private final IgniteClient dest;

    /** */
    private final CacheMigrationConfiguration cfg;

    /** */
    private final List<MigrationCache> caches;

    /**
     * @param from Connected source (thick or thin).
     * @param dest Connected thin-client destination.
     * @param log Logger.
     * @param cfg Caches migration configuration.
     * @param caches Selected and transformed cache definitions.
     */
    public ThinClientCacheConfigurationsTransferrer(
        Object from,
        IgniteClient dest,
        IgniteLogger log,
        CacheMigrationConfiguration cfg,
        List<MigrationCache> caches
    ) {
        super(from, MigrationCategory.CACHE_CONFIGURATION, MigrationCache.class, log);

        this.dest = dest;
        this.cfg = cfg;
        this.caches = caches;
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(MigrationDestination destEndpoint, MigrationTransformer<MigrationCache> transformer) {
        if (caches.isEmpty()) {
            info("No caches selected for cache configuration migration.");

            return;
        }

        warn("Thin destination supports only ClientCacheConfiguration properties; unsupported source cache " +
            "properties will use destination defaults.");

        for (MigrationCache cache : caches) {
            if (cache.configuration().getAffinity() != null &&
                !(cache.configuration().getAffinity() instanceof RendezvousAffinityFunction)) {
                warn("Thin destination cannot represent the cache's custom affinity; using default rendezvous affinity " +
                    "[cache=" + cache.configuration().getName() + "].");
            }

            String action = createOrValidateThinCache(cache);

            info("Cache " + action + " [sourceName=" + cache.sourceName() + ", destinationName=" +
                cache.configuration().getName() + ", sql=" + cache.sql() + ", configuration=" +
                cache.configuration() + "].");
        }
    }

    /** */
    private String createOrValidateThinCache(MigrationCache cache) {
        String name = cache.configuration().getName();
        boolean exists = dest.cacheNames().contains(name);

        if (cfg.noCacheCreate()) {
            if (!exists)
                throw new IgniteException("Destination cache does not exist [cache=" + name + "].");

            return "creation skipped; using existing [reason='no cache create']";
        }

        if (exists && !cfg.createCacheIfNotExists())
            throw new CacheExistsException("Destination cache already exists [cache=" + name + "].");

        if (exists)
            return "creation skipped; using existing [reason='already exists']";

        ClientCacheConfiguration clientCfg = ThinClientCacheConfigurationAdapter
            .toClientCacheConfiguration(cache.configuration());

        if (cfg.createCacheIfNotExists())
            dest.getOrCreateCache(clientCfg);
        else
            dest.createCache(clientCfg);

        return "created";
    }

    /**
     * Reads cache configuration through the thin-client API. Thin clients do not expose cache type,
     * SQL-created state, data-structure caches, or the full server-side cache configuration.
     */
    public static List<MigrationCache> sourceCaches(IgniteClient client) {
        List<MigrationCache> res = new ArrayList<>();

        for (String name : client.cacheNames()) {
            ClientCache<Object, Object> cache = client.<Object, Object>cache(name);
            ClientCacheConfiguration clientCfg = cache.getConfiguration();
            org.apache.ignite.configuration.CacheConfiguration<?, ?> cacheCfg =
                ThinClientCacheConfigurationAdapter.toCacheConfiguration(clientCfg);

            res.add(new MigrationCache(name, cacheCfg, false, CacheType.USER));
        }

        res.sort(Comparator.comparing(MigrationCache::sourceName));

        return res;
    }
}
