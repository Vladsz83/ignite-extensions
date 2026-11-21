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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransfer;

/** Transfers selected cache configurations through thick or thin clients. */
public class CacheConfigurationsTransferrer extends AbstractCategoryTransfer<MigrationCache> {
    /** Source connection. */
    private final Object from;

    /** Destination connection. */
    private final Object to;

    /** */
    private final CacheConfigurationsMigrationConfiguration cfg;

    /** */
    private final MigrationCachePlan cachePlan;

    /**
     * @param from Connected source.
     * @param to Connected destination.
     * @param log Migration logger.
     * @param cfg Destination cache creation policy.
     * @param cachePlan Cache selection for this migration.
     */
    public CacheConfigurationsTransferrer(
        Object from,
        Object to,
        IgniteLogger log,
        CacheConfigurationsMigrationConfiguration cfg,
        MigrationCachePlan cachePlan
    ) {
        super(log);

        this.from = from;
        this.to = to;
        this.cfg = cfg;
        this.cachePlan = cachePlan;
    }

    /** {@inheritDoc} */
    @Override public MigrationCategory category() {
        return MigrationCategory.CACHE_CONFIGURATION;
    }

    /** {@inheritDoc} */
    @Override public Class<?> dataClass() {
        return MigrationCache.class;
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(MigrationDestination dest, MigrationTransformer<MigrationCache> transformer)
        throws IgniteCheckedException {
        Object src = from;
        List<MigrationCache> caches = cachePlan.caches();
        Object destOrigin = to;

        if (caches.isEmpty()) {
            info("No caches selected for cache configuration migration.");

            return;
        }

        if (!(src instanceof IgniteEx) && !(src instanceof IgniteClient))
            throw new IgniteCheckedException("Unsupported cache configuration source: " + src.getClass().getName());

        if (src instanceof IgniteClient)
            warn("Thin source exposes only ClientCacheConfiguration; SQL-created flags and unsupported cache " +
                "properties cannot be copied exactly.");

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
                action = createOrValidateThinCache((IgniteClient)destOrigin, cache);
            else
                throw new IgniteCheckedException("Unsupported cache configuration destination: " +
                    destOrigin.getClass().getName());

            info("Cache " + action + " [sourceName=" + cache.sourceName() + ", destinationName=" +
                cache.configuration().getName() + ", sql=" + cache.sql() + ", configuration=" +
                cache.configuration() + "].");
        }
    }

    /** */
    static String createOrValidateCache(IgniteEx ig, MigrationCache cache,
        CacheConfigurationsMigrationConfiguration cfg) {
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

    /** Creates or validates a cache through the thin-client API. */
    private String createOrValidateThinCache(IgniteClient client, MigrationCache cache) throws IgniteCheckedException {
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
            throw new IgniteCheckedException("Failed to create destination cache through the thin client: " + name + '.', e);
        }
    }

}
