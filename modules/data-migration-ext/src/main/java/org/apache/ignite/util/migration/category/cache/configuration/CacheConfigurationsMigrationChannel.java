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
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.util.migration.MigrationOrigin;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractMigrationChannel;

/** Transfers selected cache and SQL configurations to the destination. */
public class CacheConfigurationsMigrationChannel extends AbstractMigrationChannel<IgniteEx, IgniteEx, MigrationCache> {
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
    public CacheConfigurationsMigrationChannel(
        MigrationOrigin<IgniteEx> from,
        MigrationOrigin<IgniteEx> to,
        IgniteLogger log,
        CacheConfigurationsMigrationConfiguration cfg,
        MigrationCachePlan cachePlan
    ) {
        super(from, to, log);

        this.cfg = cfg;
        this.cachePlan = cachePlan;
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(ExecutorService pool, MigrationTransformer<MigrationCache> transformer)
        throws IgniteCheckedException {
        List<MigrationCache> caches = cachePlan.caches();

        for (MigrationCache cache : caches) {
            createOrValidateCache(to.origin(), cache, cfg);

            info("Prepared cache: " + cache.configuration().getName() + '.');
        }
    }

    /** */
    private static void createOrValidateCache(IgniteEx ig, MigrationCache cache,
        CacheConfigurationsMigrationConfiguration cfg) {
        String name = cache.configuration().getName();
        DynamicCacheDescriptor desc = ig.context().cache().cacheDescriptor(name);

        if (desc != null && desc.cacheType() != cache.type())
            throw new IgniteException("Destination cache type differs from source: " + name + '.');

        if (cfg.noCacheCreate()) {
            if (desc == null)
                throw new IgniteException("Destination cache does not exist: " + name + '.');

            openCache(ig, name);

            return;
        }

        boolean created;

        try {
            created = ig.context().cache().dynamicStartCache(cache.configuration(), name, null, cache.type(),
                cache.sql(), false, true, true).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to create destination cache: " + name + '.', e);
        }

        if (!created && !cfg.createCacheIfNotExists())
            throw new CacheExistsException("Destination cache already exists: " + name + '.');
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
}
