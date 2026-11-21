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

/** Transfers cache configurations to a thick-client destination. */
public class CacheConfigurationsTransferrer extends AbstractCategoryTransferrer<Object, MigrationCache> {
    /** Connected destination. */
    private final IgniteEx dest;

    /** */
    private final CacheMigrationConfiguration cfg;

    /** */
    private final List<MigrationCache> caches;

    /**
     * @param from Connected source (thick or thin).
     * @param dest Connected destination.
     * @param log Logger.
     * @param cfg Caches migration configuration.
     * @param caches Selected and transformed cache definitions.
     */
    public CacheConfigurationsTransferrer(
        Object from,
        IgniteEx dest,
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

        for (MigrationCache cache : caches) {
            String name = cache.configuration().getName();
            DynamicCacheDescriptor desc = dest.context().cache().cacheDescriptor(name);

            if (desc != null && desc.cacheType() != cache.type())
                throw new IgniteException("Destination cache type differs from source [cache=" + name + "].");

            String action;

            if (cfg.noCacheCreate()) {
                if (desc == null)
                    throw new IgniteException("Destination cache does not exist [cache=" + name + "].");

                try {
                    dest.context().cache().getOrStartCache(name);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to open migration cache [cache=" + name + "].", e);
                }

                action = "creation skipped; using existing [reason='no cache create']";
            }
            else {
                try {
                    boolean created = dest.context().cache().dynamicStartCache(cache.configuration(), name, null,
                        cache.type(), cache.sql(), false, true, true).get();

                    if (!created && !cfg.createCacheIfNotExists())
                        throw new CacheExistsException("Destination cache already exists [cache=" + name + "].");

                    action = created ? "created" : "creation skipped; using existing [reason='already exists'].";
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to create destination cache [cache=" + name + "].", e);
                }
            }

            info("Cache " + action + " [sourceName=" + cache.sourceName() + ", destinationName=" +
                cache.configuration().getName() + ", sql=" + cache.sql() + ", configuration=" +
                cache.configuration() + "].");
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
                throw new IgniteException("Failed to copy cache configuration [cache=" + cacheCfg.getName() + "].", e);
            }

            for (QueryEntity entity : cacheCfg.getQueryEntities()) {
                if (entity instanceof QueryEntityEx ex) {
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
