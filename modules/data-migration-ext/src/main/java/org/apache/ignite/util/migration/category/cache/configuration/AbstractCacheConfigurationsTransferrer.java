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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.jetbrains.annotations.Nullable;

/**
 * Transfers cache configurations to the destination.
 *
 * @param <SRC> Type of the connection used to read from the source cluster.
 */
public abstract class AbstractCacheConfigurationsTransferrer<SRC> extends AbstractCategoryTransferrer<SRC, CacheConfigurationMigrationEntity> {
    /** Cache migration settings. */
    private final CacheMigrationConfiguration cfg;

    /** Memoized prepared cache list. */
    private List<CacheConfigurationMigrationEntity> prepared;

    /** Transformer used for the memoized list. */
    private MigrationTransformer<CacheConfigurationMigrationEntity> preparedTr;

    /**
     * @param src Connected source.
     * @param cfg Cache selection and creation settings.
     * @param log Logger.
     */
    protected AbstractCacheConfigurationsTransferrer(SRC src, CacheMigrationConfiguration cfg, IgniteLogger log) {
        super(src, MigrationCategory.CACHE_CONFIGURATION, CacheConfigurationMigrationEntity.class, log);

        this.cfg = cfg;
    }

    /**
     * Returns the source-to-destination cache name mapping for the selected caches,
     * applying the same filters and transformers as the configuration transfer.
     *
     * @param destSupportsDs Whether the destination supports data structure caches.
     * @param transformer Cache entity transformer.
     * @return Source cache name to destination cache name.
     */
    public Map<String, String> cacheNameMap(
        boolean destSupportsDs,
        MigrationTransformer<CacheConfigurationMigrationEntity> transformer
    ) {
        List<CacheConfigurationMigrationEntity> caches = prepare(destSupportsDs, transformer);
        Map<String, String> map = new LinkedHashMap<>(caches.size());

        for (CacheConfigurationMigrationEntity c : caches)
            map.put(c.sourceName(), c.configuration().getName());

        return Collections.unmodifiableMap(map);
    }

    /** @return Source cache definitions. */
    protected abstract Collection<CacheConfigurationMigrationEntity> sourceCaches();

    /** Returns the prepared cache list, memoized by transformer identity. */
    private List<CacheConfigurationMigrationEntity> prepare(
        boolean destSupportsDs,
        MigrationTransformer<CacheConfigurationMigrationEntity> transformer
    ) {
        if (prepared == null || preparedTr != transformer) {
            prepared = prepareCacheList(destSupportsDs, transformer);
            preparedTr = transformer;
        }

        return prepared;
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(MigrationDestination<?> dest, @Nullable MigrationTransformer<CacheConfigurationMigrationEntity> transformer) {
        MigrationTransformer<CacheConfigurationMigrationEntity> tr =
            transformer == null ? item -> item : transformer;
        boolean destSupportsDs = dest.origin() instanceof org.apache.ignite.internal.IgniteEx;
        List<CacheConfigurationMigrationEntity> caches = prepare(destSupportsDs, tr);

        if (caches.isEmpty()) {
            info("No caches selected for cache configuration migration.");

            return;
        }

        for (CacheConfigurationMigrationEntity cache : caches) {
            dest.accept(MigrationCategory.CACHE_CONFIGURATION, cache);

            info("Transferred cache configuration [sourceName=" + cache.sourceName() + ", destinationName=" +
                cache.configuration().getName() + ", sql=" + cache.sql() + ", configuration=" + cache.configuration() + "].");
        }
    }

    /** */
    private List<CacheConfigurationMigrationEntity> prepareCacheList(
        boolean destSupportDataStructures,
        MigrationTransformer<CacheConfigurationMigrationEntity> transformer
    ) {
        List<CacheConfigurationMigrationEntity> sel = new ArrayList<>();
        Set<String> dstNames = new HashSet<>();

        for (CacheConfigurationMigrationEntity item : sourceCaches()) {
            if (!cfg.includes(item.configuration())) {
                if (log.isInfoEnabled())
                    log.info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache group filter'].");

                continue;
            }

            if (item.type() == CacheType.DATA_STRUCTURES && (!cfg.moveDataStructures() || !destSupportDataStructures)) {
                if (log.isInfoEnabled())
                    log.info("Skipped data structure cache [sourceName=" + item.sourceName() + ", reason='" +
                        (!cfg.moveDataStructures() ? "move-data-structures=false" :
                            "destination does not support data structures") + "'].");

                continue;
            }

            String grpName = item.configuration().getGroupName();
            CacheConfigurationMigrationEntity cache = transformer.transform(item);

            if (cache == null) {
                if (log.isInfoEnabled())
                    log.info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache transformer returned null'].");

                continue;
            }

            CacheConfiguration<?, ?> cacheCfg = cfg.cacheConfigurationTransformer().apply(cache.configuration());

            if (cacheCfg == null) {
                if (log.isInfoEnabled())
                    log.info("Skipped cache [sourceName=" + item.sourceName() +
                        ", reason='cache configuration transformer returned null'].");

                continue;
            }

            if (item.type() == CacheType.DATA_STRUCTURES &&
                (!item.sourceName().equals(cacheCfg.getName()) || !Objects.equals(grpName, cacheCfg.getGroupName())))
                throw new IgniteException("Data structure cache and group names must be preserved: " + item.sourceName() + ".");

            if (cacheCfg.getName() == null || cacheCfg.getName().isEmpty() || !dstNames.add(cacheCfg.getName()))
                throw new IgniteException("Invalid or duplicate transformed cache name: " + cacheCfg.getName() + ".");

            sel.add(new CacheConfigurationMigrationEntity(item.sourceName(), cacheCfg, cache.sql(), item.type(),
                cfg.createCacheIfNotExists()));
        }

        return sel;
    }
}
