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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.jetbrains.annotations.Nullable;

/** Prepares source cache configurations once for both configuration and data transfers. */
public abstract class AbstractCacheConfigurationsTransferrer<SRC> extends AbstractCategoryTransferrer<SRC, CacheConfigurationMigration> {
    /** Cache migration settings. */
    private final CacheMigrationConfiguration cfg;

    /** Prepared cache definitions shared with the cache data transferrer. */
    private List<CacheConfigurationMigration> caches;

    /**
     * @param src Connected source.
     * @param cfg Cache selection and creation settings.
     * @param log Logger.
     */
    protected AbstractCacheConfigurationsTransferrer(SRC src, CacheMigrationConfiguration cfg, IgniteLogger log) {
        super(src, MigrationCategory.CACHE_CONFIGURATION, CacheConfigurationMigration.class, log);
        this.cfg = cfg;
    }

    /**
     * Prepares selected cache definitions for configuration and data transfers.
     *
     * @param dest Connected thick destination, if available to the cache configuration transformer.
     * @param destSupportsDs Whether the destination supports data structure caches.
     * @param transformer Cache entity transformer.
     * @return Prepared cache definitions.
     */
    public List<CacheConfigurationMigration> prepareCaches(
        @Nullable IgniteEx dest,
        boolean destSupportsDs,
        MigrationTransformer<CacheConfigurationMigration> transformer
    ) {
        if (caches == null)
            caches = cfg.prepare(this::sourceCaches, dest, destSupportsDs, transformer, log);

        return caches;
    }

    /** @return Source cache definitions. */
    protected abstract Collection<CacheConfigurationMigration> sourceCaches();

    /** {@inheritDoc} */
    @Override protected void doTransfer(
        MigrationDestination dest,
        @Nullable MigrationTransformer<CacheConfigurationMigration> transformer
    ) {
        if (caches == null)
            throw new IllegalStateException("Cache configurations must be prepared before transfer.");

        if (caches.isEmpty()) {
            info("No caches selected for cache configuration migration.");

            return;
        }

        for (CacheConfigurationMigration cache : caches) {
            dest.accept(MigrationCategory.CACHE_CONFIGURATION, cache);

            info("Transferred cache configuration [sourceName=" + cache.sourceName() + ", destinationName=" +
                cache.configuration().getName() + ", sql=" + cache.sql() + ", configuration=" + cache.configuration() + "].");
        }
    }
}
