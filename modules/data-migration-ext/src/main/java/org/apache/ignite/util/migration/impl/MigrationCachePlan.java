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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationCache;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.api.MigrationTransformer;

/** Per-migration cache selection and name mapping. Prepared once on first use; performs no destination writes. */
public final class MigrationCachePlan {
    /** */
    private final MigrationSource src;

    /** */
    private final IgniteToIgniteMigrationConfiguration cfg;

    /** */
    private final MigrationTransformer<MigrationCache> transformer;

    /** Selected cache definitions; non-null once preparation has completed. */
    private List<MigrationCache> caches;

    /** */
    private Map<String, String> cacheNames;

    /** */
    private boolean keepViews;

    /**
     * @param src Connected source.
     * @param cfg Migration configuration.
     * @param transformer Cache selection and transformation callback.
     */
    public MigrationCachePlan(
        MigrationSource src,
        IgniteToIgniteMigrationConfiguration cfg,
        MigrationTransformer<MigrationCache> transformer
    ) {
        this.src = src;
        this.cfg = cfg;
        this.transformer = transformer;
    }

    /** @return Selected and transformed cache definitions. */
    public List<MigrationCache> caches() throws IgniteCheckedException {
        prepare();

        return caches;
    }

    /** @return Source to destination cache names. */
    public Map<String, String> cacheNames() throws IgniteCheckedException {
        prepare();

        return cacheNames;
    }

    /** @return Whether all user caches are selected without renaming, allowing SQL view transfer. */
    public boolean keepViews() throws IgniteCheckedException {
        prepare();

        return keepViews;
    }

    /** */
    private synchronized void prepare() throws IgniteCheckedException {
        if (caches != null)
            return;

        Collection<MigrationCache> srcCaches = src.caches();
        List<MigrationCache> selected = new ArrayList<>();
        Map<String, String> names = new LinkedHashMap<>();
        Set<String> destNames = new HashSet<>();

        for (MigrationCache item : srcCaches) {
            if (!cfg.includes(item.configuration()) ||
                item.type() == CacheType.DATA_STRUCTURES && !cfg.isMoveDataStructures())
                continue;

            String grpName = item.configuration().getGroupName();
            MigrationCache cache = transformer.transform(item);

            if (cache == null)
                continue;

            CacheConfiguration<?, ?> cacheCfg = cfg.getCacheConfigurationTransformer().apply(cache.configuration());

            if (cacheCfg == null)
                continue;

            if (item.type() == CacheType.DATA_STRUCTURES &&
                (!item.sourceName().equals(cacheCfg.getName()) || !Objects.equals(grpName, cacheCfg.getGroupName()))) {
                throw new IgniteCheckedException("Data structure cache and group names must be preserved: " +
                    item.sourceName());
            }

            if (cacheCfg.getName() == null || cacheCfg.getName().isEmpty() || !destNames.add(cacheCfg.getName()))
                throw new IgniteCheckedException("Invalid or duplicate transformed cache name: " + cacheCfg.getName());

            names.put(item.sourceName(), cacheCfg.getName());
            selected.add(new MigrationCache(item.sourceName(), cacheCfg, cache.sql(), item.type()));
        }

        keepViews = srcCaches.stream().filter(cache -> cache.type() == CacheType.USER)
            .allMatch(cache -> cache.sourceName().equals(names.get(cache.sourceName())));
        cacheNames = Collections.unmodifiableMap(names);
        caches = Collections.unmodifiableList(selected);
    }
}
