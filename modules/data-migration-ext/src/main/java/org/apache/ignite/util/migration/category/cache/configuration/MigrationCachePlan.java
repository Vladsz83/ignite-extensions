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
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.MigrationTransformer;

/** Per-migration cache selection and name mapping. Prepared once on first use; performs no destination writes. */
public final class MigrationCachePlan {
    /** */
    private final IgniteEx srcIg;

    /** */
    private final CacheSelectionConfiguration selection;

    /** */
    private final MigrationTransformer<MigrationCache> transformer;

    /** Selected cache definitions; non-null once preparation has completed. */
    private List<MigrationCache> caches;

    /** */
    private Map<String, String> cacheNames;

    /**
     * @param srcIg Connected source cluster.
     * @param selection Shared cache selection and name mapping.
     * @param transformer Cache selection and transformation callback.
     */
    public MigrationCachePlan(
        IgniteEx srcIg,
        CacheSelectionConfiguration selection,
        MigrationTransformer<MigrationCache> transformer
    ) {
        this.srcIg = srcIg;
        this.selection = selection;
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

    /** */
    private synchronized void prepare() throws IgniteCheckedException {
        if (caches != null)
            return;

        Collection<MigrationCache> srcCaches = sourceCaches(srcIg);
        List<MigrationCache> selected = new ArrayList<>();
        Map<String, String> names = new LinkedHashMap<>();
        Set<String> destNames = new HashSet<>();

        for (MigrationCache item : srcCaches) {
            if (!selection.includes(item.configuration()) ||
                item.type() == CacheType.DATA_STRUCTURES && !selection.moveDataStructures())
                continue;

            String grpName = item.configuration().getGroupName();
            MigrationCache cache = transformer.transform(item);

            if (cache == null)
                continue;

            CacheConfiguration<?, ?> cacheCfg = selection.cacheConfigurationTransformer().apply(cache.configuration());

            if (cacheCfg == null)
                continue;

            if (item.type() == CacheType.DATA_STRUCTURES &&
                (!item.sourceName().equals(cacheCfg.getName()) || !Objects.equals(grpName, cacheCfg.getGroupName()))) {
                throw new IgniteCheckedException("Data structure cache and group names must be preserved: " +
                    item.sourceName() + '.');
            }

            if (cacheCfg.getName() == null || cacheCfg.getName().isEmpty() || !destNames.add(cacheCfg.getName()))
                throw new IgniteCheckedException("Invalid or duplicate transformed cache name: " + cacheCfg.getName() + '.');

            names.put(item.sourceName(), cacheCfg.getName());
            selected.add(new MigrationCache(item.sourceName(), cacheCfg, cache.sql(), item.type()));
        }

        cacheNames = Collections.unmodifiableMap(names);
        caches = Collections.unmodifiableList(selected);
    }

    /** */
    private static List<MigrationCache> sourceCaches(IgniteEx ig) {
        List<MigrationCache> res = new ArrayList<>();

        for (DynamicCacheDescriptor desc : ig.context().cache().cacheDescriptors().values()) {
            if (desc.cacheType() != CacheType.USER && desc.cacheType() != CacheType.DATA_STRUCTURES)
                continue;

            // The descriptor schema contains ALTER TABLE/CREATE INDEX changes absent from the original configuration.
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(desc.cacheConfiguration());

            cacheCfg.clearQueryEntities();
            cacheCfg.setQueryEntities(desc.schema().entities());

            try {
                // Transformers must never mutate a live descriptor or its nested affinity/query objects.
                cacheCfg = U.unmarshal(ig.context(), U.marshal(ig.context(), cacheCfg),
                    MigrationCachePlan.class.getClassLoader());
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
