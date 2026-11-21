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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
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
    private final IgniteClient srcClient;

    /** */
    private final CacheSelectionConfiguration selection;

    /** */
    private final boolean supportsDataStructures;

    /** */
    private final MigrationTransformer<MigrationCache> transformer;

    /** Logger; may be absent for callers using the standalone planning API. */
    private final IgniteLogger log;

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
        this(srcIg, selection, transformer, true, null);
    }

    /**
     * @param srcIg Connected source cluster.
     * @param selection Shared cache selection and name mapping.
     * @param transformer Cache selection and transformation callback.
     * @param log Logger.
     */
    public MigrationCachePlan(
        IgniteEx srcIg,
        CacheSelectionConfiguration selection,
        MigrationTransformer<MigrationCache> transformer,
        IgniteLogger log
    ) {
        this(srcIg, selection, transformer, true, log);
    }

    /**
     * @param srcIg Connected source cluster.
     * @param selection Shared cache selection and name mapping.
     * @param transformer Cache selection and transformation callback.
     * @param supportsDataStructures Whether the destination can preserve data-structure cache semantics.
     */
    public MigrationCachePlan(
        IgniteEx srcIg,
        CacheSelectionConfiguration selection,
        MigrationTransformer<MigrationCache> transformer,
        boolean supportsDataStructures
    ) {
        this(srcIg, selection, transformer, supportsDataStructures, null);
    }

    /**
     * @param srcIg Connected source cluster.
     * @param selection Shared cache selection and name mapping.
     * @param transformer Cache selection and transformation callback.
     * @param supportsDataStructures Whether the destination can preserve data-structure cache semantics.
     * @param log Logger.
     */
    public MigrationCachePlan(
        IgniteEx srcIg,
        CacheSelectionConfiguration selection,
        MigrationTransformer<MigrationCache> transformer,
        boolean supportsDataStructures,
        IgniteLogger log
    ) {
        this.srcIg = srcIg;
        srcClient = null;
        this.selection = selection;
        this.supportsDataStructures = supportsDataStructures;
        this.transformer = transformer;
        this.log = log;
    }

    /**
     * @param srcClient Connected thin-client source.
     * @param selection Shared cache selection and name mapping.
     * @param transformer Cache selection and transformation callback.
     */
    public MigrationCachePlan(
        IgniteClient srcClient,
        CacheSelectionConfiguration selection,
        MigrationTransformer<MigrationCache> transformer
    ) {
        this(srcClient, selection, transformer, null);
    }

    /**
     * @param srcClient Connected thin-client source.
     * @param selection Shared cache selection and name mapping.
     * @param transformer Cache selection and transformation callback.
     * @param log Logger.
     */
    public MigrationCachePlan(
        IgniteClient srcClient,
        CacheSelectionConfiguration selection,
        MigrationTransformer<MigrationCache> transformer,
        IgniteLogger log
    ) {
        srcIg = null;
        this.srcClient = srcClient;
        this.selection = selection;
        supportsDataStructures = false;
        this.transformer = transformer;
        this.log = log;
    }

    /** @return Selected and transformed cache definitions. */
    public List<MigrationCache> caches() {
        prepare();

        return caches;
    }

    /** @return Source to destination cache names. */
    public Map<String, String> cacheNames() {
        prepare();

        return cacheNames;
    }

    /** */
    private synchronized void prepare() {
        if (caches != null)
            return;

        Collection<MigrationCache> srcCaches = srcIg == null ? sourceCaches(srcClient) : sourceCaches(srcIg);
        List<MigrationCache> selected = new ArrayList<>();
        Map<String, String> names = new LinkedHashMap<>();
        Set<String> destNames = new HashSet<>();

        for (MigrationCache item : srcCaches) {
            if (!selection.includes(item.configuration())) {
                info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache group filter'].");

                continue;
            }

            if (item.type() == CacheType.DATA_STRUCTURES &&
                (!selection.moveDataStructures() || !supportsDataStructures)) {
                info("Skipped data structure cache [sourceName=" + item.sourceName() + ", reason='" +
                    (!selection.moveDataStructures() ? "move-data-structures=false" :
                        "destination does not support data structures") + "'].");

                continue;
            }

            String grpName = item.configuration().getGroupName();
            MigrationCache cache = transformer.transform(item);

            if (cache == null) {
                info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache transformer returned null'].");

                continue;
            }

            CacheConfiguration<?, ?> cacheCfg = selection.cacheConfigurationTransformer().apply(cache.configuration());

            if (cacheCfg == null) {
                info("Skipped cache [sourceName=" + item.sourceName() +
                    ", reason='cache configuration transformer returned null'].");

                continue;
            }

            if (item.type() == CacheType.DATA_STRUCTURES &&
                (!item.sourceName().equals(cacheCfg.getName()) || !Objects.equals(grpName, cacheCfg.getGroupName()))) {
                throw new IgniteException("Data structure cache and group names must be preserved: " +
                    item.sourceName() + '.');
            }

            if (cacheCfg.getName() == null || cacheCfg.getName().isEmpty() || !destNames.add(cacheCfg.getName()))
                throw new IgniteException("Invalid or duplicate transformed cache name: " + cacheCfg.getName() + '.');

            names.put(item.sourceName(), cacheCfg.getName());
            selected.add(new MigrationCache(item.sourceName(), cacheCfg, cache.sql(), item.type()));
        }

        cacheNames = Collections.unmodifiableMap(names);
        caches = Collections.unmodifiableList(selected);
    }

    /** Logs a cache omitted during selection or transformation. */
    private void info(String msg) {
        if (log != null && log.isInfoEnabled())
            log.info(msg);
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

    /**
     * Reads cache configuration through the thin-client API. Thin clients do not expose cache type, SQL-created
     * state, data-structure caches, or the full server-side cache configuration.
     */
    private static List<MigrationCache> sourceCaches(IgniteClient client) {
        List<MigrationCache> res = new ArrayList<>();

        try {
            for (String name : client.cacheNames()) {
                ClientCache<Object, Object> cache = client.<Object, Object>cache(name);
                ClientCacheConfiguration clientCfg = cache.getConfiguration();
                CacheConfiguration<?, ?> cacheCfg = ThinClientCacheConfigurationAdapter.toCacheConfiguration(clientCfg);

                // The thin API returns query entities but does not expose whether the cache was created by SQL DDL.
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
