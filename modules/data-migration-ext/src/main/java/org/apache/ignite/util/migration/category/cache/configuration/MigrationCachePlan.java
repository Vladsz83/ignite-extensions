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
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.util.migration.MigrationTransformer;

/** Per-migration cache selection and name mapping. Prepared once on first use; performs no destination writes. */
public final class MigrationCachePlan {
    /** */
    private final Supplier<Collection<MigrationCache>> src;

    /** */
    private final CacheMigrationConfiguration mCfg;

    /** */
    private final MigrationTransformer<MigrationCache> transformer;

    /** */
    private final IgniteLogger log;

    /** Selected cache definitions; non-null once preparation has completed. */
    private List<MigrationCache> caches;

    /** */
    private Map<String, String> cacheNames;

    /**
     * @param src Lazy supplier of source cache definitions.
     * @param mCfg Cache migration settings.
     * @param transformer Cache selection and transformation callback.
     * @param log Logger.
     */
    public MigrationCachePlan(
        Supplier<Collection<MigrationCache>> src,
        CacheMigrationConfiguration mCfg,
        MigrationTransformer<MigrationCache> transformer,
        IgniteLogger log
    ) {
        this.src = src;
        this.mCfg = mCfg;
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

        List<MigrationCache> selected = new ArrayList<>();
        Map<String, String> names = new LinkedHashMap<>();
        Set<String> destNames = new HashSet<>();

        for (MigrationCache item : src.get()) {
            if (!mCfg.includes(item.configuration())) {
                info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache group filter'].");

                continue;
            }

            if (item.type() == CacheType.DATA_STRUCTURES && (!mCfg.moveDataStructures() || !mCfg.supportsDataStructures())) {
                info("Skipped data structure cache [sourceName=" + item.sourceName() + ", reason='" +
                    (!mCfg.moveDataStructures() ? "move-data-structures=false" :
                        "destination does not support data structures") + "'].");

                continue;
            }

            String grpName = item.configuration().getGroupName();
            MigrationCache cache = transformer.transform(item);

            if (cache == null) {
                info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache transformer returned null'].");

                continue;
            }

            CacheConfiguration<?, ?> cacheCfg = mCfg.cacheConfigurationTransformer().apply(cache.configuration());

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
}
