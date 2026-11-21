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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.Nullable;

/** Caches migration configuration. */
public class CacheMigrationConfiguration {
    /** */
    private Set<String> grpNames = Collections.emptySet();

    /** */
    private boolean moveDataStructures = true;

    /** */
    private boolean noCacheCreate;

    /** */
    private boolean createCacheIfNotExists;

    /** */
    private Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheCfgTransformer = Function.identity();

    /** Resolved at prepare() time with the connected destination node. */
    @Nullable
    private BiFunction<IgniteEx, CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheCfgBiTransformer;

    /**
     * @param cfg Source cache configuration.
     * @return Whether the source cache belongs to a selected group. Ungrouped caches use their cache name.
     */
    public boolean includes(CacheConfiguration<?, ?> cfg) {
        return grpNames.isEmpty() || grpNames.contains(cfg.getGroupName() == null ?
            cfg.getName() : cfg.getGroupName());
    }

    /** Validates programmatic and Spring-configured cache selection values before connecting. */
    public void validate() {
        validatedNames(grpNames);
        Objects.requireNonNull(cacheCfgTransformer, "cache configuration transformer");

        if (noCacheCreate && createCacheIfNotExists)
            throw new IllegalArgumentException("no-cache-create and create-cache-if-not-exists are mutually exclusive.");
    }

    /** @return Whether data structure caches are included. */
    public boolean moveDataStructures() {
        return moveDataStructures;
    }

    /** @return Cache configuration transformer. */
    public Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheConfigurationTransformer() {
        return cacheCfgTransformer;
    }

    /** @return Cache configuration BiFunction (receives the destination IgniteEx at prepare time). */
    @Nullable
    public BiFunction<IgniteEx, CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheConfigurationBiTransformer() {
        return cacheCfgBiTransformer;
    }

    /** @return Source cache group names. */
    public Set<String> cacheGroupNames() {
        return grpNames;
    }

    /** @return Whether cache creation is disabled. */
    public boolean noCacheCreate() {
        return noCacheCreate;
    }

    /** @return Whether existing destination caches are accepted. */
    public boolean createCacheIfNotExists() {
        return createCacheIfNotExists;
    }

    /** @param names Source cache group names. */
    public void setCacheGroupNames(List<String> names) {
        grpNames = validatedNames(names);
    }

    /** @param val Whether to include data structure caches. */
    public void setMoveDataStructures(boolean val) {
        moveDataStructures = val;
    }

    /** @param val Whether all selected destination caches must already exist. */
    public void setNoCacheCreate(boolean val) {
        noCacheCreate = val;
    }

    /** @param val Whether existing destination caches are accepted. */
    public void setCreateCacheIfNotExists(boolean val) {
        createCacheIfNotExists = val;
    }

    /** @param transformer Cache configuration transformer. */
    public void setCacheConfigurationTransformer(Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> transformer) {
        cacheCfgTransformer = Objects.requireNonNull(transformer);
    }

    /** @param transformer Cache configuration BiFunction receiving the connected destination. */
    public void setCacheConfigurationBiTransformer(
        BiFunction<IgniteEx, CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> transformer
    ) {
        cacheCfgBiTransformer = Objects.requireNonNull(transformer);
    }

    /**
     * Selects and transforms source caches according to this configuration.
     *
     * @param src Lazy supplier of source cache definitions.
     * @param dest Connected destination node, passed to the BiFunction transformer if set.
     * @param destSupportsDs Whether the destination supports data-structure caches.
     * @param transformer Cache selection and transformation callback.
     * @param log Logger.
     * @return Selected and transformed cache definitions.
     */
    public List<MigrationCache> prepare(
        Supplier<Collection<MigrationCache>> src,
        @Nullable IgniteEx dest,
        boolean destSupportsDs,
        MigrationTransformer<MigrationCache> transformer,
        IgniteLogger log
    ) {
        List<MigrationCache> sel = new ArrayList<>();
        Set<String> dstNames = new HashSet<>();

        for (MigrationCache item : src.get()) {
            if (!includes(item.configuration())) {
                if (log.isInfoEnabled()) {
                    log.info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache group filter'].");
                }

                continue;
            }

            if (item.type() == CacheType.DATA_STRUCTURES && (!moveDataStructures() || !destSupportsDs)) {
                if (log.isInfoEnabled()) {
                    log.info("Skipped data structure cache [sourceName=" + item.sourceName() + ", reason='" +
                        (!moveDataStructures() ? "move-data-structures=false" :
                            "destination does not support data structures") + "'].");
                }

                continue;
            }

            String grpName = item.configuration().getGroupName();
            MigrationCache cache = transformer.transform(item);

            if (cache == null) {
                if (log.isInfoEnabled()) {
                    log.info("Skipped cache [sourceName=" + item.sourceName() + ", reason='cache transformer returned null'].");
                }

                continue;
            }

            CacheConfiguration<?, ?> cacheCfg = cacheCfgBiTransformer != null && dest != null ?
                cacheCfgBiTransformer.apply(dest, cache.configuration()) : cacheCfgTransformer.apply(cache.configuration());

            if (cacheCfg == null) {
                if (log.isInfoEnabled()) {
                    log.info("Skipped cache [sourceName=" + item.sourceName() +
                        ", reason='cache configuration transformer returned null'].");
                }

                continue;
            }

            if (item.type() == CacheType.DATA_STRUCTURES &&
                (!item.sourceName().equals(cacheCfg.getName()) || !Objects.equals(grpName, cacheCfg.getGroupName())))
                throw new IgniteException("Data structure cache and group names must be preserved: " + item.sourceName() + '.');

            if (cacheCfg.getName() == null || cacheCfg.getName().isEmpty() || !dstNames.add(cacheCfg.getName()))
                throw new IgniteException("Invalid or duplicate transformed cache name: " + cacheCfg.getName() + '.');

            sel.add(new MigrationCache(item.sourceName(), cacheCfg, cache.sql(), item.type()));
        }

        return Collections.unmodifiableList(sel);
    }

    /** Creates a validated, detached set of selected names. */
    private static Set<String> validatedNames(Collection<String> names) {
        Set<String> res = new HashSet<>(Objects.requireNonNull(names, "cache group names"));

        if (res.stream().anyMatch(name -> name == null || name.isEmpty()))
            throw new IllegalArgumentException("Cache group names must not be empty.");

        return res;
    }
}
