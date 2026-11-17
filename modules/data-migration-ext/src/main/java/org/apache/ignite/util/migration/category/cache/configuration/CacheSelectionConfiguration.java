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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Cache selection and name mapping shared by the cache-configuration and cache-data channels.
 */
public class CacheSelectionConfiguration {
    /** */
    private Set<String> cacheGrpNames = Collections.emptySet();

    /** */
    private boolean moveDataStructures = true;

    /** */
    private Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheCfgTransformer = Function.identity();

    /**
     * @param cfg Source cache configuration.
     * @return Whether the source cache belongs to a selected group. Ungrouped caches use their cache name.
     */
    public boolean includes(CacheConfiguration<?, ?> cfg) {
        return cacheGrpNames.isEmpty() || cacheGrpNames.contains(cfg.getGroupName() == null ?
            cfg.getName() : cfg.getGroupName());
    }

    /**
     * @param names Source cache group names. An empty set selects all user caches.
     * @return {@code this} for chaining.
     */
    public CacheSelectionConfiguration cacheGroupNames(String... names) {
        cacheGrpNames = new HashSet<>(Arrays.asList(names));

        if (cacheGrpNames.stream().anyMatch(name -> name == null || name.isEmpty()))
            throw new IllegalArgumentException("Cache group names must not be empty.");

        return this;
    }

    /** @return Whether data structure caches are included. */
    public boolean moveDataStructures() {
        return moveDataStructures;
    }

    /**
     * @param val Whether to include data structure caches.
     * @return {@code this} for chaining.
     */
    public CacheSelectionConfiguration moveDataStructures(boolean val) {
        moveDataStructures = val;

        return this;
    }

    /** @return Cache configuration transformer. */
    public Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheConfigurationTransformer() {
        return cacheCfgTransformer;
    }

    /**
     * @param transformer Cache configuration transformer. Returning {@code null} excludes the cache and its data.
     * @return {@code this} for chaining.
     */
    public CacheSelectionConfiguration cacheConfigurationTransformer(
        Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> transformer
    ) {
        cacheCfgTransformer = Objects.requireNonNull(transformer);

        return this;
    }

    /** @return Source cache group names. */
    public Set<String> getCacheGroupNames() {
        return cacheGrpNames;
    }

    /** @param names Source cache group names. An empty set selects all user caches. */
    public void setCacheGroupNames(Set<String> names) {
        cacheGrpNames = names;
    }

    /** @param names Source cache group names. An empty set selects all user caches. */
    public void setCacheGroupNames(List<String> names) {
        cacheGrpNames = new HashSet<>(names);
    }

    /** @return Whether data structure caches are included. */
    public boolean isMoveDataStructures() {
        return moveDataStructures;
    }

    /** @param val Whether to include data structure caches. */
    public void setMoveDataStructures(boolean val) {
        moveDataStructures = val;
    }

    /** @return Cache configuration transformer. */
    public Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> getCacheConfigurationTransformer() {
        return cacheCfgTransformer;
    }

    /**
     * @param transformer Cache configuration transformer. Returning {@code null} excludes the cache and its data.
     */
    public void setCacheConfigurationTransformer(
        Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> transformer
    ) {
        cacheCfgTransformer = Objects.requireNonNull(transformer);
    }
}
