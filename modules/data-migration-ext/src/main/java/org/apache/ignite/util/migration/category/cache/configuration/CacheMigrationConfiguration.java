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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Cache selection and name mapping shared by the cache-configuration and cache-data transferrers.
 */
public class CacheMigrationConfiguration {
    /** */
    private Set<String> cacheGrpNames = Collections.emptySet();

    /** */
    private boolean moveDataStructures = true;

    /** */
    private boolean noCacheCreate;

    /** */
    private boolean createCacheIfNotExists;

    /** */
    private boolean supportsDataStructures = true;

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

    /** Validates programmatic and Spring-configured cache selection values before connecting. */
    public void validate() {
        validatedNames(cacheGrpNames);
        Objects.requireNonNull(cacheCfgTransformer, "cache configuration transformer");

        if (noCacheCreate && createCacheIfNotExists)
            throw new IllegalArgumentException("no-cache-create and create-cache-if-not-exists are mutually exclusive.");
    }

    /**
     * @param names Source cache group names. An empty set selects all user caches.
     * @return {@code this} for chaining.
     */
    public CacheMigrationConfiguration cacheGroupNames(String... names) {
        cacheGrpNames = validatedNames(Arrays.asList(Objects.requireNonNull(names)));

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
    public CacheMigrationConfiguration moveDataStructures(boolean val) {
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
    public CacheMigrationConfiguration cacheConfigurationTransformer(
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
        cacheGrpNames = validatedNames(names);
    }

    /** @param names Source cache group names. An empty set selects all user caches. */
    public void setCacheGroupNames(List<String> names) {
        cacheGrpNames = validatedNames(names);
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

    /** @return Whether cache creation is disabled. */
    public boolean noCacheCreate() {
        return noCacheCreate;
    }

    /**
     * @param val Whether all selected destination caches must already exist.
     * @return {@code this} for chaining.
     */
    public CacheMigrationConfiguration noCacheCreate(boolean val) {
        noCacheCreate = val;

        return this;
    }

    /** @return Whether existing destination caches are accepted. */
    public boolean createCacheIfNotExists() {
        return createCacheIfNotExists;
    }

    /**
     * @param val Whether existing destination caches are accepted.
     * @return {@code this} for chaining.
     */
    public CacheMigrationConfiguration createCacheIfNotExists(boolean val) {
        createCacheIfNotExists = val;

        return this;
    }

    /** @return Whether cache creation is disabled. */
    public boolean isNoCacheCreate() {
        return noCacheCreate;
    }

    /** @param val Whether all selected destination caches must already exist. */
    public void setNoCacheCreate(boolean val) {
        noCacheCreate = val;
    }

    /** @return Whether existing destination caches are accepted. */
    public boolean isCreateCacheIfNotExists() {
        return createCacheIfNotExists;
    }

    /** @param val Whether existing destination caches are accepted. */
    public void setCreateCacheIfNotExists(boolean val) {
        createCacheIfNotExists = val;
    }

    /** @return Whether the destination supports data-structure caches. */
    public boolean supportsDataStructures() {
        return supportsDataStructures;
    }

    /**
     * @param val Whether the destination supports data-structure caches.
     * @return {@code this} for chaining.
     */
    public CacheMigrationConfiguration supportsDataStructures(boolean val) {
        supportsDataStructures = val;

        return this;
    }

    /** @return Whether the destination supports data-structure caches. */
    public boolean isSupportsDataStructures() {
        return supportsDataStructures;
    }

    /** @param val Whether the destination supports data-structure caches. */
    public void setSupportsDataStructures(boolean val) {
        supportsDataStructures = val;
    }

    /** Creates a validated, detached set of selected names. */
    private static Set<String> validatedNames(Collection<String> names) {
        Set<String> res = new HashSet<>(Objects.requireNonNull(names, "cache group names"));

        if (res.stream().anyMatch(name -> name == null || name.isEmpty()))
            throw new IllegalArgumentException("Cache group names must not be empty.");

        return res;
    }
}
