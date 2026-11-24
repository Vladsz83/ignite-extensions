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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.configuration.CacheConfiguration;

/** Caches migration configuration. */
public class CacheMigrationConfiguration {
    /** */
    private Set<String> grpNames = Collections.emptySet();

    /** */
    private boolean moveDataStructures = true;

    /** */
    private boolean createCacheIfNotExists;

    /** */
    private Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheCfgTransformer = Function.identity();

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
    }

    /** @return Whether data structure caches are included. */
    public boolean moveDataStructures() {
        return moveDataStructures;
    }

    /** @return Cache configuration transformer. */
    public Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheConfigurationTransformer() {
        return cacheCfgTransformer;
    }

    /** @return Source cache group names. */
    public Set<String> cacheGroupNames() {
        return grpNames;
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

    /** @param val Whether existing destination caches are accepted. */
    public void setCreateCacheIfNotExists(boolean val) {
        createCacheIfNotExists = val;
    }

    /** @param transformer Cache configuration transformer. */
    public void setCacheConfigurationTransformer(Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> transformer) {
        cacheCfgTransformer = Objects.requireNonNull(transformer);
    }

    /** Creates a validated, detached set of selected names. */
    private static Set<String> validatedNames(Collection<String> names) {
        Set<String> res = new HashSet<>(Objects.requireNonNull(names, "cache group names"));

        if (res.stream().anyMatch(name -> name == null || name.isEmpty()))
            throw new IllegalArgumentException("Cache group names must not be empty.");

        return res;
    }
}
