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

/** Destination cache creation policy. */
public class CacheConfigurationsMigrationConfiguration {
    /** */
    private boolean noCacheCreate;

    /** */
    private boolean createCacheIfNotExists;

    /** @return Whether cache creation is disabled. */
    public boolean noCacheCreate() {
        return noCacheCreate;
    }

    /**
     * @param val Whether all selected destination caches must already exist.
     * @return {@code this} for chaining.
     */
    public CacheConfigurationsMigrationConfiguration noCacheCreate(boolean val) {
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
    public CacheConfigurationsMigrationConfiguration createCacheIfNotExists(boolean val) {
        createCacheIfNotExists = val;

        return this;
    }

    /** Validates settings before a migration starts. */
    public void validate() {
        if (noCacheCreate && createCacheIfNotExists)
            throw new IllegalArgumentException("no-cache-create and create-cache-if-not-exists are mutually exclusive.");
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
}
