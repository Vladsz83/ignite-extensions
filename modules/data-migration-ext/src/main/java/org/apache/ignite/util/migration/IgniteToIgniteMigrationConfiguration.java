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

package org.apache.ignite.util.migration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationsMigrationConfiguration;
import org.apache.ignite.util.migration.category.cache.configuration.CacheSelectionConfiguration;
import org.apache.ignite.util.migration.category.cache.data.CacheDataMigrationConfiguration;
import org.apache.ignite.util.migration.category.metastorage.MetastorageMigrationConfiguration;

/**
 * Migration settings. Aggregates shared settings and per-channel sub-configurations; do not mutate while a
 * migration is running.
 */
public class IgniteToIgniteMigrationConfiguration {
    /** Shared cache selection and name mapping. */
    private final CacheSelectionConfiguration cacheSelection = new CacheSelectionConfiguration();

    /** */
    private final CacheConfigurationsMigrationConfiguration cacheConfigurations = new CacheConfigurationsMigrationConfiguration();

    /** */
    private final CacheDataMigrationConfiguration cacheData = new CacheDataMigrationConfiguration();

    /** */
    private final MetastorageMigrationConfiguration metastorage = new MetastorageMigrationConfiguration();

    /** Thread count of the shared executor used by all channels. */
    private int migrationThreads = 4;

    /** */
    private boolean noBinaryMetadataCreate;

    /** */
    private boolean noMappingsCreate;

    /** */
    private final Map<Class<? extends MigrationChannel<?>>, MigrationTransformer<?>> transformers = new HashMap<>();

    /** */
    private IgniteLogger log = new JavaLogger();

    /**
     * Validates shared and all per-channel settings before either connection is opened.
     */
    public void validate() {
        positive(migrationThreads, "migration-threads");

        cacheConfigurations.validate();
        cacheData.validate();
    }

    /** */
    private static void positive(int val, String name) {
        if (val <= 0)
            throw new IllegalArgumentException(name + " must be positive.");
    }

    /** @return Shared cache selection and name mapping. */
    public CacheSelectionConfiguration cacheSelection() {
        return cacheSelection;
    }

    /** @return Destination cache creation policy. */
    public CacheConfigurationsMigrationConfiguration cacheConfigurations() {
        return cacheConfigurations;
    }

    /** @return Data transfer knobs. */
    public CacheDataMigrationConfiguration cacheData() {
        return cacheData;
    }

    /** @return Metastorage transfer settings. */
    public MetastorageMigrationConfiguration metastorage() {
        return metastorage;
    }

    /** @return Thread count of the shared executor used by all channels. */
    public int migrationThreads() {
        return migrationThreads;
    }

    /**
     * @param val Thread count of the shared executor used by all channels.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration migrationThreads(int val) {
        migrationThreads = val;

        return this;
    }

    /** @return Whether explicit binary schema registration is disabled. */
    public boolean noBinaryMetadataCreate() {
        return noBinaryMetadataCreate;
    }

    /**
     * @param val Whether to skip explicit binary metadata registration.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration noBinaryMetadataCreate(boolean val) {
        noBinaryMetadataCreate = val;

        return this;
    }

    /** @return Whether explicit mapping registration is disabled. */
    public boolean noMappingsCreate() {
        return noMappingsCreate;
    }

    /**
     * @param val Whether to skip explicit type mapping registration.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration noMappingsCreate(boolean val) {
        noMappingsCreate = val;

        return this;
    }

    /**
     * @param <T> Channel item type.
     * @param channel Migration channel class.
     * @param transformer Channel transformer. Returning {@code null} skips an item.
     * @return {@code this} for chaining.
     */
    public <T> IgniteToIgniteMigrationConfiguration transformer(
        Class<? extends MigrationChannel<T>> channel,
        MigrationTransformer<T> transformer
    ) {
        transformers.put(channel, Objects.requireNonNull(transformer));

        return this;
    }

    /**
     * @param <T> Channel item type.
     * @param channel Migration channel class.
     * @return Configured transformer, or an identity transformer if none is configured.
     */
    @SuppressWarnings("unchecked")
    public <T> MigrationTransformer<T> transformer(Class<? extends MigrationChannel<T>> channel) {
        MigrationTransformer<T> transformer = (MigrationTransformer<T>)transformers.get(channel);

        return transformer == null ? item -> item : transformer;
    }

    /** @return Migration logger. */
    public IgniteLogger logger() {
        return log;
    }

    /**
     * @param log Migration logger.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration logger(IgniteLogger log) {
        this.log = Objects.requireNonNull(log);

        return this;
    }

    /** @return Shared cache selection and name mapping. */
    public CacheSelectionConfiguration getCacheSelection() {
        return cacheSelection;
    }

    /** @return Destination cache creation policy. */
    public CacheConfigurationsMigrationConfiguration getCacheConfigurations() {
        return cacheConfigurations;
    }

    /** @return Data transfer knobs. */
    public CacheDataMigrationConfiguration getCacheData() {
        return cacheData;
    }

    /** @return Metastorage transfer settings. */
    public MetastorageMigrationConfiguration getMetastorage() {
        return metastorage;
    }

    /** @return Thread count of the shared executor. */
    public int getMigrationThreads() {
        return migrationThreads;
    }

    /** @param val Thread count of the shared executor. */
    public void setMigrationThreads(int val) {
        migrationThreads = val;
    }

    /** @return Whether binary metadata registration is disabled. */
    public boolean isNoBinaryMetadataCreate() {
        return noBinaryMetadataCreate;
    }

    /** @param val Whether to skip explicit binary metadata registration. */
    public void setNoBinaryMetadataCreate(boolean val) {
        noBinaryMetadataCreate = val;
    }

    /** @return Whether type mapping registration is disabled. */
    public boolean isNoMappingsCreate() {
        return noMappingsCreate;
    }

    /** @param val Whether to skip explicit type mapping registration. */
    public void setNoMappingsCreate(boolean val) {
        noMappingsCreate = val;
    }
}
