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

package org.apache.ignite.util.migration.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.logger.java.JavaLogger;

/** Migration settings. Configure before starting; do not mutate while a migration is running. */
public class IgniteToIgniteMigrationConfiguration {
    /** */
    private Set<String> cacheGrpNames = Collections.emptySet();

    /** */
    private boolean noCacheCreate;

    /** */
    private boolean createCacheIfNotExists;

    /** */
    private boolean noBinaryMetadataCreate;

    /** */
    private boolean noMappingsCreate;

    /** */
    private boolean skipExpireTime;

    /** */
    private boolean skipConflictVersion;

    /** */
    private boolean moveDataStructures = true;

    /** */
    private boolean useDataStreamer;

    /** */
    private Consumer<IgniteDataStreamer<Object, Object>> streamerCfg = streamer -> { /* No-op. */ };

    /** */
    private int batchSize = 1;

    /** */
    private int readThreads = 1;

    /** */
    private int writeThreads = 1;

    /** Number of queued batches, independent of the number of source partitions. */
    private int queueCapacity = 64;

    /** Zero means unlimited. */
    private int readLimit;

    /** Zero means unlimited. */
    private int writeLimit;

    /** */
    private Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> cacheCfgTransformer = Function.identity();

    /** Only portable SQL settings and views are transferred by default. */
    private Predicate<String> metastorageFilter = key -> key.startsWith("ignite.internal.sql.view.") ||
        key.startsWith("distrConf-sql.");

    /** */
    private final Map<Class<? extends MigrationChannel<?>>, MigrationTransformer<?>> transformers = new HashMap<>();

    /** */
    private IgniteLogger log = new JavaLogger();

    /** Validates settings before either connection is opened. */
    public void validate() {
        positive(batchSize, "batch-size");
        positive(readThreads, "read-threads");
        positive(writeThreads, "write-threads");
        positive(queueCapacity, "queue-capacity");

        if (readLimit < 0 || writeLimit < 0)
            throw new IllegalArgumentException("Read and write rate limits must be non-negative");

        if (noCacheCreate && createCacheIfNotExists)
            throw new IllegalArgumentException("no-cache-create and create-cache-if-not-exists are mutually exclusive");
    }

    /** */
    private static void positive(int val, String name) {
        if (val <= 0)
            throw new IllegalArgumentException(name + " must be positive");
    }

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
    public IgniteToIgniteMigrationConfiguration setCacheGroupNames(String... names) {
        cacheGrpNames = new HashSet<>(Arrays.asList(names));

        if (cacheGrpNames.stream().anyMatch(name -> name == null || name.isEmpty()))
            throw new IllegalArgumentException("Cache group names must not be empty");

        return this;
    }

    /** @return Whether cache creation is disabled. */
    public boolean isNoCacheCreate() {
        return noCacheCreate;
    }

    /**
     * @param val Whether all selected destination caches must already exist.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setNoCacheCreate(boolean val) {
        noCacheCreate = val;

        return this;
    }

    /** @return Whether existing destination caches are accepted. */
    public boolean isCreateCacheIfNotExists() {
        return createCacheIfNotExists;
    }

    /**
     * @param val Whether existing destination caches are accepted.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setCreateCacheIfNotExists(boolean val) {
        createCacheIfNotExists = val;

        return this;
    }

    /** @return Whether explicit binary schema registration is disabled. */
    public boolean isNoBinaryMetadataCreate() {
        return noBinaryMetadataCreate;
    }

    /**
     * @param val Whether to skip explicit binary metadata registration.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setNoBinaryMetadataCreate(boolean val) {
        noBinaryMetadataCreate = val;

        return this;
    }

    /** @return Whether explicit mapping registration is disabled. */
    public boolean isNoMappingsCreate() {
        return noMappingsCreate;
    }

    /**
     * @param val Whether to skip explicit type mapping registration.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setNoMappingsCreate(boolean val) {
        noMappingsCreate = val;

        return this;
    }

    /** @return Whether source expiry is ignored. */
    public boolean isSkipExpireTime() {
        return skipExpireTime;
    }

    /**
     * @param val Whether source expiration timestamps are ignored.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setSkipExpireTime(boolean val) {
        skipExpireTime = val;

        return this;
    }

    /** @return Whether source conflict versions are ignored. */
    public boolean isSkipConflictVersion() {
        return skipConflictVersion;
    }

    /**
     * @param val Whether source conflict versions are ignored.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setSkipConflictVersion(boolean val) {
        skipConflictVersion = val;

        return this;
    }

    /** @return Whether data structure caches are included. */
    public boolean isMoveDataStructures() {
        return moveDataStructures;
    }

    /**
     * @param val Whether to include data structure caches.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setMoveDataStructures(boolean val) {
        moveDataStructures = val;

        return this;
    }

    /** @return Whether to load data using a data streamer. */
    public boolean isUseDataStreamer() {
        return useDataStreamer;
    }

    /**
     * @param val Whether to load data using a data streamer.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setUseDataStreamer(boolean val) {
        useDataStreamer = val;

        return this;
    }

    /** @return Data streamer configuration callback. */
    public Consumer<IgniteDataStreamer<Object, Object>> getDataStreamerConfiguration() {
        return streamerCfg;
    }

    /**
     * @param cfg Data streamer configuration callback, invoked once per cache before adding data.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setDataStreamerConfiguration(
        Consumer<IgniteDataStreamer<Object, Object>> cfg
    ) {
        streamerCfg = Objects.requireNonNull(cfg);

        return this;
    }

    /** @return Maximum entries per batch. */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @param val Maximum number of entries per batch.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setBatchSize(int val) {
        batchSize = val;

        return this;
    }

    /** @return Reader count. */
    public int getReadThreads() {
        return readThreads;
    }

    /**
     * @param val Number of reader threads.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setReadThreads(int val) {
        readThreads = val;

        return this;
    }

    /** @return Writer count. */
    public int getWriteThreads() {
        return writeThreads;
    }

    /**
     * @param val Number of writer threads.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setWriteThreads(int val) {
        writeThreads = val;

        return this;
    }

    /** @return Maximum queued batches. */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * @param val Maximum number of queued batches.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setQueueCapacity(int val) {
        queueCapacity = val;

        return this;
    }

    /** @return Global read entries per second, or zero. */
    public int getReadLimitInSec() {
        return readLimit;
    }

    /**
     * @param val Global read rate in entries per second, or zero for unlimited.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setReadLimitInSec(int val) {
        readLimit = val;

        return this;
    }

    /** @return Global write entries per second, or zero. */
    public int getWriteLimitInSec() {
        return writeLimit;
    }

    /**
     * @param val Global write rate in entries per second, or zero for unlimited.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setWriteLimitInSec(int val) {
        writeLimit = val;

        return this;
    }

    /** @return Cache configuration transformer. */
    public Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> getCacheConfigurationTransformer() {
        return cacheCfgTransformer;
    }

    /**
     * @param transformer Cache configuration transformer. Returning {@code null} excludes the cache and its data.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setCacheConfigurationTransformer(
        Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> transformer
    ) {
        cacheCfgTransformer = Objects.requireNonNull(transformer);

        return this;
    }

    /** @return Distributed metadata key filter. */
    public Predicate<String> getMetastorageFilter() {
        return metastorageFilter;
    }

    /**
     * @param filter Distributed metadata key filter. Local metastorage is never enumerated.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setMetastorageFilter(Predicate<String> filter) {
        metastorageFilter = Objects.requireNonNull(filter);

        return this;
    }

    /**
     * @param <T> Channel item type.
     * @param channel Migration channel class.
     * @param transformer Channel transformer. Returning {@code null} skips an item.
     * @return {@code this} for chaining.
     */
    public <T> IgniteToIgniteMigrationConfiguration setTransformer(
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
    public <T> MigrationTransformer<T> getTransformer(Class<? extends MigrationChannel<T>> channel) {
        MigrationTransformer<T> transformer = (MigrationTransformer<T>)transformers.get(channel);

        return transformer == null ? item -> item : transformer;
    }

    /**
     * @param <T> Channel item type.
     * @param channel Migration channel.
     * @param item Channel item.
     * @return Transformed item, or {@code null} to skip it.
     */
    @SuppressWarnings("unchecked")
    public <T> T transform(MigrationChannel<T> channel, T item) {
        MigrationTransformer<T> transformer = (MigrationTransformer<T>)transformers.get(channel.getClass());

        return transformer == null ? item : transformer.transform(item);
    }

    /** @return Migration logger. */
    public IgniteLogger getLogger() {
        return log;
    }

    /**
     * @param log Migration logger.
     * @return {@code this} for chaining.
     */
    public IgniteToIgniteMigrationConfiguration setLogger(IgniteLogger log) {
        this.log = Objects.requireNonNull(log);

        return this;
    }
}
