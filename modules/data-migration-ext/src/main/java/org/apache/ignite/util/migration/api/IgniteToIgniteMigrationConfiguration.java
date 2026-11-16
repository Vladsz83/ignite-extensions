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
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
    private boolean skipCdcInfo;

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
    private final Map<MigrationChannelType, MigrationTransformer<?>> transformers = new EnumMap<>(MigrationChannelType.class);

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

    /** @return Whether a source cache belongs to a selected group. Ungrouped caches use their cache name. */
    public boolean includes(CacheConfiguration<?, ?> cfg) {
        return cacheGrpNames.isEmpty() || cacheGrpNames.contains(cfg.getGroupName() == null ?
            cfg.getName() : cfg.getGroupName());
    }

    /** Sets source cache group names. An empty set selects all user caches. */
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

    /** Sets whether all selected destination caches must already exist. */
    public IgniteToIgniteMigrationConfiguration setNoCacheCreate(boolean val) {
        noCacheCreate = val;
        return this;
    }

    /** @return Whether existing destination caches are accepted. */
    public boolean isCreateCacheIfNotExists() {
        return createCacheIfNotExists;
    }

    /** Sets whether existing destination caches are accepted. */
    public IgniteToIgniteMigrationConfiguration setCreateCacheIfNotExists(boolean val) {
        createCacheIfNotExists = val;
        return this;
    }

    /** @return Whether explicit binary schema registration is disabled. */
    public boolean isNoBinaryMetadataCreate() {
        return noBinaryMetadataCreate;
    }

    /** Disables explicit binary schema registration. */
    public IgniteToIgniteMigrationConfiguration setNoBinaryMetadataCreate(boolean val) {
        noBinaryMetadataCreate = val;
        return this;
    }

    /** @return Whether explicit mapping registration is disabled. */
    public boolean isNoMappingsCreate() {
        return noMappingsCreate;
    }

    /** Disables explicit mapping registration. */
    public IgniteToIgniteMigrationConfiguration setNoMappingsCreate(boolean val) {
        noMappingsCreate = val;
        return this;
    }

    /** @return Whether source expiry is ignored. */
    public boolean isSkipExpireTime() {
        return skipExpireTime;
    }

    /** Sets whether source expiry is ignored. */
    public IgniteToIgniteMigrationConfiguration setSkipExpireTime(boolean val) {
        skipExpireTime = val;
        return this;
    }

    /** @return Whether source conflict versions are ignored. */
    public boolean isSkipCdcInfo() {
        return skipCdcInfo;
    }

    /** Sets whether ordinary writes replace conflict-aware writes. */
    public IgniteToIgniteMigrationConfiguration setSkipCdcInfo(boolean val) {
        skipCdcInfo = val;
        return this;
    }

    /** @return Maximum entries per batch. */
    public int getBatchSize() {
        return batchSize;
    }

    /** Sets maximum entries per batch. */
    public IgniteToIgniteMigrationConfiguration setBatchSize(int val) {
        batchSize = val;
        return this;
    }

    /** @return Reader count. */
    public int getReadThreads() {
        return readThreads;
    }

    /** Sets reader count. */
    public IgniteToIgniteMigrationConfiguration setReadThreads(int val) {
        readThreads = val;
        return this;
    }

    /** @return Writer count. */
    public int getWriteThreads() {
        return writeThreads;
    }

    /** Sets writer count. */
    public IgniteToIgniteMigrationConfiguration setWriteThreads(int val) {
        writeThreads = val;
        return this;
    }

    /** @return Maximum queued batches. */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /** Sets maximum queued batches (backpressure). */
    public IgniteToIgniteMigrationConfiguration setQueueCapacity(int val) {
        queueCapacity = val;
        return this;
    }

    /** @return Global read entries per second, or zero. */
    public int getReadLimitInSec() {
        return readLimit;
    }

    /** Sets global read entries per second. */
    public IgniteToIgniteMigrationConfiguration setReadLimitInSec(int val) {
        readLimit = val;
        return this;
    }

    /** @return Global write entries per second, or zero. */
    public int getWriteLimitInSec() {
        return writeLimit;
    }

    /** Sets global write entries per second. */
    public IgniteToIgniteMigrationConfiguration setWriteLimitInSec(int val) {
        writeLimit = val;
        return this;
    }

    /** @return Cache configuration transformer. */
    public Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> getCacheConfigurationTransformer() {
        return cacheCfgTransformer;
    }

    /** Sets a cache transformer; returning null excludes the cache and its data. */
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

    /** Sets the distributed metadata allowlist. Local authentication/encryption storage is never enumerated. */
    public IgniteToIgniteMigrationConfiguration setMetastorageFilter(Predicate<String> filter) {
        metastorageFilter = Objects.requireNonNull(filter);
        return this;
    }

    /** Sets a channel transformer; see the endpoint methods for the corresponding item type. */
    public <T> IgniteToIgniteMigrationConfiguration setTransformer(MigrationChannelType channel, MigrationTransformer<T> transformer) {
        transformers.put(channel, Objects.requireNonNull(transformer));
        return this;
    }

    /** Applies a configured channel transformer. */
    @SuppressWarnings("unchecked")
    public <T> T transform(MigrationChannelType channel, T item) {
        MigrationTransformer<T> transformer = (MigrationTransformer<T>)transformers.get(channel);
        return transformer == null ? item : transformer.transform(item);
    }

    /** @return Migration logger. */
    public IgniteLogger getLogger() {
        return log;
    }

    /** Sets the logger shared by all migration workers. */
    public IgniteToIgniteMigrationConfiguration setLogger(IgniteLogger log) {
        this.log = Objects.requireNonNull(log);
        return this;
    }

}
