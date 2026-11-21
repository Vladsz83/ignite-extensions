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

package org.apache.ignite.util.migration.category.cache.data;

import java.util.Objects;
import java.util.function.Consumer;
import org.apache.ignite.IgniteDataStreamer;

/** Data transfer knobs for the cache-data transferrer. */
public class CacheDataMigrationConfiguration {
    /** Default maximum number of entries per batch. */
    public static final int DFLT_BATCH_SIZE = 256;

    /** Default number of queued batches. */
    public static final int DFLT_QUEUE_CAPACITY = 64;

    /** Default cache-data reader worker count. */
    public static final int DFLT_READ_THREADS = 1;

    /** Default cache-data writer worker count. */
    public static final int DFLT_WRITE_THREADS = 1;

    /** */
    private int batchSize = DFLT_BATCH_SIZE;

    /** Cache-data reader worker count. */
    private int readThreads = DFLT_READ_THREADS;

    /** Whether the reader count was explicitly configured. */
    private boolean readThreadsConfigured;

    /** Cache-data writer worker count. */
    private int writeThreads = DFLT_WRITE_THREADS;

    /** Whether the writer count was explicitly configured. */
    private boolean writeThreadsConfigured;

    /** Number of queued batches, independent of the number of source partitions. */
    private int queueCapacity = DFLT_QUEUE_CAPACITY;

    /** Zero means unlimited. */
    private int readLimit;

    /** Zero means unlimited. */
    private int writeLimit;

    /** */
    private boolean skipExpireTime;

    /** */
    private boolean skipConflictVersion;

    /** */
    private boolean useDataStreamer;

    /** */
    private Consumer<IgniteDataStreamer<Object, Object>> streamerCfg = streamer -> { /* No-op. */ };

    /** Human-readable description of the configured DataStreamer settings for startup logging. */
    private String streamerCfgDescription = "Ignite defaults";

    /** Validates settings before a migration starts. */
    public void validate() {
        positive(batchSize, "batch-size");
        positive(readThreads, "read-threads");
        positive(writeThreads, "write-threads");
        positive(queueCapacity, "queue-capacity");

        if (readLimit < 0 || writeLimit < 0)
            throw new IllegalArgumentException("Read and write rate limits must be non-negative.");
    }

    /** */
    private static void positive(int val, String name) {
        if (val <= 0)
            throw new IllegalArgumentException(name + " must be positive.");
    }

    /** @return Maximum entries per batch. */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @param val Maximum number of entries per batch.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration batchSize(int val) {
        batchSize = val;

        return this;
    }

    /** @return Maximum entries per batch. */
    public int getBatchSize() {
        return batchSize;
    }

    /** @param val Maximum entries per batch. */
    public void setBatchSize(int val) {
        batchSize = val;
    }

    /** @return Reader worker count. */
    public int readThreads() {
        return readThreads;
    }

    /**
     * @param val Positive number of reader threads.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration readThreads(int val) {
        positive(val, "read-threads");
        readThreads = val;
        readThreadsConfigured = true;

        return this;
    }

    /** @return Reader worker count. */
    public int getReadThreads() {
        return readThreads;
    }

    /** @param val Positive number of reader threads. */
    public void setReadThreads(int val) {
        positive(val, "read-threads");
        readThreads = val;
        readThreadsConfigured = true;
    }

    /** @return Writer worker count. */
    public int writeThreads() {
        return writeThreads;
    }

    /**
     * @param val Positive number of writer threads.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration writeThreads(int val) {
        positive(val, "write-threads");
        writeThreads = val;
        writeThreadsConfigured = true;

        return this;
    }

    /** @return Writer worker count. */
    public int getWriteThreads() {
        return writeThreads;
    }

    /** @param val Positive number of writer threads. */
    public void setWriteThreads(int val) {
        positive(val, "write-threads");
        writeThreads = val;
        writeThreadsConfigured = true;
    }

    /** @return Maximum queued batches. */
    public int queueCapacity() {
        return queueCapacity;
    }

    /**
     * @param val Maximum number of queued batches.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration queueCapacity(int val) {
        queueCapacity = val;

        return this;
    }

    /** @return Maximum queued batches. */
    public int getQueueCapacity() {
        return queueCapacity;
    }

    /** @param val Maximum queued batches. */
    public void setQueueCapacity(int val) {
        queueCapacity = val;
    }

    /** @return Whether read threads have an explicit dedicated-pool override. */
    public boolean isReadThreadsConfigured() {
        return readThreadsConfigured;
    }

    /** @return Whether write threads have an explicit dedicated-pool override. */
    public boolean isWriteThreadsConfigured() {
        return writeThreadsConfigured;
    }

    /** @return Global read entries per second, or zero. */
    public int readLimitInSec() {
        return readLimit;
    }

    /**
     * @param val Global read rate in entries per second, or zero for unlimited.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration readLimitInSec(int val) {
        readLimit = val;

        return this;
    }

    /** @return Global read entries per second, or zero. */
    public int getReadLimitInSec() {
        return readLimit;
    }

    /** @param val Global read rate in entries per second, or zero for unlimited. */
    public void setReadLimitInSec(int val) {
        readLimit = val;
    }

    /** @return Global write entries per second, or zero. */
    public int writeLimitInSec() {
        return writeLimit;
    }

    /**
     * @param val Global write rate in entries per second, or zero for unlimited.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration writeLimitInSec(int val) {
        writeLimit = val;

        return this;
    }

    /** @return Global write entries per second, or zero. */
    public int getWriteLimitInSec() {
        return writeLimit;
    }

    /** @param val Global write rate in entries per second, or zero for unlimited. */
    public void setWriteLimitInSec(int val) {
        writeLimit = val;
    }

    /** @return Whether source expiry is ignored. */
    public boolean skipExpireTime() {
        return skipExpireTime;
    }

    /**
     * @param val Whether source expiration timestamps are ignored.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration skipExpireTime(boolean val) {
        skipExpireTime = val;

        return this;
    }

    /** @return Whether source expiry is ignored. */
    public boolean isSkipExpireTime() {
        return skipExpireTime;
    }

    /** @param val Whether source expiration timestamps are ignored. */
    public void setSkipExpireTime(boolean val) {
        skipExpireTime = val;
    }

    /** @return Whether source conflict versions are ignored. */
    public boolean skipConflictVersion() {
        return skipConflictVersion;
    }

    /**
     * @param val Whether source conflict versions are ignored.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration skipConflictVersion(boolean val) {
        skipConflictVersion = val;

        return this;
    }

    /** @return Whether source conflict versions are ignored. */
    public boolean isSkipConflictVersion() {
        return skipConflictVersion;
    }

    /** @param val Whether source conflict versions are ignored. */
    public void setSkipConflictVersion(boolean val) {
        skipConflictVersion = val;
    }

    /** @return Whether to load data using a data streamer. */
    public boolean useDataStreamer() {
        return useDataStreamer;
    }

    /**
     * @param val Whether to load data using a data streamer.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration useDataStreamer(boolean val) {
        useDataStreamer = val;

        return this;
    }

    /** @return Whether to load data using a data streamer. */
    public boolean isUseDataStreamer() {
        return useDataStreamer;
    }

    /** @param val Whether to load data using a data streamer. */
    public void setUseDataStreamer(boolean val) {
        useDataStreamer = val;
    }

    /** @return Data streamer configuration callback. */
    public Consumer<IgniteDataStreamer<Object, Object>> dataStreamerConfiguration() {
        return streamerCfg;
    }

    /** @return Human-readable DataStreamer configuration description. */
    public String dataStreamerConfigurationDescription() {
        return streamerCfgDescription;
    }

    /**
     * @param cfg Data streamer configuration callback, invoked once per cache before adding data.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration dataStreamerConfiguration(Consumer<IgniteDataStreamer<Object, Object>> cfg) {
        return dataStreamerConfiguration(cfg, "custom callback");
    }

    /**
     * @param cfg Data streamer configuration callback, invoked once per cache before adding data.
     * @param description Human-readable description for startup logging.
     * @return {@code this} for chaining.
     */
    public CacheDataMigrationConfiguration dataStreamerConfiguration(
        Consumer<IgniteDataStreamer<Object, Object>> cfg,
        String description
    ) {
        streamerCfg = Objects.requireNonNull(cfg);
        streamerCfgDescription = Objects.requireNonNull(description);

        return this;
    }
}
