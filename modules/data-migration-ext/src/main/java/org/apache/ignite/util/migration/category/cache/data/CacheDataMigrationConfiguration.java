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
    public static final int DFLT_READ_THREADS = 4;

    /** Default cache-data writer worker count. */
    public static final int DFLT_WRITE_THREADS = 4;

    /** */
    private int batchSize = DFLT_BATCH_SIZE;

    /** Cache-data reader worker count. */
    private int readThreads = DFLT_READ_THREADS;

    /** Cache-data writer worker count. */
    private int writeThreads = DFLT_WRITE_THREADS;

    /** Number of queued batches, independent of the number of source partitions. */
    private int queueCapacity = DFLT_QUEUE_CAPACITY;

    /** Zero means unlimited. */
    private int readLimit;

    /** Zero means unlimited. */
    private int writeLimit;

    /** */
    private boolean skipExpireTime;

    /** */
    private boolean skipConflictVer;

    /** */
    private boolean useDataStreamer;

    /** */
    private Consumer<IgniteDataStreamer<Object, Object>> streamerCfg = streamer -> { /* No-op. */ };

    /** Validates settings before a migration starts. */
    public void validate() {
        positive(batchSize, "batch-size");
        positive(readThreads, "read-threads");
        positive(writeThreads, "write-threads");
        positive(queueCapacity, "queue-capacity");

        if (readLimit < 0 || writeLimit < 0)
            throw new IllegalArgumentException("Read and write rate limits must be non-negative.");
    }

    /** @return Maximum entries per batch. */
    public int batchSize() {
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

    /** @param val Reader worker count. */
    public void setReadThreads(int val) {
        positive(val, "read-threads");
        readThreads = val;
    }

    /** @return Writer worker count. */
    public int writeThreads() {
        return writeThreads;
    }

    /** @param val Writer worker count. */
    public void setWriteThreads(int val) {
        positive(val, "write-threads");
        writeThreads = val;
    }

    /** @return Maximum queued batches. */
    public int queueCapacity() {
        return queueCapacity;
    }

    /** @param val Maximum queued batches. */
    public void setQueueCapacity(int val) {
        queueCapacity = val;
    }

    /** @return Global read entries per second, or zero. */
    public int readLimitInSec() {
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

    /** @param val Global write rate in entries per second, or zero for unlimited. */
    public void setWriteLimitInSec(int val) {
        writeLimit = val;
    }

    /** @return Whether source expiry is ignored. */
    public boolean skipExpireTime() {
        return skipExpireTime;
    }

    /** @param val Whether source expiration timestamps are ignored. */
    public void setSkipExpireTime(boolean val) {
        skipExpireTime = val;
    }

    /** @return Whether source conflict versions are ignored. */
    public boolean skipConflictVersion() {
        return skipConflictVer;
    }

    /** @param val Whether source conflict versions are ignored. */
    public void setSkipConflictVersion(boolean val) {
        skipConflictVer = val;
    }

    /** @return Whether to load data using a data streamer. */
    public boolean useDataStreamer() {
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

    /** @param cfg Data streamer configuration callback. */
    public void setDataStreamerConfiguration(Consumer<IgniteDataStreamer<Object, Object>> cfg) {
        streamerCfg = Objects.requireNonNull(cfg);
    }

    /** */
    private static void positive(int val, String name) {
        if (val <= 0)
            throw new IllegalArgumentException(name + " must be positive.");
    }
}
