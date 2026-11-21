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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.thread.IgniteThreadFactory;

/** Independent reader and writer executors for one cache-data migration. */
final class CacheDataMigrationThreadPools {
    /** Reader executor. */
    private final ExecutorService readerPool;

    /** Writer executor. */
    private final ExecutorService writerPool;

    /** Reader worker count. */
    private final int readerThreads;

    /** Writer worker count. */
    private final int writerThreads;

    /**
     * @param cfg Transfer configuration.
     */
    CacheDataMigrationThreadPools(CacheDataMigrationConfiguration cfg) {
        readerThreads = cfg.readThreads();
        writerThreads = cfg.writeThreads();
        readerPool = Executors.newFixedThreadPool(readerThreads,
            new IgniteThreadFactory(null, "migration-cache-reader"));
        writerPool = Executors.newFixedThreadPool(writerThreads,
            new IgniteThreadFactory(null, "migration-cache-writer"));
    }

    /** @return Reader executor. */
    ExecutorService readerPool() {
        return readerPool;
    }

    /** @return Writer executor. */
    ExecutorService writerPool() {
        return writerPool;
    }

    /** @return Reader worker count. */
    int readerThreads() {
        return readerThreads;
    }

    /** @return Writer worker count. */
    int writerThreads() {
        return writerThreads;
    }

    /** Stops both transferrer-owned executors. */
    void stop(boolean success) {
        stop(readerPool, success);
        stop(writerPool, success);
    }

    /** Stops a transferrer-owned executor. */
    private static void stop(ExecutorService pool, boolean success) {
        if (success)
            pool.shutdown();
        else
            pool.shutdownNow();
    }
}
