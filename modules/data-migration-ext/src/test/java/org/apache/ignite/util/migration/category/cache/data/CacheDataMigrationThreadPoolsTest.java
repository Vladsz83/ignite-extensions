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

import java.util.concurrent.ThreadPoolExecutor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/** Cache-data reader and writer workers always use separate, channel-owned pools. */
public class CacheDataMigrationThreadPoolsTest {
    /** Defaults create four reader and four writer threads in separate pools. */
    @Test public void testDefaultReaderAndWriterPools() {
        CacheDataMigrationThreadPools pools = new CacheDataMigrationThreadPools(new CacheDataMigrationConfiguration());

        try {
            assertNotSame(pools.readerPool(), pools.writerPool());
            assertEquals(4, pools.readerThreads());
            assertEquals(4, pools.writerThreads());
            assertEquals(4, ((ThreadPoolExecutor)pools.readerPool()).getCorePoolSize());
            assertEquals(4, ((ThreadPoolExecutor)pools.writerPool()).getCorePoolSize());
        }
        finally {
            pools.stop(true);

            assertTrue(pools.readerPool().isShutdown());
            assertTrue(pools.writerPool().isShutdown());
        }
    }

    /** Explicit counts set the sizes of their independent pools. */
    @Test public void testConfiguredReaderAndWriterPools() {
        CacheDataMigrationThreadPools pools = new CacheDataMigrationThreadPools(
            new CacheDataMigrationConfiguration().readThreads(3).writeThreads(5));

        try {
            assertNotSame(pools.readerPool(), pools.writerPool());
            assertEquals(3, pools.readerThreads());
            assertEquals(5, pools.writerThreads());
            assertEquals(3, ((ThreadPoolExecutor)pools.readerPool()).getCorePoolSize());
            assertEquals(5, ((ThreadPoolExecutor)pools.writerPool()).getCorePoolSize());
        }
        finally {
            pools.stop(false);

            assertTrue(pools.readerPool().isShutdown());
            assertTrue(pools.writerPool().isShutdown());
        }
    }
}
