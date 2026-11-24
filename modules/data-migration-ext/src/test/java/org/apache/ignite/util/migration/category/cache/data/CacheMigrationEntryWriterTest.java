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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Cache entry batches of an unknown destination type are forwarded through the accept contract. */
public class CacheMigrationEntryWriterTest {
    /** A destination with an unrecognized connection type receives the batch through acceptAll. */
    @Test public void testUnknownDestinationUsesAcceptContract() {
        List<Object> accepted = new ArrayList<>();
        MigrationDestination<Object> dest = destination(accepted, new Object());
        CacheMigrationEntry entry = new CacheMigrationEntry(1, "val", null, null);

        new TestTransferrer(new CacheDataMigrationConfiguration(), Collections.emptyMap()).writeBatch(dest, "cache",
            Arrays.asList(entry));

        assertEquals(1, accepted.size());
        assertSame(entry, accepted.get(0));
    }

    /** The default acceptAll forwards each entity to accept in order. */
    @Test public void testDefaultAcceptAll() {
        List<Object> accepted = new ArrayList<>();
        MigrationDestination<Object> dest = destination(accepted, new Object());
        CacheMigrationEntry e1 = new CacheMigrationEntry(1, "a", null, null);
        CacheMigrationEntry e2 = new CacheMigrationEntry(2, "b", null, null);

        dest.acceptAll(MigrationCategory.CACHE_ENTRY, Arrays.asList(e1, e2));

        assertEquals(2, accepted.size());
        assertSame(e1, accepted.get(0));
        assertSame(e2, accepted.get(1));
    }

    /** */
    private static MigrationDestination<Object> destination(List<Object> accepted, Object origin) {
        return new MigrationDestination<>() {
            @Override public Object origin() {
                return origin;
            }

            @Override public Object connect() {
                return origin;
            }

            @Override public void close() {
                // No-op.
            }

            @Override public void accept(MigrationCategory category, Object entity) {
                assertEquals(MigrationCategory.CACHE_ENTRY, category);

                accepted.add(entity);
            }
        };
    }

    /** Test double exposing the write path without a source connection. */
    private static final class TestTransferrer extends AbstractCacheDataTransferrer<Object> {
        TestTransferrer(CacheDataMigrationConfiguration cfg, Map<String, String> cacheNameMap) {
            super(null, new JavaLogger(), cfg, cacheNameMap);
        }

        @Override protected int readWorkerCount(int readerThreads) {
            return 1;
        }

        @Override protected void submitReadTasks(
            ExecutorCompletionService<Void> exec,
            int readWorkers,
            MigrationDestination<?> dest,
            @Nullable MigrationTransformer<CacheMigrationEntry> transformer
        ) {
            // No read tasks.
        }
    }
}
