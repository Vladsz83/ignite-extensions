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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationCache;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationEntry;
import org.apache.ignite.util.migration.api.MigrationPartition;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.api.MigrationTypeMapping;
import org.apache.ignite.util.migration.impl.MigrationCachePlan;
import org.apache.ignite.util.migration.impl.channel.CacheConfigurationsMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.MetastorageMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.SqlViewsMigrationChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Pipeline failure and resource ownership tests without cluster startup. */
public class IgniteToIgniteMigrationPipelineTest {
    /** A failed writer must release a reader blocked by a full queue. */
    @Test(timeout = 10_000)
    public void testWriterFailureWithFullQueue() throws Exception {
        Endpoint src = new Endpoint();
        Endpoint dest = new Endpoint();
        RuntimeException err = new IllegalStateException("write failure");

        dest.writeErr = err;
        dest.ready = src.ready;

        checkFailure(src, dest, err);

        assertTrue(src.cursorClosed.get());
    }

    /** A failed reader must terminate writers waiting for batches and close its cursor. */
    @Test(timeout = 10_000)
    public void testReaderFailure() throws Exception {
        Endpoint src = new Endpoint();
        Endpoint dest = new Endpoint();
        RuntimeException err = new IllegalStateException("read failure");

        src.readErr = err;

        checkFailure(src, dest, err);

        assertTrue(src.cursorClosed.get());
    }

    /** Both endpoints are closed even if the destination connection fails. */
    @Test
    public void testConnectionFailure() throws Exception {
        Endpoint src = new Endpoint();
        Endpoint dest = new Endpoint();
        RuntimeException err = new IllegalStateException("connect failure");

        dest.connectErr = err;

        try {
            IgniteToIgniteMigration.start(src, dest, configuration());

            fail("Expected connection failure");
        }
        catch (RuntimeException e) {
            assertSame(err, e);
        }

        assertTrue(src.closed.get());
        assertTrue(dest.closed.get());
    }

    /** A buffered destination failure must propagate even after every batch was accepted. */
    @Test(timeout = 10_000)
    public void testFinishFailure() throws Exception {
        Endpoint src = new Endpoint();
        Endpoint dest = new Endpoint();
        RuntimeException err = new IllegalStateException("flush failure");

        dest.finishErr = err;

        try {
            IgniteToIgniteMigration.start(src, dest, configuration());

            fail("Expected buffered write failure");
        }
        catch (RuntimeException e) {
            assertSame(err, e);
        }

        assertTrue(src.closed.get());
        assertTrue(dest.closed.get());
    }

    /** Disabled metadata channels must not read the source; cache selection is still required for existing caches. */
    @Test(timeout = 10_000)
    public void testSkippedMetadataChannels() throws Exception {
        for (int mask = 1; mask < 4; mask++) {
            Endpoint src = new Endpoint();
            Endpoint dest = new Endpoint();
            boolean skipMetadata = (mask & 1) != 0;
            boolean skipMappings = (mask & 2) != 0;

            if (skipMetadata)
                src.binaryMetadataErr = new IllegalStateException("Binary metadata must not be read");

            if (skipMappings)
                src.mappingsErr = new IllegalStateException("Type mappings must not be read");

            IgniteToIgniteMigration.start(src, dest, configuration().setNoBinaryMetadataCreate(skipMetadata)
                .setNoMappingsCreate(skipMappings).setNoCacheCreate(true));

            assertEquals(100, dest.written.get());
            assertTrue(src.closed.get());
            assertTrue(dest.closed.get());
        }
    }

    /** Settings must be available during cache creation, while SQL views must wait for their tables. */
    @Test(timeout = 10_000)
    public void testMetastorageBeforeCaches() throws Exception {
        String settingKey = "distrConf-sql.defaultQueryTimeout";
        String viewKey = "ignite.internal.sql.view.PUBLIC.TEST_VIEW";
        Map<String, Serializable> entries = new LinkedHashMap<>();
        Map<String, Serializable> applied = new LinkedHashMap<>();
        AtomicBoolean cachePrepared = new AtomicBoolean();
        AtomicInteger metaReads = new AtomicInteger();
        AtomicInteger viewReads = new AtomicInteger();

        entries.put(settingKey, 1000);
        entries.put(viewKey, "view definition");

        Endpoint src = new Endpoint() {
            @Override public Map<String, Serializable> metastorage(IgniteToIgniteMigrationConfiguration cfg) {
                metaReads.incrementAndGet();

                return Collections.singletonMap(settingKey, 1000);
            }

            @Override public Map<String, Serializable> sqlViews(IgniteToIgniteMigrationConfiguration cfg) {
                viewReads.incrementAndGet();

                return Collections.singletonMap(viewKey, "view definition");
            }
        };
        Endpoint dest = new Endpoint() {
            @Override public void cache(MigrationCache cache, IgniteToIgniteMigrationConfiguration cfg) {
                assertEquals(1000, applied.get(settingKey));
                assertFalse(applied.containsKey(viewKey));

                cachePrepared.set(true);
            }

            @Override public void metastorage(String key, Serializable val) {
                assertFalse(cachePrepared.get());

                applied.put(key, val);
            }

            @Override public void sqlView(String key, Serializable val) {
                assertTrue(cachePrepared.get());

                applied.put(key, val);
            }
        };

        IgniteToIgniteMigration.start(src, dest, configuration());

        assertEquals(entries, applied);
        assertEquals(1, metaReads.get());
        assertEquals(1, viewReads.get());
        assertEquals(100, dest.written.get());
    }

    /** The SQL-view channel reads and writes views without invoking settings or cache transfer. */
    @Test
    public void testSqlViewsIndependentOfMetastorage() throws Exception {
        Map<String, Serializable> views = Collections.singletonMap("ignite.internal.sql.view.PUBLIC.VIEW", "definition");
        Map<String, Serializable> applied = new LinkedHashMap<>();
        Endpoint src = new Endpoint() {
            @Override public Map<String, Serializable> metastorage(IgniteToIgniteMigrationConfiguration cfg) {
                throw new IllegalStateException("SQL view transfer must not read settings");
            }

            @Override public Map<String, Serializable> sqlViews(IgniteToIgniteMigrationConfiguration cfg) {
                return views;
            }
        };
        Endpoint dest = new Endpoint() {
            @Override public void metastorage(String key, Serializable val) {
                throw new IllegalStateException("SQL view transfer must not write settings");
            }

            @Override public void cache(MigrationCache cache, IgniteToIgniteMigrationConfiguration cfg) {
                throw new IllegalStateException("SQL view transfer must not create caches");
            }

            @Override public void sqlView(String key, Serializable val) {
                applied.put(key, val);
            }
        };
        IgniteToIgniteMigrationConfiguration cfg = configuration();
        MigrationCachePlan cachePlan = new MigrationCachePlan(src, cfg,
            cfg.getTransformer(CacheConfigurationsMigrationChannel.class));

        new SqlViewsMigrationChannel(src, dest, cfg, cachePlan).transfer();

        assertEquals(views, applied);
        assertFalse(src.closed.get());
        assertFalse(dest.closed.get());
    }

    /** Settings transfer must not query SQL views or cache definitions. */
    @Test
    public void testMetastorageIndependentOfSqlViews() throws Exception {
        Map<String, Serializable> settings = Collections.singletonMap("distrConf-sql.defaultQueryTimeout", 1000);
        Map<String, Serializable> applied = new LinkedHashMap<>();
        Endpoint src = new Endpoint() {
            @Override public Map<String, Serializable> metastorage(IgniteToIgniteMigrationConfiguration cfg) {
                return settings;
            }

            @Override public Map<String, Serializable> sqlViews(IgniteToIgniteMigrationConfiguration cfg) {
                throw new IllegalStateException("Settings transfer must not read SQL views");
            }

            @Override public Collection<MigrationCache> caches() {
                throw new IllegalStateException("Settings transfer must not select caches");
            }
        };
        Endpoint dest = new Endpoint() {
            @Override public void metastorage(String key, Serializable val) {
                applied.put(key, val);
            }

            @Override public void sqlView(String key, Serializable val) {
                throw new IllegalStateException("Settings transfer must not write SQL views");
            }
        };

        new MetastorageMigrationChannel(src, dest, configuration()).transfer();

        assertEquals(settings, applied);
        assertFalse(src.closed.get());
        assertFalse(dest.closed.get());
    }

    /** */
    private void checkFailure(Endpoint src, Endpoint dest, RuntimeException err) throws Exception {
        try {
            IgniteToIgniteMigration.start(src, dest, configuration());

            fail("Expected worker failure");
        }
        catch (IgniteCheckedException e) {
            assertSame(err, e.getCause());
        }

        assertTrue(src.closed.get());
        assertTrue(dest.closed.get());
    }

    /** */
    private static IgniteToIgniteMigrationConfiguration configuration() {
        return new IgniteToIgniteMigrationConfiguration().setQueueCapacity(1).setLogger(new NullLogger());
    }

    /** Controllable endpoint for exercising pipeline failures and resource cleanup. */
    private static class Endpoint implements MigrationSource, MigrationDestination {
        /** */
        private final AtomicBoolean closed = new AtomicBoolean();

        /** */
        private final AtomicBoolean cursorClosed = new AtomicBoolean();

        /** */
        private final AtomicInteger written = new AtomicInteger();

        /** Signals that the reader has reached a batch that cannot fit in the queue. */
        private CountDownLatch ready = new CountDownLatch(1);

        /** */
        private RuntimeException connectErr;

        /** */
        private RuntimeException binaryMetadataErr;

        /** */
        private RuntimeException mappingsErr;

        /** */
        private RuntimeException readErr;

        /** */
        private RuntimeException writeErr;

        /** */
        private RuntimeException finishErr;

        /** {@inheritDoc} */
        @Override public void connect() {
            if (connectErr != null)
                throw connectErr;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            closed.set(true);
        }

        /** {@inheritDoc} */
        @Override public Collection<BinaryMetadata> binaryMetadata() {
            if (binaryMetadataErr != null)
                throw binaryMetadataErr;

            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public Collection<MigrationTypeMapping> typeMappings() {
            if (mappingsErr != null)
                throw mappingsErr;

            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public Collection<MigrationCache> caches() {
            return Collections.singletonList(new MigrationCache("cache", new CacheConfiguration<>("cache"), false));
        }

        /** {@inheritDoc} */
        @Override public Map<String, Serializable> metastorage(IgniteToIgniteMigrationConfiguration cfg) {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Serializable> sqlViews(IgniteToIgniteMigrationConfiguration cfg) {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public List<MigrationPartition> partitions(Map<String, String> cacheNames) {
            assertEquals(Collections.singletonMap("cache", "cache"), cacheNames);

            return Collections.singletonList(new MigrationPartition("cache", "cache", 0));
        }

        /** {@inheritDoc} */
        @Override public QueryCursor<MigrationEntry> read(
            MigrationPartition part,
            IgniteToIgniteMigrationConfiguration cfg
        ) {
            return new QueryCursorImpl<MigrationEntry>(() -> new Iterator<MigrationEntry>() {
                private int idx;

                @Override public boolean hasNext() {
                    if (readErr != null)
                        throw readErr;

                    return idx < 100;
                }

                @Override public MigrationEntry next() {
                    if (++idx == 3)
                        ready.countDown();

                    return new MigrationEntry(idx, "value", 0, null);
                }
            }) {
                @Override public void close() {
                    super.close();

                    cursorClosed.set(true);
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void binaryMetadata(BinaryMetadata meta) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void typeMapping(MigrationTypeMapping mapping) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cache(MigrationCache cache, IgniteToIgniteMigrationConfiguration cfg) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void metastorage(String key, Serializable val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sqlView(String key, Serializable val) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void write(String cache, List<MigrationEntry> entries, IgniteToIgniteMigrationConfiguration cfg)
            throws IgniteCheckedException {
            if (writeErr == null) {
                written.addAndGet(entries.size());

                return;
            }

            try {
                assertTrue(ready.await(5, TimeUnit.SECONDS));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteCheckedException(e);
            }

            throw writeErr;
        }

        /** {@inheritDoc} */
        @Override public void finish() {
            if (finishErr != null)
                throw finishErr;
        }
    }
}
