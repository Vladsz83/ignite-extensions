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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.migration.api.CacheConfigTransformer;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationEntry;
import org.apache.ignite.util.migration.impl.ThickClientMigrationOrigin;
import org.apache.ignite.util.migration.impl.channel.BinaryMetadataMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.CacheConfigurationsMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.CacheDataMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.MetastorageMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.TypeMappingsMigrationChannel;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** End-to-end migration between isolated clusters with different storage and affinity topologies. */
public class IgniteToIgniteMigrationTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx src;

    /** */
    private IgniteEx dest;

    /** */
    private boolean persistentDestination;

    /** */
    private boolean conflictResolver = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);
        int port = name.startsWith("dest") ? 47600 : 47500;

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setLocalPort(port).setLocalPortRange(10)
            .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Collections.singleton("127.0.0.1:" + port))));
        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setIncludeEventTypes(EVT_CACHE_QUERY_OBJECT_READ);

        if (conflictResolver)
            cfg.setPluginProviders(new AbstractTestPluginProvider() {
                @Override public String name() {
                    return "MigrationConflictResolver";
                }

                @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                    return cls == CacheConflictResolutionManager.class ? (T)new TestConflictResolver<>() : null;
                }
            });

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setInitialSize(32L * 1024 * 1024).setMaxSize(64L * 1024 * 1024)
                .setPersistenceEnabled(persistentDestination && name.startsWith("dest"))));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** Basic migration of primitive, string and custom values; a starting point for debugging the migration flow. */
    @Test
    public void testSimpleMigration() throws Exception {
        startClusters();

        IgniteCache<Integer, Object> mixed = src.createCache(new CacheConfiguration<Integer, Object>("mixed")
            .setAffinity(new RendezvousAffinityFunction(false, 4)));
        IgniteCache<String, String> strings = src.createCache(new CacheConfiguration<String, String>("strings")
            .setAffinity(new RendezvousAffinityFunction(false, 4)));

        mixed.put(1, 10);
        mixed.put(2, 20);

        CustomData person = new CustomData("Alice", 30, "London");

        mixed.put(3, person);

        strings.put("first", "hello");
        strings.put("second", "world");

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true));

        IgniteCache<Integer, Object> mixedCopy = dest.cache("mixed");

        assertEquals(3, mixedCopy.size());
        assertEquals(10, mixedCopy.get(1));
        assertEquals(20, mixedCopy.get(2));

        assertEquals(2, dest.cache("strings").size());
        assertEquals("hello", dest.cache("strings").get("first"));
        assertEquals("world", dest.cache("strings").get("second"));

        assertEquals(person, mixedCopy.get(3));
    }

    /** Binary records retain expiry and original versions across different partition counts and storage modes. */
    @Test
    public void testBinaryMetadataExpiryVersionsAndAffinity() throws Exception {
        persistentDestination = true;
        startClusters();

        for (CacheAtomicityMode mode : CacheAtomicityMode.values()) {
            String name = "records-" + mode;
            src.createCache(new CacheConfiguration<>(name)
                .setAtomicityMode(mode).setBackups(1).setAffinity(new RendezvousAffinityFunction(false, 8)));

            for (int i = 0; i < 40; i++) {
                BinaryObject key = src.binary().builder("MigrationKey").setField("id", i).build();
                BinaryObject val = src.binary().builder("MigrationValue").setField("name", "value-" + i).build();

                GridCacheContext<Object, Object> ctx = src.cachex(name).context();

                src.cachex(name).putAllConflict(Collections.singletonMap(ctx.toCacheKeyObject(key),
                    new GridCacheDrExpirationInfo(ctx.toCacheObject(val), new GridCacheVersion(1, i + 1, 1, (byte)1),
                        0, System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5))));
            }
        }

        migrate(new IgniteToIgniteMigrationConfiguration().setBatchSize(7).setReadThreads(3).setWriteThreads(2)
            .setQueueCapacity(1).setCacheConfigurationTransformer(cfg -> cfg.setName("copy-" + cfg.getName())
                .setBackups(0).setAffinity(new RendezvousAffinityFunction(false, 32))));

        for (CacheAtomicityMode mode : CacheAtomicityMode.values()) {
            String name = "records-" + mode;
            IgniteCache<Object, Object> copy = dest.cache("copy-" + name).withKeepBinary();

            assertEquals(40, copy.size());
            assertEquals(32, dest.affinity(copy.getName()).partitions());

            for (int i = 0; i < 40; i++) {
                BinaryObject key = src.binary().builder("MigrationKey").setField("id", i).build();
                BinaryObject val = (BinaryObject)copy.get(key);

                assertEquals("value-" + i, val.field("name"));

                IgniteEx primary = (IgniteEx)grid(src.affinity(name).mapKeyToNode(key));
                CacheDataRow srcRow = row(primary, name, key);
                CacheDataRow destRow = row(dest, copy.getName(), key);

                assertEquals(srcRow.expireTime(), destRow.expireTime());
                assertEquals(srcRow.version().conflictVersion(), destRow.version().conflictVersion());
            }
        }

        assertNotNull(dest.binary().type("MigrationValue"));
        assertEquals(2, src.cluster().nodes().size());
        assertEquals(1, dest.cluster().nodes().size());
    }

    /** Group selection and create policies apply to data as well as definitions. */
    @Test
    public void testGroupsAndExistingCaches() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("selected").setGroupName("included")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "source");
        src.createCache(new CacheConfiguration<Integer, String>("excluded").setGroupName("excluded")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(2, "excluded");
        dest.createCache(new CacheConfiguration<Integer, String>("selected")
            .setAffinity(new RendezvousAffinityFunction(false, 16))).put(3, "retain");

        assertThrows(log, () -> {
            migrate(new IgniteToIgniteMigrationConfiguration().setCacheGroupNames("included"));
            return null;
        }, CacheExistsException.class, "Destination cache already exists");

        migrate(new IgniteToIgniteMigrationConfiguration().setCacheGroupNames("included")
            .setNoCacheCreate(true).setSkipConflictVersion(true).setBatchSize(2));

        assertEquals("source", dest.cache("selected").get(1));
        assertEquals("retain", dest.cache("selected").get(3));
        assertNull(dest.cache("excluded"));
    }

    /** Distributed metadata is allowlisted; the destination cluster identity remains distinct. */
    @Test
    public void testMetastorageFilter() throws Exception {
        startClusters();

        src.context().distributedMetastorage().write("migration.allowed", "value");
        src.context().distributedMetastorage().write("migration.private", "private");

        migrate(new IgniteToIgniteMigrationConfiguration().setMetastorageFilter("migration.allowed"::equals));

        assertEquals("value", dest.context().distributedMetastorage().read("migration.allowed"));
        assertNull(dest.context().distributedMetastorage().read("migration.private"));
        assertFalse(src.cluster().id().equals(dest.cluster().id()));
    }

    /** Each channel applies its typed transformer, and later channels use the selected destination cache names. */
    @Test
    public void testChannelTransformers() throws Exception {
        startClusters();

        IgniteCache<Integer, Object> cache = src.createCache(new CacheConfiguration<Integer, Object>("channel")
            .setAffinity(new RendezvousAffinityFunction(false, 8)));
        BinaryObject val = src.binary().builder("ChannelValue").setField("value", "payload").build();

        cache.put(1, val);
        cache.put(2, val);
        src.createCache(new CacheConfiguration<>("excluded").setAffinity(new RendezvousAffinityFunction(false, 8)));
        src.context().distributedMetastorage().write("channel.setting", "source");

        AtomicBoolean metaTransferred = new AtomicBoolean();
        AtomicBoolean mappingTransferred = new AtomicBoolean();
        AtomicInteger cacheSelections = new AtomicInteger();

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true)
            .setMetastorageFilter("channel.setting"::equals)
            .setTransformer(BinaryMetadataMigrationChannel.class, meta -> {
                if ("ChannelValue".equals(meta.typeName()))
                    metaTransferred.set(true);

                return meta;
            })
            .setTransformer(TypeMappingsMigrationChannel.class, mapping -> {
                if ("ChannelValue".equals(mapping.typeName()))
                    mappingTransferred.set(true);

                return mapping;
            })
            .setTransformer(CacheConfigurationsMigrationChannel.class, cfg -> {
                cacheSelections.incrementAndGet();

                if ("excluded".equals(cfg.sourceName()))
                    return null;

                cfg.configuration().setName("copy-" + cfg.sourceName());

                return cfg;
            })
            .setTransformer(MetastorageMigrationChannel.class,
                entry -> new SimpleImmutableEntry<>(entry.getKey(), "transformed"))
            .setTransformer(CacheDataMigrationChannel.class, entry -> Integer.valueOf(1).equals(entry.key()) ?
                new MigrationEntry(101, entry.value(), entry.expireTime(), entry.version()) : null));

        IgniteCache<Object, Object> copy = dest.cache("copy-channel").withKeepBinary();

        assertTrue(metaTransferred.get());
        assertTrue(mappingTransferred.get());
        assertEquals(2, cacheSelections.get());
        assertEquals(1, copy.size());
        assertEquals("payload", ((BinaryObject)copy.get(101)).field("value"));
        assertNull(dest.cache("excluded"));
        assertEquals("transformed", dest.context().distributedMetastorage().read("channel.setting"));
        assertEquals(2, src.cache("channel").size());
    }

    /** Expiration timestamps do not turn into a relative TTL when source versions are skipped. */
    @Test
    public void testExpiryWithoutConflictVersions() throws Exception {
        conflictResolver = false;
        startClusters();

        for (CacheAtomicityMode mode : CacheAtomicityMode.values()) {
            String name = "expiry-" + mode;

            src.createCache(new CacheConfiguration<>(name).setAtomicityMode(mode)
                .setAffinity(new RendezvousAffinityFunction(false, 8)));

            GridCacheContext<Object, Object> ctx = src.cachex(name).context();

            for (int key = 0; key < 10; key++) {
                src.cachex(name).putAllConflict(Collections.singletonMap(ctx.toCacheKeyObject(key),
                    new GridCacheDrExpirationInfo(ctx.toCacheObject("value"),
                        ctx.versions().next(src.cluster().topologyVersion()), 0,
                        System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5))));
            }
        }

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true).setBatchSize(3));

        for (CacheAtomicityMode mode : CacheAtomicityMode.values()) {
            String name = "expiry-" + mode;

            assertEquals(10, dest.cache(name).size());

            for (int key = 0; key < 10; key++) {
                CacheDataRow srcRow = row(grid(src.affinity(name).mapKeyToNode(key)), name, key);
                CacheDataRow destRow = row(dest, name, key);

                assertEquals(srcRow.expireTime(), destRow.expireTime());
                assertNull(destRow.version().otherClusterVersion());
            }
        }
    }

    /** Data structure metadata and backing caches remain usable on the destination. */
    @Test
    public void testDataStructures() throws Exception {
        checkDataStructures(false);
    }

    /** Backing caches can also be loaded using the streamer after ignoring entry metadata. */
    @Test
    public void testDataStructuresWithDataStreamer() throws Exception {
        checkDataStructures(true);
    }

    /** */
    private void checkDataStructures(boolean streamer) throws Exception {
        startClusters();

        src.atomicLong("counter", 42, true);
        src.atomicSequence("sequence", 100, true);
        src.atomicReference("reference", "value", true);
        src.atomicStamped("stamped", "value", 2, true);
        src.countDownLatch("latch", 3, false, true).countDown();
        src.semaphore("semaphore", 4, true, true);
        src.reentrantLock("lock", true, true, true);

        IgniteSet<String> set = src.set("set", new CollectionConfiguration().setBackups(1));
        IgniteQueue<String> queue = src.queue("queue", 10, new CollectionConfiguration().setBackups(1));

        set.add("item");
        queue.add("first");
        queue.add("second");

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true).setSkipExpireTime(true)
            .setBatchSize(10).setUseDataStreamer(streamer));

        assertEquals(42, dest.atomicLong("counter", 0, false).get());
        assertTrue(dest.atomicSequence("sequence", 0, false).get() >= 100);
        assertEquals("value", dest.atomicReference("reference", null, false).get());
        assertEquals("value", dest.atomicStamped("stamped", null, null, false).value());
        assertEquals(2, dest.atomicStamped("stamped", null, null, false).stamp());
        assertEquals(2, dest.countDownLatch("latch", 0, false, false).count());
        assertEquals(3, dest.countDownLatch("latch", 0, false, false).initialCount());
        assertEquals(4, dest.semaphore("semaphore", 0, true, false).availablePermits());

        IgniteLock lock = dest.reentrantLock("lock", true, true, false);

        assertTrue(lock.tryLock());
        lock.unlock();

        assertTrue(dest.set("set", null).contains("item"));
        assertEquals("first", dest.queue("queue", 0, null).poll());
        assertEquals("second", dest.queue("queue", 0, null).poll());
    }

    /** SQL caches retain generated type names, ALTER TABLE changes, indexes and views. */
    @Test
    public void testSqlSchema() throws Exception {
        startClusters();

        sql(src, "CREATE TABLE MIGRATION_TEST (ID INT PRIMARY KEY, NAME VARCHAR) WITH \"BACKUPS=0\"");
        sql(src, "ALTER TABLE MIGRATION_TEST ADD COLUMN AGE INT");
        sql(src, "CREATE INDEX MIGRATION_TEST_AGE ON MIGRATION_TEST (AGE)");
        sql(src, "INSERT INTO MIGRATION_TEST VALUES (1, 'Alice', 30)");
        sql(src, "CREATE VIEW MIGRATION_VIEW AS SELECT ID, AGE FROM MIGRATION_TEST");

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true).setBatchSize(3));

        assertEquals(sql(src, "SELECT ID, NAME, AGE FROM MIGRATION_TEST"),
            sql(dest, "SELECT ID, NAME, AGE FROM MIGRATION_TEST"));
        assertEquals(sql(src, "SELECT * FROM MIGRATION_VIEW"), sql(dest, "SELECT * FROM MIGRATION_VIEW"));
        assertFalse(sql(dest, "SELECT INDEX_NAME FROM SYS.INDEXES WHERE INDEX_NAME = 'MIGRATION_TEST_AGE'").isEmpty());
    }

    /** Each partition is scanned on its primary, even when a backup is available. */
    @Test
    public void testPrimaryScanRouting() throws Exception {
        startClusters();

        IgniteCache<Integer, String> cache = src.createCache(new CacheConfiguration<Integer, String>("routing")
            .setBackups(1).setAffinity(new RendezvousAffinityFunction(false, 16)));

        for (int key = 0; key < 64; key++)
            cache.put(key, "value");

        Queue<Object> nonPrimary = new ConcurrentLinkedQueue<>();
        AtomicInteger[] reads = {new AtomicInteger(), new AtomicInteger()};

        for (int idx = 0; idx < 2; idx++) {
            IgniteEx node = grid("source-" + idx);
            AtomicInteger cnt = reads[idx];

            node.events().localListen(evt -> {
                CacheQueryReadEvent<?, ?> read = (CacheQueryReadEvent<?, ?>)evt;

                if ("routing".equals(read.cacheName())) {
                    cnt.incrementAndGet();

                    if (!node.affinity("routing").isPrimary(node.localNode(), read.key()))
                        nonPrimary.add(read.key());
                }

                return true;
            }, EVT_CACHE_QUERY_OBJECT_READ);
        }

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true).setBatchSize(4).setReadThreads(2));

        assertTrue(nonPrimary.toString(), nonPrimary.isEmpty());
        assertTrue(reads[0].get() > 0);
        assertTrue(reads[1].get() > 0);
        assertEquals(64, reads[0].get() + reads[1].get());
    }

    /** The command line loads the example XMLs and configures a destination-aware transformer. */
    @Test
    public void testCommandLine() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("cli")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        assertEquals(0, IgniteToIgniteMigrationCommandLine.run(
            "--src-client-node-config", getClass().getResource("/migration/source.xml").toExternalForm(),
            "--dest-client-node-config", getClass().getResource("/migration/destination.xml").toExternalForm(),
            "--skip-conflict-version", "--batch-size", "3",
            "--cache-config-transformer", RenameTransformer.class.getName(),
            "--cache-config-transformer-parameters", "prefix=copy-"));

        assertEquals("value", dest.cache("copy-cli").get(1));
    }

    /** Streamer buffers from several writers must be flushed before the runner returns. */
    @Test
    public void testDataStreamer() throws Exception {
        startClusters();

        IgniteCache<Integer, String> cache = src.createCache(new CacheConfiguration<Integer, String>("streamer")
            .setAffinity(new RendezvousAffinityFunction(false, 8)));

        for (int key = 0; key < 150; key++)
            cache.put(key, "value-" + key);

        migrate(new IgniteToIgniteMigrationConfiguration().setUseDataStreamer(true).setSkipConflictVersion(true)
            .setSkipExpireTime(true).setWriteThreads(2).setReadThreads(2).setBatchSize(3).setQueueCapacity(1)
            .setDataStreamerConfiguration(streamer -> {
                streamer.allowOverwrite(true);
                streamer.perNodeBufferSize(7);
                streamer.perThreadBufferSize(2);
            }));

        assertEquals(150, dest.cache("streamer").size());

        for (int key = 0; key < 150; key++)
            assertEquals("value-" + key, dest.cache("streamer").get(key));
    }

    /** Metadata must not be silently dropped when using a streamer. */
    @Test
    public void testDataStreamerRejectsMetadata() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("streamer")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        Throwable err = assertThrows(log, () -> {
            migrate(new IgniteToIgniteMigrationConfiguration().setUseDataStreamer(true));

            return null;
        }, IgniteCheckedException.class, "Migration worker failed");

        assertTrue(err.getCause().getMessage().contains("DataStreamer cannot preserve entry metadata"));
    }

    /** Source versions cannot be silently discarded by a destination without conflict resolution. */
    @Test
    public void testMissingConflictResolver() throws Exception {
        conflictResolver = false;
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("versions")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        Throwable err = assertThrows(log, () -> {
            migrate(new IgniteToIgniteMigrationConfiguration());

            return null;
        }, IgniteCheckedException.class, "Migration worker failed");

        assertTrue(err.getCause().getMessage().contains("Conflict resolution must be configured"));
        assertEquals(0, dest.cache("versions").size());
    }

    /** Source and destination must never resolve to the same cluster. */
    @Test
    public void testSameCluster() throws Exception {
        startClusters();

        assertThrows(log, () -> {
            IgniteToIgniteMigration.start(
                ThickClientMigrationOrigin.create(getConfiguration("source-reader").setClientMode(true)),
                ThickClientMigrationOrigin.create(getConfiguration("source-writer").setClientMode(true)),
                new IgniteToIgniteMigrationConfiguration().setLogger(log));

            return null;
        }, IgniteCheckedException.class, "Source and destination must be different clusters");

        assertEquals(2, src.cluster().nodes().size());
    }

    /** Group filtering must not install views referencing an excluded table. */
    @Test
    public void testPartialSqlMigration() throws Exception {
        startClusters();

        sql(src, "CREATE TABLE INCLUDED (ID INT PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_GROUP=selected\"");
        sql(src, "CREATE TABLE EXCLUDED (ID INT PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_GROUP=excluded\"");
        sql(src, "CREATE VIEW EXCLUDED_VIEW AS SELECT ID FROM EXCLUDED");
        sql(src, "INSERT INTO INCLUDED VALUES (1, 'value')");

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true).setCacheGroupNames("selected"));

        assertEquals(sql(src, "SELECT ID FROM INCLUDED"), sql(dest, "SELECT ID FROM INCLUDED"));

        AtomicInteger views = new AtomicInteger();

        dest.context().distributedMetastorage().iterate("ignite.internal.sql.view.", (key, val) -> views.incrementAndGet());

        assertEquals(0, views.get());
    }

    /** Data structure migration can be disabled independently of user caches. */
    @Test
    public void testSkipDataStructures() throws Exception {
        startClusters();

        src.atomicLong("counter", 42, true);
        src.createCache(new CacheConfiguration<Integer, String>("user-cache")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true).setMoveDataStructures(false));

        assertEquals("value", dest.cache("user-cache").get(1));
        assertNull(dest.atomicLong("counter", 0, false));
    }

    /** Runtime node/thread ownership cannot be transferred to a different cluster. */
    @Test
    public void testAcquiredLock() throws Exception {
        startClusters();

        IgniteLock lock = src.reentrantLock("lock", true, true, true);

        lock.lock();

        try {
            Throwable err = assertThrows(log, () -> {
                migrate(new IgniteToIgniteMigrationConfiguration().setSkipConflictVersion(true));

                return null;
            }, IgniteCheckedException.class, "Migration worker failed");

            assertTrue(err.getCause().getMessage().contains("Cannot migrate a lock with an owner"));
        }
        finally {
            lock.unlock();
        }
    }

    /** */
    private static List<List<?>> sql(IgniteEx node, String sql) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC"), false).getAll();
    }

    /** */
    private void startClusters() throws Exception {
        src = startGrid("source-0");
        startGrid("source-1");
        dest = startGrid("dest-0");
        dest.cluster().state(ClusterState.ACTIVE);

        assertEquals(2, src.cluster().nodes().size());
        assertEquals(1, dest.cluster().nodes().size());
    }

    /** */
    private void migrate(IgniteToIgniteMigrationConfiguration cfg) throws Exception {
        IgniteToIgniteMigration.start(
            ThickClientMigrationOrigin.create(getConfiguration("source-reader").setClientMode(true)),
            ThickClientMigrationOrigin.create(getConfiguration("dest-writer").setClientMode(true)),
            cfg.setLogger(log)
        );
    }

    /** */
    private static CacheDataRow row(IgniteEx node, String name, Object key) throws Exception {
        GridCacheContext<Object, Object> ctx = node.cachex(name).context();

        ctx.shared().database().checkpointReadLock();

        try {
            return ctx.offheap().read(ctx, ctx.toCacheKeyObject(key));
        }
        finally {
            ctx.shared().database().checkpointReadUnlock();
        }
    }

    /** Simple custom cache value for the basic migration test. */
    public static class CustomData {
        /** */
        private final String name;

        /** */
        private final int age;

        /** */
        private final String city;

        /** */
        private CustomData(String name, int age, String city) {
            this.name = name;
            this.age = age;
            this.city = city;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (obj == null || getClass() != obj.getClass())
                return false;

            CustomData other = (CustomData)obj;

            return age == other.age && Objects.equals(name, other.name) && Objects.equals(city, other.city);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name, age, city);
        }
    }

    /** CLI transformer with a public no-argument constructor. */
    public static class RenameTransformer implements CacheConfigTransformer {
        /** */
        private String prefix;

        /** {@inheritDoc} */
        @Override public void configure(Map<String, String> parameters) {
            prefix = parameters.get("prefix");
        }

        /** {@inheritDoc} */
        @Override public CacheConfiguration<?, ?> apply(IgniteConfiguration destCfg, CacheConfiguration<?, ?> cfg) {
            assertTrue(destCfg.isClientMode());

            return cfg.setName(prefix + cfg.getName());
        }
    }

    /** Accepts incoming records while retaining their versions, as a conflict-enabled destination does. */
    private static class TestConflictResolver<K, V> extends GridCacheManagerAdapter<K, V>
        implements CacheConflictResolutionManager<K, V>, CacheVersionConflictResolver {
        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public <K1, V1> GridCacheVersionConflictContext<K1, V1> resolve(
            CacheObjectValueContext ctx,
            GridCacheVersionedEntryEx<K1, V1> oldEntry,
            GridCacheVersionedEntryEx<K1, V1> newEntry,
            boolean atomicVerComparator
        ) {
            GridCacheVersionConflictContext<K1, V1> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

            res.useNew();

            return res;
        }
    }
}
