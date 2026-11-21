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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
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
import org.apache.ignite.internal.processors.cluster.ClusterIdAndTag;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationsTransferrer;
import org.apache.ignite.util.migration.category.cache.data.CacheDataTransferrer;
import org.apache.ignite.util.migration.category.metastorage.MetastorageKeyFilterTransformer;
import org.apache.ignite.util.migration.category.metastorage.MetastorageTransferrer;
import org.apache.ignite.util.migration.category.type.BinaryMetadataTransferrer;
import org.apache.ignite.util.migration.category.type.MigrationTypeMapping;
import org.apache.ignite.util.migration.category.type.TypeMappingsTransferrer;
import org.apache.ignite.util.migration.endpoint.ThickClientMigrationEndpoint;
import org.apache.ignite.util.migration.endpoint.ThinClientMigrationEndpoint;
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

        IgniteToIgniteMigrationConfiguration simple = new IgniteToIgniteMigrationConfiguration();
        simple.cacheData().skipConflictVersion(true);
        migrate(simple);

        IgniteCache<Integer, Object> mixedCopy = dest.cache("mixed");

        assertEquals(3, mixedCopy.size());
        assertEquals(10, mixedCopy.get(1));
        assertEquals(20, mixedCopy.get(2));

        assertEquals(2, dest.cache("strings").size());
        assertEquals("hello", dest.cache("strings").get("first"));
        assertEquals("world", dest.cache("strings").get("second"));

        assertEquals(person, mixedCopy.get(3));

        MigrationTypeMapping srcMapping = typeMapping(src, CustomData.class.getName());
        MigrationTypeMapping destMapping = typeMapping(dest, CustomData.class.getName());

        assertNotNull(srcMapping);
        assertNotNull(destMapping);
        assertEquals(srcMapping.typeId(), destMapping.typeId());
        assertEquals(srcMapping.platformId(), destMapping.platformId());
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

        IgniteToIgniteMigrationConfiguration bin = new IgniteToIgniteMigrationConfiguration();
        bin.cacheData().batchSize(7).readThreads(3).writeThreads(2).queueCapacity(1);
        bin.cacheMigration().cacheConfigurationTransformer(cfg -> cfg.setName("copy-" + cfg.getName())
            .setBackups(0).setAffinity(new RendezvousAffinityFunction(false, 32)));
        migrate(bin);

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
        BinaryType srcType = src.binary().type("MigrationValue");
        BinaryType destType = dest.binary().type("MigrationValue");

        assertEquals(srcType.typeId(), destType.typeId());
        assertEquals(srcType.fieldNames(), destType.fieldNames());
        assertEquals(src.context().cacheObjects().binaryContext().metadata0(srcType.typeId()).schemaIds(),
            dest.context().cacheObjects().binaryContext().metadata0(destType.typeId()).schemaIds());
        assertEquals(2, src.cluster().nodes().size());
        assertEquals(1, dest.cluster().nodes().size());
    }

    /** Disabled explicit metadata channels are not invoked, while the stored binary cache row is still copied. */
    @Test
    public void testSkipExplicitMetadataChannels() throws Exception {
        startClusters();

        BinaryObject val = src.binary().builder("ImplicitMetadata").setField("value", "payload").build();
        src.createCache(new CacheConfiguration<Integer, BinaryObject>("skip-metadata")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, val);

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration()
            .noBinaryMetadataCreate(true)
            .noMappingsCreate(true);
        cfg.cacheData().skipConflictVersion(true);
        cfg.transformer(BinaryMetadataTransferrer.class, metadata -> {
            throw new AssertionError("Disabled binary metadata channel must not run.");
        });
        cfg.transformer(TypeMappingsTransferrer.class, mapping -> {
            throw new AssertionError("Disabled type mapping channel must not run.");
        });

        migrate(cfg);

        assertEquals(1, dest.cache("skip-metadata").size());
        assertNull(dest.binary().type("ImplicitMetadata"));
    }

    /** Binary metadata and type mapping transferrers apply configured transformers to each item. */
    @Test
    public void testMetadataAndMappingTransformers() throws Exception {
        startClusters();

        String metadataName = "FilteredMetadata";
        src.binary().builder(metadataName).setField("value", "payload").build();
        src.binary().toBinary(new CustomData("Filtered", 1, "test"));

        assertNotNull(typeMapping(src, CustomData.class.getName()));

        AtomicInteger filteredMetadata = new AtomicInteger();
        AtomicInteger filteredMappings = new AtomicInteger();
        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();

        cfg.transformer(BinaryMetadataTransferrer.class, metadata -> {
            if (metadataName.equals(metadata.typeName())) {
                filteredMetadata.incrementAndGet();

                return null;
            }

            return metadata;
        });
        cfg.transformer(TypeMappingsTransferrer.class, mapping -> {
            if (CustomData.class.getName().equals(mapping.typeName())) {
                filteredMappings.incrementAndGet();

                return null;
            }

            return mapping;
        });

        migrate(cfg);

        assertTrue(filteredMetadata.get() > 0);
        assertTrue(filteredMappings.get() > 0);
        assertNull(dest.binary().type(metadataName));
        assertNull(typeMapping(dest, CustomData.class.getName()));
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
            IgniteToIgniteMigrationConfiguration grps = new IgniteToIgniteMigrationConfiguration();
            grps.cacheMigration().cacheGroupNames("included");
            migrate(grps);
            return null;
        }, CacheExistsException.class, "Destination cache already exists");

        IgniteToIgniteMigrationConfiguration grps2 = new IgniteToIgniteMigrationConfiguration();
        grps2.cacheMigration().cacheGroupNames("included");
        grps2.cacheMigration().noCacheCreate(true);
        grps2.cacheData().skipConflictVersion(true).batchSize(2);
        migrate(grps2);

        assertEquals("source", dest.cache("selected").get(1));
        assertEquals("retain", dest.cache("selected").get(3));
        assertNull(dest.cache("excluded"));

        IgniteToIgniteMigrationConfiguration createMissing = new IgniteToIgniteMigrationConfiguration();
        createMissing.cacheMigration().cacheGroupNames("excluded");
        createMissing.cacheMigration().createCacheIfNotExists(true);
        createMissing.cacheData().skipConflictVersion(true);
        migrate(createMissing);

        assertEquals("excluded", dest.cache("excluded").get(2));
    }

    /** --no-cache-create rejects a selected destination cache that is missing. */
    @Test
    public void testNoCacheCreateRejectsMissingCache() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("missing-destination")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheMigration().noCacheCreate(true);

        Throwable err = assertThrows(log, () -> {
            migrate(cfg);

            return null;
        }, IgniteException.class, "Destination cache does not exist");

        assertNull(err.getCause());
        assertNull(dest.cache("missing-destination"));
    }

    /** Cache name transformers cannot map two selected source caches to the same destination name. */
    @Test
    public void testDuplicateTransformedCacheNames() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("first")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "first");
        src.createCache(new CacheConfiguration<Integer, String>("second")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(2, "second");

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheMigration().cacheConfigurationTransformer(cache -> cache.setName("duplicate"));

        Throwable err = assertThrows(log, () -> {
            migrate(cfg);

            return null;
        }, IgniteException.class, "Invalid or duplicate transformed cache name");

        assertTrue(err.getMessage().contains("Invalid or duplicate transformed cache name"));
        assertNull(dest.cache("duplicate"));
    }

    /** All distributed metastorage is copied to the destination; cluster identity remains distinct. */
    @Test
    public void testMetastorage() throws Exception {
        startClusters();

        src.context().distributedMetastorage().write("migration.first", "value");
        src.context().distributedMetastorage().write("migration.second", "other");
        src.context().distributedMetastorage().write("ignite.internal.graceful.shutdown",
            new HashSet<>(Collections.singleton(src.localNode().id())));

        migrate(new IgniteToIgniteMigrationConfiguration());

        assertEquals("value", dest.context().distributedMetastorage().read("migration.first"));
        assertEquals("other", dest.context().distributedMetastorage().read("migration.second"));
        assertFalse(src.cluster().id().equals(dest.cluster().id()));

        // The source cluster identity must not reach the destination: destination nodes adopt the
        // cluster ID from this key on startup, so copying it would rename the destination cluster
        // after a restart.
        ClusterIdAndTag destIdentity = dest.context().distributedMetastorage().read("ignite.internal.cluster.id.tag");

        assertEquals(dest.cluster().id(), destIdentity.id());

        // Graceful-stop coordination holds source node ids that are not valid on the destination.
        assertNull(dest.context().distributedMetastorage().read("ignite.internal.graceful.shutdown"));
    }

    /** Disabling the default key filter copies the non-portable entries. */
    @Test
    public void testIgnoreDefaultFilter() throws Exception {
        startClusters();

        src.context().distributedMetastorage().write("migration.first", "value");
        src.context().distributedMetastorage().write("ignite.internal.graceful.shutdown",
            new HashSet<>(Collections.singleton(src.localNode().id())));

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.metastorage().ignoreDefaultFilter(true);
        migrate(cfg);

        assertEquals("value", dest.context().distributedMetastorage().read("migration.first"));

        ClusterIdAndTag destIdentity = dest.context().distributedMetastorage().read("ignite.internal.cluster.id.tag");

        assertEquals(src.cluster().id(), destIdentity.id());
        assertEquals(Collections.singleton(src.localNode().id()),
            dest.context().distributedMetastorage().read("ignite.internal.graceful.shutdown"));
    }

    /** Configured key filters skip entries by name and prefix, case-insensitively. */
    @Test
    public void testMetastorageKeyFilters() throws Exception {
        startClusters();

        src.context().distributedMetastorage().write("migration.keep", "value");
        src.context().distributedMetastorage().write("secret.password", "pwd");
        src.context().distributedMetastorage().write("Config.SkipMe", "v");

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.metastorage().filter(new MetastorageKeyFilterTransformer(
            Collections.singleton("SECRET.Password"),
            Collections.singletonList("config.")));
        migrate(cfg);

        assertEquals("value", dest.context().distributedMetastorage().read("migration.keep"));
        assertNull(dest.context().distributedMetastorage().read("secret.password"));
        assertNull(dest.context().distributedMetastorage().read("Config.SkipMe"));
    }

    /** Cache-config and metastorage channels apply their typed transformers; later channels use the selected names. */
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

        AtomicInteger cacheSelections = new AtomicInteger();

        IgniteToIgniteMigrationConfiguration tr = new IgniteToIgniteMigrationConfiguration();
        tr.cacheData().skipConflictVersion(true);
        tr.transformer(CacheConfigurationsTransferrer.class, cfg -> {
            cacheSelections.incrementAndGet();

            if ("excluded".equals(cfg.sourceName()))
                return null;

            cfg.configuration().setName("copy-" + cfg.sourceName());

            return cfg;
        }).transformer(MetastorageTransferrer.class,
            entry -> "channel.setting".equals(entry.getKey())
                ? new SimpleImmutableEntry<>(entry.getKey(), "transformed")
                : entry).transformer(CacheDataTransferrer.class,
                    entry -> Integer.valueOf(2).equals(entry.key()) ? null : entry);
        migrate(tr);

        IgniteCache<Object, Object> copy = dest.cache("copy-channel").withKeepBinary();

        assertEquals(2, cacheSelections.get());
        assertEquals(1, copy.size());
        assertEquals("payload", ((BinaryObject)copy.get(1)).field("value"));
        assertNull(copy.get(2));
        assertNull(dest.cache("excluded"));
        assertEquals("transformed", dest.context().distributedMetastorage().read("channel.setting"));
    }

    /** A cache-data transformer failure is propagated and migration endpoints are closed. */
    @Test
    public void testCacheDataTransformerFailure() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("transformer-failure")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheData().skipConflictVersion(true);
        cfg.transformer(CacheDataTransferrer.class, entry -> {
            throw new IllegalStateException("forced cache-data transformer failure");
        });

        Throwable err = assertThrows(log, () -> {
            migrate(cfg);

            return null;
        }, IgniteException.class, "Migration worker failed.");

        assertTrue(rootCause(err).getMessage().contains("forced cache-data transformer failure"));
        assertEquals(2, src.cluster().nodes().size());
        assertEquals(1, dest.cluster().nodes().size());
    }

    /** Read and write rate limits apply globally to the cache-data reader/writer stages. */
    @Test
    public void testReadWriteRateLimits() throws Exception {
        startClusters();

        IgniteCache<Integer, String> cache = src.createCache(new CacheConfiguration<Integer, String>("rate-limited")
            .setAffinity(new RendezvousAffinityFunction(false, 8)));

        for (int key = 0; key < 4; key++)
            cache.put(key, "value-" + key);

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheData().batchSize(1).readThreads(1).writeThreads(1).readLimitInSec(4).writeLimitInSec(4)
            .skipConflictVersion(true);

        long start = System.nanoTime();
        migrate(cfg);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue("Rate limits should throttle the transfer [elapsedMs=" + elapsedMs + ']', elapsedMs >= 500);
        assertEquals(4, dest.cache("rate-limited").size());
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

        IgniteToIgniteMigrationConfiguration exp = new IgniteToIgniteMigrationConfiguration();
        exp.cacheData().skipConflictVersion(true).batchSize(3);
        migrate(exp);

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

        IgniteToIgniteMigrationConfiguration ds = new IgniteToIgniteMigrationConfiguration();
        ds.cacheData().skipConflictVersion(true).skipExpireTime(true).batchSize(10).useDataStreamer(streamer);
        migrate(ds);

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

        IgniteToIgniteMigrationConfiguration sqlCfg = new IgniteToIgniteMigrationConfiguration();
        sqlCfg.cacheData().skipConflictVersion(true).batchSize(3);
        migrate(sqlCfg);

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

        IgniteToIgniteMigrationConfiguration routing = new IgniteToIgniteMigrationConfiguration();
        routing.cacheData().skipConflictVersion(true).batchSize(4).readThreads(2);
        migrate(routing);

        assertTrue(nonPrimary.toString(), nonPrimary.isEmpty());
        assertTrue(reads[0].get() > 0);
        assertTrue(reads[1].get() > 0);
        assertEquals(64, reads[0].get() + reads[1].get());
    }

    /** Thick source can migrate cache configuration and records to a thin-client destination. */
    @Test
    public void testThickToThinClient() throws Exception {
        startClusters();

        IgniteCache<Integer, Object> cache = src.createCache(new CacheConfiguration<Integer, Object>("thick-to-thin")
            .setBackups(1).setGroupName("thin-group")
            .setAffinity(new RendezvousAffinityFunction(false, 16)));

        for (int key = 0; key < 40; key++)
            cache.put(key, "value-" + key);

        GridCacheContext<Object, Object> srcCtx = src.cachex("thick-to-thin").context();
        BinaryObject binVal = src.binary().builder("ThinCopyValue").setField("value", "binary").build();
        long expireTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
        GridCacheVersion ver = new GridCacheVersion(1, 100, 1, (byte)1);

        src.cachex("thick-to-thin").putAllConflict(Collections.singletonMap(srcCtx.toCacheKeyObject(100),
            new GridCacheDrExpirationInfo(srcCtx.toCacheObject(binVal), ver, 0, expireTime)));

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheData().batchSize(5).readThreads(2).writeThreads(2);

        IgniteToIgniteMigration.start(
            ThickClientMigrationEndpoint.create(getConfiguration("source-reader").setClientMode(true)),
            thinEndpoint("dest-0"), cfg.logger(log));

        try (IgniteClient client = thinClient("dest-0")) {
            org.apache.ignite.client.ClientCache<Integer, Object> dst = client.cache("thick-to-thin").withKeepBinary();

            assertEquals("thin-group", dst.getConfiguration().getGroupName());
            assertEquals(1, dst.getConfiguration().getBackups());
            assertEquals(41, dst.sizeLong());

            for (int key = 0; key < 40; key++)
                assertEquals("value-" + key, dst.get(key));

            assertEquals("binary", ((BinaryObject)dst.get(100)).field("value"));
        }

        CacheDataRow srcRow = row(grid(src.affinity("thick-to-thin").mapKeyToNode(100)), "thick-to-thin", 100);
        CacheDataRow destRow = row(grid(dest.affinity("thick-to-thin").mapKeyToNode(100)), "thick-to-thin", 100);

        assertEquals(expireTime, destRow.expireTime());
        assertEquals(srcRow.version().conflictVersion(), destRow.version().conflictVersion());
    }

    /** A thick source can retain absolute expiry through a thin destination when source versions are skipped. */
    @Test
    public void testThickToThinSkipVersionsPreservesExpiry() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("thin-expiry")
            .setAffinity(new RendezvousAffinityFunction(false, 8)));
        GridCacheContext<Object, Object> srcCtx = src.cachex("thin-expiry").context();
        long expireTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
        GridCacheVersion ver = new GridCacheVersion(1, 42, 1, (byte)1);

        src.cachex("thin-expiry").putAllConflict(Collections.singletonMap(srcCtx.toCacheKeyObject(1),
            new GridCacheDrExpirationInfo(srcCtx.toCacheObject("value"), ver, 0, expireTime)));

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheData().skipConflictVersion(true);

        IgniteToIgniteMigration.start(ThickClientMigrationEndpoint.create(
            getConfiguration("source-reader").setClientMode(true)), thinEndpoint("dest-0"), cfg.logger(log));

        CacheDataRow destRow = row(dest, "thin-expiry", 1);
        long expiryDiff = Math.abs(expireTime - destRow.expireTime());

        assertEquals("value", dest.cache("thin-expiry").get(1));
        assertTrue("Thin cache expiry drifted [source=" + expireTime + ", destination=" + destRow.expireTime() + ']',
            expiryDiff < TimeUnit.SECONDS.toMillis(1));
        assertNull(destRow.version().otherClusterVersion());
    }

    /** Thin source can migrate the exposed cache configuration and records to a thick-client destination. */
    @Test
    public void testThinToThickClient() throws Exception {
        startClusters();

        IgniteCache<Integer, Object> cache = src.createCache(new CacheConfiguration<Integer, Object>("thin-to-thick")
            .setBackups(1).setGroupName("thin-source-group")
            .setAffinity(new RendezvousAffinityFunction(false, 16)));

        for (int key = 0; key < 40; key++)
            cache.put(key, "value-" + key);

        cache.put(100, src.binary().builder("ThinSourceValue").setField("value", "binary").build());

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheData().batchSize(5).readThreads(2).writeThreads(2).useDataStreamer(true)
            .skipConflictVersion(true).skipExpireTime(true);

        IgniteToIgniteMigration.start(
            thinEndpoint("source-0"),
            ThickClientMigrationEndpoint.create(getConfiguration("dest-writer").setClientMode(true)), cfg.logger(log));

        IgniteCache<Integer, Object> dst = dest.cache("thin-to-thick");

        assertEquals("thin-source-group", dst.getConfiguration(CacheConfiguration.class).getGroupName());
        assertEquals(1, dst.getConfiguration(CacheConfiguration.class).getBackups());
        assertEquals(41, dst.size());

        for (int key = 0; key < 40; key++)
            assertEquals("value-" + key, dst.get(key));

        assertEquals("binary", ((BinaryObject)dst.withKeepBinary().get(100)).field("value"));
    }

    /** Thin cache configuration preserves the exposed SQL QueryEntities when copied to a thick destination. */
    @Test
    public void testThinSqlConfigurationToThickClient() throws Exception {
        startClusters();

        sql(src, "CREATE TABLE THIN_SQL (ID INT PRIMARY KEY, VAL VARCHAR) WITH \"BACKUPS=0\"");
        sql(src, "INSERT INTO THIN_SQL VALUES (1, 'one'), (2, 'two')");

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheData().skipConflictVersion(true);

        IgniteToIgniteMigration.start(thinEndpoint("source-0"),
            ThickClientMigrationEndpoint.create(getConfiguration("dest-writer").setClientMode(true)), cfg.logger(log));

        assertEquals(new HashSet<>(sql(src, "SELECT ID, VAL FROM THIN_SQL")),
            new HashSet<>(sql(dest, "SELECT ID, VAL FROM THIN_SQL")));
        assertTrue(dest.cacheNames().stream().filter(name -> name.contains("THIN_SQL"))
            .map(name -> dest.cache(name).getConfiguration(CacheConfiguration.class))
            .anyMatch(cacheCfg -> !cacheCfg.getQueryEntities().isEmpty()));
    }

    /** Both endpoints can use thin clients for regular cache data. */
    @Test
    public void testThinToThinClient() throws Exception {
        startClusters();

        IgniteCache<Integer, String> cache = src.createCache(new CacheConfiguration<Integer, String>("thin-to-thin")
            .setAffinity(new RendezvousAffinityFunction(false, 8)));

        for (int key = 0; key < 25; key++)
            cache.put(key, "value-" + key);

        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();
        cfg.cacheData().batchSize(3).writeThreads(2);
        cfg.transformer(CacheDataTransferrer.class,
            entry -> Integer.valueOf(3).equals(entry.key()) ? null : entry);

        IgniteToIgniteMigration.start(thinEndpoint("source-0"), thinEndpoint("dest-0"), cfg.logger(log));

        try (IgniteClient client = thinClient("dest-0")) {
            org.apache.ignite.client.ClientCache<Integer, String> dst = client.cache("thin-to-thin");

            assertEquals(24, dst.sizeLong());

            for (int key = 0; key < 25; key++)
                assertEquals(key == 3 ? null : "value-" + key, dst.get(key));
        }
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
            "--skip-conflict-version", "--skip-expire-time", "--batch-size", "3",
            "--use-data-streamer", "--allow-overwrite", "--per-node-buffer-size", "7",
            "--per-node-parallel-operations", "2", "--per-thread-buffer-size", "2",
            "--timeout", "5000", "--auto-flush-frequency", "100",
            "--cache-config-transformer", RenameTransformer.class.getName(),
            "--cache-config-transformer-parameters", "prefix=copy-"));

        assertEquals("value", dest.cache("copy-cli").get(1));
    }

    /** The CLI also accepts the CacheConfiguration Function transformer variant. */
    @Test
    public void testCommandLineFunctionTransformer() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("function-cli")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        assertEquals(0, IgniteToIgniteMigrationCommandLine.run(
            "--src-client-node-config", getClass().getResource("/migration/source.xml").toExternalForm(),
            "--dest-client-node-config", getClass().getResource("/migration/destination.xml").toExternalForm(),
            "--skip-conflict-version", "--cache-config-transformer", FunctionRenameTransformer.class.getName(),
            "--cache-config-transformer-parameters", "prefix=function-"));

        assertEquals("value", dest.cache("function-function-cli").get(1));
    }

    /** Thin-client address options launch and run a cache migration through the CLI. */
    @Test
    public void testThinClientCommandLine() throws Exception {
        startClusters();

        src.createCache(new CacheConfiguration<Integer, String>("thin-cli")
            .setAffinity(new RendezvousAffinityFunction(false, 8))).put(1, "value");

        assertEquals(0, IgniteToIgniteMigrationCommandLine.run(
            "--src-thin", thinAddress("source-0"),
            "--dest-thin", thinAddress("dest-0"),
            "--batch-size", "3"));

        assertEquals("value", dest.cache("thin-cli").get(1));
    }

    /** Streamer buffers from several writers must be flushed before the runner returns. */
    @Test
    public void testDataStreamer() throws Exception {
        startClusters();

        IgniteCache<Integer, String> cache = src.createCache(new CacheConfiguration<Integer, String>("streamer")
            .setAffinity(new RendezvousAffinityFunction(false, 8)));

        for (int key = 0; key < 150; key++)
            cache.put(key, "value-" + key);

        IgniteToIgniteMigrationConfiguration strm = new IgniteToIgniteMigrationConfiguration();
        strm.cacheData().useDataStreamer(true).skipConflictVersion(true)
            .skipExpireTime(true).writeThreads(2).readThreads(2).batchSize(3).queueCapacity(1)
            .dataStreamerConfiguration(streamer -> {
                streamer.allowOverwrite(true);
                streamer.perNodeBufferSize(7);
                streamer.perThreadBufferSize(2);
            });
        migrate(strm);

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

        IgniteToIgniteMigrationConfiguration strm2 = new IgniteToIgniteMigrationConfiguration();
        strm2.cacheData().useDataStreamer(true);

        Throwable err = assertThrows(log, () -> {
            migrate(strm2);

            return null;
        }, IgniteException.class, "Migration worker failed.");

        assertTrue(rootCause(err).getMessage().contains("DataStreamer cannot preserve entry metadata"));
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
        }, IgniteException.class, "Migration worker failed.");

        assertTrue(rootCause(err).getMessage().contains("Conflict resolution must be configured"));
        assertEquals(0, dest.cache("versions").size());
    }

    /** Source and destination must never resolve to the same cluster. */
    @Test
    public void testSameCluster() throws Exception {
        startClusters();

        assertThrows(log, () -> {
            IgniteToIgniteMigration.start(
                ThickClientMigrationEndpoint.create(getConfiguration("source-reader").setClientMode(true)),
                ThickClientMigrationEndpoint.create(getConfiguration("source-writer").setClientMode(true)),
                new IgniteToIgniteMigrationConfiguration().logger(log));

            return null;
        }, IgniteCheckedException.class, "Source and destination must be different clusters.");

        assertEquals(2, src.cluster().nodes().size());
    }

    /** Same-cluster detection also compares node IDs when thick and thin client endpoints are mixed. */
    @Test
    public void testSameClusterMixedClients() throws Exception {
        startClusters();

        assertThrows(log, () -> {
            IgniteToIgniteMigration.start(
                ThickClientMigrationEndpoint.create(getConfiguration("source-reader").setClientMode(true)),
                thinEndpoint("source-0"),
                new IgniteToIgniteMigrationConfiguration().logger(log));

            return null;
        }, IgniteCheckedException.class, "Source and destination must be different clusters.");

        assertEquals(2, src.cluster().nodes().size());
    }

    /** Group filtering migrates selected data; metastorage views are copied with the default filter disabled. */
    @Test
    public void testPartialSqlMigration() throws Exception {
        startClusters();

        sql(src, "CREATE TABLE INCLUDED (ID INT PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_GROUP=selected\"");
        sql(src, "CREATE TABLE EXCLUDED (ID INT PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_GROUP=excluded\"");
        sql(src, "CREATE VIEW EXCLUDED_VIEW AS SELECT ID FROM EXCLUDED");
        sql(src, "INSERT INTO INCLUDED VALUES (1, 'value')");

        IgniteToIgniteMigrationConfiguration ps = new IgniteToIgniteMigrationConfiguration();
        ps.cacheData().skipConflictVersion(true);
        ps.cacheMigration().cacheGroupNames("selected");
        migrate(ps);

        assertEquals(sql(src, "SELECT ID FROM INCLUDED"), sql(dest, "SELECT ID FROM INCLUDED"));

        // The destination has a SQL schema (the selected table), so the view is portable and is
        // copied. A view referencing a table that was not migrated is registered against the schema
        // and does not fail the migration.
        AtomicInteger views = new AtomicInteger();

        dest.context().distributedMetastorage().iterate("ignite.internal.sql.view.", (key, val) -> views.incrementAndGet());

        assertEquals(1, views.get());
    }

    /** A view is not portable by default and is skipped by the default filter; migration does not fail. */
    @Test
    public void testViewWithoutSchemaDoesNotFail() throws Exception {
        startClusters();

        sql(src, "CREATE TABLE ORPHANED (ID INT PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_GROUP=selected\"");
        sql(src, "CREATE VIEW ORPHANED_VIEW AS SELECT ID FROM ORPHANED");

        // Select a group that has no caches, so the destination receives no SQL tables or schema.
        IgniteToIgniteMigrationConfiguration os = new IgniteToIgniteMigrationConfiguration();
        os.cacheData().skipConflictVersion(true);
        os.cacheMigration().cacheGroupNames("empty-group");
        migrate(os);

        // The SQL table (and its PUBLIC schema) was not migrated.
        assertNull(dest.cache("ORPHANED"));

        // The default filter skipped the view record.
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

        IgniteToIgniteMigrationConfiguration sd = new IgniteToIgniteMigrationConfiguration();
        sd.cacheData().skipConflictVersion(true);
        sd.cacheMigration().moveDataStructures(false);
        migrate(sd);

        assertEquals("value", dest.cache("user-cache").get(1));
        assertNull(dest.atomicLong("counter", 0, false));
    }

    /** Runtime node/thread ownership cannot be transferred to a different cluster. */
    @Test
    public void testAcquiredLock() throws Exception {
        startClusters();

        IgniteLock lock = src.reentrantLock("lock", true, true, true);

        lock.lock();

        IgniteToIgniteMigrationConfiguration lockCfg = new IgniteToIgniteMigrationConfiguration();
        lockCfg.cacheData().skipConflictVersion(true);

        try {
            Throwable err = assertThrows(log, () -> {
                migrate(lockCfg);

                return null;
            }, IgniteException.class, "Migration worker failed.");

            assertTrue(rootCause(err).getMessage().contains("Cannot migrate a lock with an owner"));
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
            ThickClientMigrationEndpoint.create(getConfiguration("source-reader").setClientMode(true)),
            ThickClientMigrationEndpoint.create(getConfiguration("dest-writer").setClientMode(true)),
            cfg.logger(log)
        );
    }

    /** Creates an owned thin migration endpoint connected to a test cluster. */
    private ThinClientMigrationEndpoint thinEndpoint(String nodeName) {
        return ThinClientMigrationEndpoint.create(thinConfiguration(nodeName));
    }

    /** Thin-client listener address for the named test cluster. */
    private String thinAddress(String nodeName) {
        IgniteEx server = grid(nodeName);

        return "127.0.0.1:" + server.context().clientListener().port();
    }

    /** Opens a temporary thin client for test assertions. */
    private IgniteClient thinClient(String nodeName) {
        return Ignition.startClient(thinConfiguration(nodeName));
    }

    /** Thin-client configuration targeting a server in the named cluster. */
    private ClientConfiguration thinConfiguration(String nodeName) {
        return new ClientConfiguration().setAddresses(thinAddress(nodeName));
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
    public static class RenameTransformer
        implements BiFunction<IgniteConfiguration, CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> {
        /** */
        private String prefix;

        /** */
        public void configure(Map<String, String> parameters) {
            prefix = parameters.get("prefix");
        }

        /** {@inheritDoc} */
        @Override public CacheConfiguration<?, ?> apply(IgniteConfiguration destCfg, CacheConfiguration<?, ?> cfg) {
            assertTrue(destCfg.isClientMode());

            return cfg.setName(prefix + cfg.getName());
        }
    }

    /** Finds a class-name mapping in the node's local marshaller context snapshot. */
    private static MigrationTypeMapping typeMapping(IgniteEx node, String typeName) {
        List<Map<Integer, MappedName>> mappings = node.context().marshallerContext().getCachedMappings();

        for (int platform = 0; platform < mappings.size(); platform++) {
            for (Map.Entry<Integer, MappedName> entry : mappings.get(platform).entrySet()) {
                if (typeName.equals(entry.getValue().className()))
                    return new MigrationTypeMapping((byte)platform, entry.getKey(), typeName);
            }
        }

        return null;
    }

    /** Gets the deepest cause in an exception chain. */
    private static Throwable rootCause(Throwable err) {
        while (err.getCause() != null)
            err = err.getCause();

        return err;
    }

    /** CLI transformer implementing the source-compatible Function variant. */
    public static class FunctionRenameTransformer
        implements Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> {
        /** */
        private String prefix;

        /** Configures this transformer from CLI parameters. */
        public void configure(Map<String, String> parameters) {
            prefix = parameters.get("prefix");
        }

        /** {@inheritDoc} */
        @Override public CacheConfiguration<?, ?> apply(CacheConfiguration<?, ?> cfg) {
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
