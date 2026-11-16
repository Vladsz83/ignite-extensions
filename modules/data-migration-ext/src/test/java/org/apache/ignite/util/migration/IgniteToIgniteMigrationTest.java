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

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.impl.ThickClientMigrationOrigin;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** End-to-end migration between isolated clusters with different storage and affinity topologies. */
public class IgniteToIgniteMigrationTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx src;

    /** */
    private IgniteEx dest;

    /** */
    private boolean persistentDestination;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);
        int port = name.startsWith("dest") ? 47600 : 47500;

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setLocalPort(port).setLocalPortRange(10)
            .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Collections.singleton("127.0.0.1:" + port))));
        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(true);
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

    /** Binary records retain expiry and original versions across different partition counts and storage modes. */
    @Test
    public void testBinaryMetadataExpiryVersionsAndAffinity() throws Exception {
        persistentDestination = true;
        startClusters();

        for (CacheAtomicityMode mode : CacheAtomicityMode.values()) {
            String name = "records-" + mode;
            IgniteCache<Object, Object> cache = src.createCache(new CacheConfiguration<>(name)
                .setAtomicityMode(mode).setBackups(1).setAffinity(new RendezvousAffinityFunction(false, 8)));

            for (int i = 0; i < 40; i++) {
                BinaryObject key = src.binary().builder("MigrationKey").setField("id", i).build();
                BinaryObject val = src.binary().builder("MigrationValue").setField("name", "value-" + i).build();

                cache.withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MINUTES, 5))).put(key, val);
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
                CacheDataRow sourceRow = row(primary, name, key);
                CacheDataRow destRow = row(dest, copy.getName(), key);

                assertEquals(sourceRow.expireTime(), destRow.expireTime());
                CacheEntryVersion ver = destRow.version().otherClusterVersion();
                assertNotNull(ver);
                assertEquals(sourceRow.version(), ver);
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
            .setNoCacheCreate(true).setSkipCdcInfo(true).setBatchSize(2));

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
            ThickClientMigrationOrigin.create(getConfiguration("dest-writer").setClientMode(true)), cfg.setLogger(log));
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
}
