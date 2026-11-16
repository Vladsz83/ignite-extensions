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

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.jdbc.model.Person;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;

/** */
public class IgniteToIgniteMigrationTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_PER_CLUSTER = 2;

    /** */
    private static int PORT_RANGE = 10;

    /** Discovery ports to split the cluster. */
    private int discPort = TcpDiscoverySpi.DFLT_PORT;

    /** Communication ports to split the cluster. */
    private int commPort = TcpCommunicationSpi.DFLT_PORT;

    /** Ignite instance name. */
    private String instName = "src";

    /** */
    private Ignite gridSrc;

    /** */
    private Ignite gridDest;

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return instName;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        // Also cleans separated working directories.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // IP finder to split the clusters.
        TcpDiscoveryIpFinderAdapter ipFinder = new TcpDiscoveryVmIpFinder(true).
            setAddresses(Collections.singleton("127.0.0.1:%d..%d".formatted(discPort, discPort + PORT_RANGE - 1)));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setLocalPort(discPort).setLocalPortRange(PORT_RANGE).setIpFinder(ipFinder));

        cfg.setCommunicationSpi(new TcpCommunicationSpi().setLocalPort(commPort).setLocalPortRange(PORT_RANGE));

        cfg.setWorkDirectory(U.defaultWorkDirectory() + File.separator + instName + File.separator + igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** */
    @Test
    public void test0() throws Exception {
        startClusters();

        IgniteConfiguration destCfg = getConfiguration("sender").setClientMode(true);

        discPort = TcpDiscoverySpi.DFLT_PORT;
        commPort = TcpCommunicationSpi.DFLT_PORT;

        gridSrc.createCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            gridSrc.cache(DEFAULT_CACHE_NAME).put(i, i);

        for (int i = 100; i < 200; i++) {
            gridSrc.cache(DEFAULT_CACHE_NAME).put(
                String.valueOf(i),
                new CustomData(i, "str" + i, UUID.randomUUID(), UUID.randomUUID().toString().getBytes())
            );
        }

        IgniteConfiguration srcCfg = getConfiguration("reader").setClientMode(true);

        IgniteToIgniteMigrationConfiguration mCfg = new IgniteToIgniteMigrationConfiguration(srcCfg, destCfg);

        new IgniteToIgniteMigration(mCfg).start();
    }

    /** */
    private void startClusters() throws Exception {
        gridSrc = startGridsMultiThreaded(NODES_PER_CLUSTER);

        discPort += PORT_RANGE;
        commPort += PORT_RANGE;
        instName = "dest";

        gridDest = startGridsMultiThreaded(NODES_PER_CLUSTER, NODES_PER_CLUSTER);

        // Ensure that the cluster are split.
        assertEquals(NODES_PER_CLUSTER, gridSrc.cluster().nodes().size());
        assertEquals(NODES_PER_CLUSTER, gridDest.cluster().nodes().size());
        assertTrue(null == gridSrc.cluster().node(gridDest.cluster().id()));
        assertTrue(null == gridDest.cluster().node(gridSrc.cluster().id()));
    }

    /** */
    private static class CustomData {
        /** */
        int iVal;

        /** */
        String sVal;

        /** */
        UUID uuid;

        /** */
        byte[] bArr;

        /** */
        public CustomData(int iVal, String sVal, UUID uuid, byte[] bArr) {
            this.iVal = iVal;
            this.sVal = sVal;
            this.uuid = uuid;
            this.bArr = bArr;
        }
    }
}
