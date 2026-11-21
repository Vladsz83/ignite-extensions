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

import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;
import org.springframework.context.support.GenericApplicationContext;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.context;
import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.nodeConfiguration;
import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.thinConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Spring XML configuration loading for client-node connections. */
public class IgniteToIgniteMigrationXmlConfigTest {
    /** A valid XML with one IgniteConfiguration bean loads and produces a client-mode configuration. */
    @Test
    public void testValidXml() throws Exception {
        try (GenericApplicationContext ctx = context("migration/source.xml")) {
            IgniteConfiguration cfg = nodeConfiguration(ctx, "source");

            assertTrue(cfg.isClientMode());
            assertEquals(TcpDiscoverySpi.class, cfg.getDiscoverySpi().getClass());

            TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();
            assertTrue(disco.getIpFinder() instanceof TcpDiscoveryVmIpFinder);
        }
    }

    /** XML without an IgniteConfiguration bean is rejected. */
    @Test
    public void testNoIgniteConfiguration() throws Exception {
        try (GenericApplicationContext ctx = context("migration/no-ignite.xml")) {
            assertThrows(
                null,
                () -> nodeConfiguration(ctx, "source"),
                IllegalArgumentException.class,
                "Expected exactly one IgniteConfiguration bean for source.");
        }
    }

    /** XML with multiple IgniteConfiguration beans is rejected. */
    @Test
    public void testTwoIgniteConfigurations() throws Exception {
        try (GenericApplicationContext ctx = context("migration/two-ignite.xml")) {
            assertThrows(
                null,
                () -> nodeConfiguration(ctx, "destination"),
                IllegalArgumentException.class,
                "Expected exactly one IgniteConfiguration bean for destination.");
        }
    }

    /** Thin-client XML requires exactly one ClientConfiguration bean and loads its addresses. */
    @Test
    public void testThinClientXml() throws Exception {
        try (GenericApplicationContext ctx = context("migration/source-thin.xml")) {
            ClientConfiguration cfg = thinConfiguration(java.util.Collections.emptyMap(), "src", ctx);

            assertEquals("127.0.0.1:10800", cfg.getAddresses()[0]);
        }
    }

    /** Thin-client connection XML rejects zero or multiple ClientConfiguration beans. */
    @Test
    public void testInvalidThinClientXml() throws Exception {
        try (GenericApplicationContext ctx = context("migration/no-ignite.xml")) {
            assertThrows(null, () -> thinConfiguration(java.util.Collections.emptyMap(), "src", ctx),
                IllegalArgumentException.class,
                "Expected exactly one ClientConfiguration bean for src thin-client configuration");
        }

        try (GenericApplicationContext ctx = context("migration/two-thin.xml")) {
            assertThrows(null, () -> thinConfiguration(java.util.Collections.emptyMap(), "dest", ctx),
                IllegalArgumentException.class,
                "Expected exactly one ClientConfiguration bean for dest thin-client configuration");
        }
    }
}
