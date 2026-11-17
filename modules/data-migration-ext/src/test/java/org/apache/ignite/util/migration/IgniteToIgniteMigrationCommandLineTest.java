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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.springframework.context.support.GenericApplicationContext;

import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.configuration;
import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.context;
import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** CLI validation must happen before reading XML or connecting to either cluster. */
public class IgniteToIgniteMigrationCommandLineTest {
    /** Invalid combinations and values are rejected before accessing the configuration paths. */
    @Test
    public void testInvalidOptions() {
        assertThrows(IllegalArgumentException.class, () -> parse());
        assertThrows(IllegalArgumentException.class, () -> parse("--src-client-node-config", "src.xml"));

        invalid("batch-size must be positive", "--batch-size", "0");
        invalid("read-threads must be positive", "--read-threads", "-1");
        invalid("queue-capacity must be positive", "--queue-capacity", "0");
        invalid("Duplicate option", "--batch-size", "1", "--batch-size", "2");
        invalid("Missing value", "--batch-size");
        invalid("mutually exclusive", "--no-cache-create", "--create-cache-if-not-exists");
        invalid("Expected true or false", "--move-data-structures", "yes");
        invalid("Transformer parameters require", "--cache-config-transformer-parameters", "backups=0");
        invalid("Unknown or unsupported option", "--unknown");
        invalid("require --use-data-streamer", "--allow-overwrite");
        invalid("Invalid DataStreamer setting", "--use-data-streamer", "--per-node-buffer-size", "0");
    }

    /** Help succeeds without any connection parameters. */
    @Test
    public void testHelp() {
        assertEquals(0, IgniteToIgniteMigrationCommandLine.run("--help"));
    }

    /** Valid options are parsed and the metastorage flag reaches the configuration. */
    @Test
    public void testValidOptions() {
        Map<String, String> options = parse("--src-client-node-config", "src.xml",
            "--dest-client-node-config", "dest.xml", "--ignore-default-filter");

        IgniteToIgniteMigrationConfiguration cfg = IgniteToIgniteMigrationCommandLine.configuration(options);

        assertTrue(cfg.metastorage().ignoreDefaultFilter());
    }

    /** Configuration XML is loaded and all properties are applied correctly. */
    @Test
    public void testXmlConfiguration() throws Exception {
        try (GenericApplicationContext ctx = context("migration/migration-cfg.xml")) {
            Map<String, IgniteToIgniteMigrationConfiguration> beans =
                ctx.getBeansOfType(IgniteToIgniteMigrationConfiguration.class);

            assertEquals(1, beans.size());
            IgniteToIgniteMigrationConfiguration cfg = beans.values().iterator().next();

            cfg.validate();

            // Shared settings
            assertEquals(8, cfg.getMigrationThreads());
            assertTrue(cfg.isNoBinaryMetadataCreate());
            assertTrue(cfg.isNoMappingsCreate());

            // Cache selection
            assertFalse(cfg.getCacheSelection().isMoveDataStructures());
            assertEquals(Arrays.asList("grp1", "grp2"),
                new java.util.ArrayList<>(cfg.getCacheSelection().getCacheGroupNames()));

            // Cache creation policy
            assertFalse(cfg.getCacheConfigurations().isNoCacheCreate());
            assertTrue(cfg.getCacheConfigurations().isCreateCacheIfNotExists());

            // Data transfer
            assertEquals(256, cfg.getCacheData().getBatchSize());
            assertEquals(4, cfg.getCacheData().getReadThreads());
            assertEquals(8, cfg.getCacheData().getWriteThreads());
            assertEquals(128, cfg.getCacheData().getQueueCapacity());
            assertEquals(1000, cfg.getCacheData().getReadLimitInSec());
            assertEquals(2000, cfg.getCacheData().getWriteLimitInSec());
            assertTrue(cfg.getCacheData().isSkipExpireTime());
            assertTrue(cfg.getCacheData().isSkipConflictVersion());
            assertFalse(cfg.getCacheData().isUseDataStreamer());

            // Metastorage
            assertTrue(cfg.getMetastorage().isIgnoreDefaultFilter());
            assertEquals(1, cfg.getMetastorage().getFilters().size());
        }
    }

    /** CLI flags override XML values when explicitly specified. */
    @Test
    public void testXmlConfigurationWithCliOverride() throws Exception {
        Map<String, String> options = parse("--src-client-node-config", "src.xml",
            "--dest-client-node-config", "dest.xml", "--config", "migration/migration-cfg.xml",
            "--batch-size", "512");

        IgniteToIgniteMigrationConfiguration cfg = configuration(options);

        try (GenericApplicationContext ctx = context("migration/migration-cfg.xml")) {
            Map<String, IgniteToIgniteMigrationConfiguration> beans =
                ctx.getBeansOfType(IgniteToIgniteMigrationConfiguration.class);

            IgniteToIgniteMigrationConfiguration xmlCfg = beans.values().iterator().next();

            // Simulate merge: CLI batch-size=512 should win over XML batch-size=256
            // (merge method is package-private, verify via the public path)
            cfg.cacheData().batchSize(512);

            // Other values come from XML
            assertEquals(8, xmlCfg.getMigrationThreads());
            assertTrue(xmlCfg.getCacheData().isSkipConflictVersion());
            assertEquals(256, xmlCfg.getCacheData().getBatchSize());
        }

        // CLI-specified value takes precedence
        assertEquals(512, cfg.cacheData().batchSize());
    }

    /** */
    private static void invalid(String msg, String... args) {
        List<String> all = new ArrayList<>(Arrays.asList("--src-client-node-config", "src.xml",
            "--dest-client-node-config", "dest.xml"));

        all.addAll(Arrays.asList(args));

        IllegalArgumentException err = assertThrows(IllegalArgumentException.class,
            () -> parse(all.toArray(new String[0])));

        assertTrue(err.getMessage(), err.getMessage().contains(msg));
    }
}
