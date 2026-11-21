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
import java.util.function.Function;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.util.migration.category.cache.data.CacheDataMigrationConfiguration;
import org.junit.Test;
import org.springframework.context.support.GenericApplicationContext;

import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.configuration;
import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.context;
import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
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
        invalid("read-threads must be positive", "--read-threads", "0");
        invalid("read-threads must be positive", "--read-threads", "-1");
        invalid("write-threads must be positive", "--write-threads", "0");
        invalid("write-threads must be positive", "--write-threads", "-1");
        invalid("non-negative", "--read-limit-in-sec", "-1");
        invalid("non-negative", "--write-limit-in-sec", "-1");
        invalid("queue-capacity must be positive", "--queue-capacity", "0");
        invalid("Duplicate option", "--batch-size", "1", "--batch-size", "2");
        invalid("Missing value", "--batch-size");
        invalid("mutually exclusive", "--no-cache-create", "--create-cache-if-not-exists");
        invalid("Cache group names must not be empty", "--cache-group-names", "accounts,,reference");
        assertThrows(IllegalArgumentException.class, () -> parse(
            "--src-client-node-config", "src.xml", "--dest-client-node-config", "dest.xml",
            "--skip-conflict-version", "--skip-cdc-info"));
        invalid("Expected true or false", "--move-data-structures", "yes");
        invalid("Transformer parameters require", "--cache-config-transformer-parameters", "backups=0");
        invalid("Unknown or unsupported option", "--unknown");
        invalid("require --use-data-streamer", "--allow-overwrite");
        invalid("Invalid DataStreamer setting", "--use-data-streamer", "--per-node-buffer-size", "0");
        invalid("Invalid DataStreamer setting", "--use-data-streamer", "--per-node-parallel-operations", "0");
        invalid("Invalid DataStreamer setting", "--use-data-streamer", "--per-thread-buffer-size", "0");
        invalid("Invalid DataStreamer setting", "--use-data-streamer", "--timeout", "-2");
        invalid("Invalid DataStreamer setting", "--use-data-streamer", "--auto-flush-frequency", "-1");
        assertThrows(IllegalArgumentException.class, () -> parse(
            "--src-client-node-config", "src.xml", "--src-thin", "127.0.0.1:10800",
            "--dest-thin", "127.0.0.1:10801"));
        assertThrows(IllegalArgumentException.class, () -> parse(
            "--src-thin", "127.0.0.1:10800", "--dest-client-node-config", "dest.xml",
            "--dest-thin", "127.0.0.1:10801"));
        assertThrows(IllegalArgumentException.class, () -> parse(
            "--src-thin", "127.0.0.1:10800", "--dest-thin", "127.0.0.1:10801", "--use-data-streamer"));
    }

    /** DataStreamer requires a thick-client destination, including when the thin configuration is loaded from XML. */
    @Test
    public void testDataStreamerRequiresThickDestination() {
        IllegalArgumentException err = assertThrows(IllegalArgumentException.class, () -> parse(
            "--src-thin", "127.0.0.1:10800", "--dest-thin-config", "destination.xml", "--use-data-streamer"));

        assertEquals("DataStreamer requires --dest-client-node-config.", err.getMessage());
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
        assertEquals(256, cfg.cacheData().batchSize());
        assertEquals(1, cfg.cacheData().readThreads());
        assertEquals(1, cfg.cacheData().writeThreads());
        assertFalse(cfg.cacheData().isReadThreadsConfigured());
        assertFalse(cfg.cacheData().isWriteThreadsConfigured());
        assertEquals(CacheDataMigrationConfiguration.DFLT_QUEUE_CAPACITY, cfg.cacheData().queueCapacity());

        IgniteToIgniteMigrationConfiguration aliasCfg = IgniteToIgniteMigrationCommandLine.configuration(parse(
            "--src-client-node-config", "src.xml", "--dest-client-node-config", "dest.xml", "--skip-cdc-info"));

        assertTrue(aliasCfg.cacheData().skipConflictVersion());

        Map<String, String> thinOptions = parse("--src-thin", "127.0.0.1:10800,127.0.0.1:10801",
            "--dest-thin-config", "dest-thin.xml");

        assertTrue(thinOptions.containsKey("src-thin"));
        assertEquals("dest-thin.xml", thinOptions.get("dest-thin-config"));

        IgniteToIgniteMigrationConfiguration explicit = configuration(parse(
            "--src-client-node-config", "src.xml", "--dest-client-node-config", "dest.xml", "--read-threads", "3"));

        assertTrue(explicit.cacheData().isReadThreadsConfigured());
        assertFalse(explicit.cacheData().isWriteThreadsConfigured());
        assertEquals(3, explicit.cacheData().readThreads());
    }

    /** Reader and writer counts have independent single-thread defaults and explicit overrides. */
    @Test
    public void testCacheWorkerThreadDefaultsAndOverrides() {
        CacheDataMigrationConfiguration cfg = new CacheDataMigrationConfiguration();

        assertEquals(1, cfg.readThreads());
        assertEquals(1, cfg.writeThreads());
        assertFalse(cfg.isReadThreadsConfigured());
        assertFalse(cfg.isWriteThreadsConfigured());

        cfg.readThreads(3);

        assertEquals(3, cfg.readThreads());
        assertEquals(1, cfg.writeThreads());
        assertTrue(cfg.isReadThreadsConfigured());
        assertFalse(cfg.isWriteThreadsConfigured());

        cfg = new CacheDataMigrationConfiguration().writeThreads(2);

        assertEquals(1, cfg.readThreads());
        assertEquals(2, cfg.writeThreads());
        assertFalse(cfg.isReadThreadsConfigured());
        assertTrue(cfg.isWriteThreadsConfigured());

        cfg = new CacheDataMigrationConfiguration().readThreads(3).writeThreads(5);

        assertEquals(3, cfg.readThreads());
        assertEquals(5, cfg.writeThreads());
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
            assertTrue(cfg.getCacheData().isReadThreadsConfigured());
            assertTrue(cfg.getCacheData().isWriteThreadsConfigured());
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
            "--batch-size", "512", "--read-threads", "2");

        IgniteToIgniteMigrationConfiguration cfg = configuration(options);

        try (GenericApplicationContext ctx = context("migration/migration-cfg.xml")) {
            Map<String, IgniteToIgniteMigrationConfiguration> beans =
                ctx.getBeansOfType(IgniteToIgniteMigrationConfiguration.class);

            IgniteToIgniteMigrationConfiguration xmlCfg = beans.values().iterator().next();

            Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> transformer = Function.identity();
            xmlCfg.cacheSelection().cacheConfigurationTransformer(transformer);

            IgniteToIgniteMigrationCommandLine.merge(cfg, xmlCfg, options);

            assertTrue(cfg.getCacheData().isSkipConflictVersion());
            assertEquals(2, cfg.getCacheData().getReadThreads());
            assertEquals(8, cfg.getCacheData().getWriteThreads());
            assertEquals(128, cfg.getCacheData().getQueueCapacity());
            assertEquals(1, cfg.getMetastorage().getFilters().size());
            assertSame(transformer, cfg.getCacheSelection().getCacheConfigurationTransformer());
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
