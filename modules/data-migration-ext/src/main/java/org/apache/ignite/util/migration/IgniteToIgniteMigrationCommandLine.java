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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.category.cache.configuration.CacheMigrationConfiguration;
import org.apache.ignite.util.migration.category.cache.data.CacheDataMigrationConfiguration;
import org.apache.ignite.util.migration.endpoint.ThickClientMigrationEndpoint;
import org.apache.ignite.util.migration.endpoint.ThinClientMigrationEndpoint;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;

import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.startup.cmdline.CommandLineStartup.isHelp;

/** Command-line entry point. */
public class IgniteToIgniteMigrationCommandLine {
    /** Whether startup banner output is suppressed. */
    private static final boolean QUIET = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_QUIET);

    /** Boolean switches. */
    private static final Set<String> FLAGS = new HashSet<>(Arrays.asList(
        "create-cache-if-not-exists", "no-binary-metadata-create", "no-binary-metdata-create",
        "no-mappings-create", "skip-expire-time", "skip-conflict-version", "skip-cdc-info", "use-data-streamer",
        "allow-overwrite", "ignore-default-filter"));

    /** Data streamer settings. */
    private static final Set<String> STREAMER_OPTIONS = new HashSet<>(Arrays.asList(
        "allow-overwrite", "per-node-buffer-size", "per-node-parallel-operations", "per-thread-buffer-size",
        "timeout", "auto-flush-frequency"));

    /** Options requiring a value. */
    private static final Set<String> VALUES = new HashSet<>(Arrays.asList(
        "src-client-node-config", "src-thin", "src-thin-config", "dest-client-node-config", "dest-thin",
        "dest-thin-config", "config", "cache-group-names", "batch-size",
        "read-threads", "write-threads", "read-limit-in-sec", "write-limit-in-sec", "queue-capacity",
        "cache-config-transformer", "cache-config-transformer-parameters", "destination",
        "move-data-structures", "per-node-buffer-size", "per-node-parallel-operations", "per-thread-buffer-size",
        "timeout", "auto-flush-frequency"));

    /**
     * Main entry point.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        System.exit(run(args));
    }

    /**
     * @param args Command line arguments.
     * @return Process exit code.
     */
    public static int run(String... args) {
        if (args.length == 1 && (isHelp(args[0]) || "--help".equals(args[0]))) {
            usage();

            return 0;
        }

        try {
            Map<String, String> options = parse(args);

            IgniteToIgniteMigrationConfiguration cfg = configuration(options);

            if (options.containsKey("config")) {
                GenericApplicationContext cfgCtx = context(options.get("config"));

                try {
                    IgniteToIgniteMigrationConfiguration xmlCfg = bean(cfgCtx,
                        IgniteToIgniteMigrationConfiguration.class, "migration configuration");
                    merge(cfg, xmlCfg, options);
                }
                finally {
                    cfgCtx.close();
                }
            }

            if (!QUIET) {
                X.println("Apache Ignite Data Migration, ver. " + VER);
                X.println(COPYRIGHT);
                X.println();
            }

            if (cfg.cacheData().useDataStreamer() && !options.containsKey("dest-client-node-config"))
                throw new IllegalArgumentException("DataStreamer requires --dest-client-node-config.");

            try (GenericApplicationContext srcCtx = connectionContext(options, "src");
                 GenericApplicationContext destCtx = connectionContext(options, "dest")) {
                IgniteConfiguration srcCfg = options.containsKey("src-client-node-config") ?
                    nodeConfiguration(srcCtx, "source") : null;
                IgniteConfiguration destCfg = options.containsKey("dest-client-node-config") ?
                    nodeConfiguration(destCtx, "destination") : null;
                ClientConfiguration srcClientCfg = options.containsKey("src-thin") ||
                    options.containsKey("src-thin-config") ? thinConfiguration(options, "src", srcCtx) : null;
                ClientConfiguration destClientCfg = destCfg == null && !options.containsKey("destination") ?
                    thinConfiguration(options, "dest", destCtx) : null;

                if (srcCfg != null)
                    U.initWorkDir(srcCfg);
                else if (srcClientCfg.getLogger() == null && destCfg != null)
                    U.initWorkDir(destCfg);

                IgniteLogger log;

                if (srcCfg != null)
                    log = U.initLogger(srcCfg, "data-migration");
                else if (srcClientCfg.getLogger() != null)
                    log = U.initLogger(srcClientCfg.getLogger(), "data-migration", UUID.randomUUID(),
                        U.defaultWorkDirectory());
                else if (destCfg != null)
                    log = U.initLogger(destCfg, "data-migration");
                else
                    log = U.initLogger(destClientCfg == null ? null : destClientCfg.getLogger(),
                        "data-migration", UUID.randomUUID(), U.defaultWorkDirectory());

                cfg.logger(log);

                if (srcClientCfg != null)
                    srcClientCfg.setLogger(log);

                if (destClientCfg != null)
                    destClientCfg.setLogger(log);

                configureTransformer(options, cfg);

                MigrationOrigin src = srcCfg != null ? ThickClientMigrationEndpoint.create(srcCfg) :
                    ThinClientMigrationEndpoint.create(srcClientCfg);
                MigrationDestination dest;

                if (destCfg != null)
                    dest = ThickClientMigrationEndpoint.create(destCfg);
                else if (options.containsKey("destination"))
                    dest = (MigrationDestination)Class.forName(options.get("destination"))
                        .getDeclaredConstructor().newInstance();
                else
                    dest = ThinClientMigrationEndpoint.create(destClientCfg);

                IgniteToIgniteMigration.start(src, dest, cfg);
            }

            return 0;
        }
        catch (Throwable e) {
            X.error("Data migration failed: " + e.getMessage());
            e.printStackTrace(System.err);

            return 1;
        }
    }

    /** Parses and validates arguments before any cluster connection or Spring bean is created. */
    static Map<String, String> parse(String... args) {
        Map<String, String> res = new LinkedHashMap<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (!arg.startsWith("--"))
                throw new IllegalArgumentException("Expected an option, got: " + arg + '.');

            String name = arg.substring(2);

            if (!FLAGS.contains(name) && !VALUES.contains(name))
                throw new IllegalArgumentException("Unknown or unsupported option: " + arg + '.');

            String val = "true";

            if (VALUES.contains(name)) {
                if (++i >= args.length || args[i].isEmpty() || args[i].startsWith("--"))
                    throw new IllegalArgumentException("Missing value for " + arg + '.');

                val = args[i];
            }

            if (res.putIfAbsent(name, val) != null)
                throw new IllegalArgumentException("Duplicate option: " + arg + '.');
        }

        int srcModes = count(res, "src-client-node-config", "src-thin", "src-thin-config");

        if (srcModes != 1)
            throw new IllegalArgumentException("Specify exactly one of --src-client-node-config, --src-thin or --src-thin-config.");

        int destModes = count(res, "dest-client-node-config", "dest-thin", "dest-thin-config", "destination");

        if (destModes != 1)
            throw new IllegalArgumentException("Specify exactly one of --dest-client-node-config, --dest-thin, " +
                "--dest-thin-config and --destination.");

        if (res.containsKey("skip-conflict-version") && res.containsKey("skip-cdc-info"))
            throw new IllegalArgumentException("Specify only one of --skip-conflict-version and --skip-cdc-info.");

        if (res.containsKey("cache-config-transformer-parameters") && !res.containsKey("cache-config-transformer"))
            throw new IllegalArgumentException("Transformer parameters require --cache-config-transformer.");

        configuration(res).validate();

        return res;
    }

    /** */
    static IgniteToIgniteMigrationConfiguration configuration(Map<String, String> options) {
        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration()
            .noBinaryMetadataCreate(options.containsKey("no-binary-metadata-create") || options.containsKey("no-binary-metdata-create"))
            .noMappingsCreate(options.containsKey("no-mappings-create"));

        CacheMigrationConfiguration mCfg = cfg.cacheMigration();

        mCfg.setCreateCacheIfNotExists(options.containsKey("create-cache-if-not-exists"));
        mCfg.setMoveDataStructures(bool(options, "move-data-structures", true));

        cfg.metastorage().setIgnoreDefaultFilter(options.containsKey("ignore-default-filter"));

        cfg.cacheData().setSkipExpireTime(options.containsKey("skip-expire-time"));
        cfg.cacheData().setSkipConflictVersion(options.containsKey("skip-conflict-version") || options.containsKey("skip-cdc-info"));
        cfg.cacheData().setBatchSize(integer(options, "batch-size", CacheDataMigrationConfiguration.DFLT_BATCH_SIZE));
        cfg.cacheData().setReadLimitInSec(integer(options, "read-limit-in-sec", 0));
        cfg.cacheData().setWriteLimitInSec(integer(options, "write-limit-in-sec", 0));
        cfg.cacheData().setQueueCapacity(integer(options, "queue-capacity", CacheDataMigrationConfiguration.DFLT_QUEUE_CAPACITY));

        if (options.containsKey("read-threads"))
            cfg.cacheData().setReadThreads(integer(options, "read-threads", 0));

        if (options.containsKey("write-threads"))
            cfg.cacheData().setWriteThreads(integer(options, "write-threads", 0));

        if (options.containsKey("cache-group-names"))
            cfg.cacheMigration().setCacheGroupNames(Arrays.asList(options.get("cache-group-names").split(",", -1)));

        configureDataStreamer(options, cfg);

        return cfg;
    }

    /** */
    private static void configureDataStreamer(Map<String, String> options, IgniteToIgniteMigrationConfiguration cfg) {
        if (!options.containsKey("use-data-streamer")) {
            if (STREAMER_OPTIONS.stream().anyMatch(options::containsKey))
                throw new IllegalArgumentException("DataStreamer settings require --use-data-streamer.");

            return;
        }

        if (!options.containsKey("dest-client-node-config"))
            throw new IllegalArgumentException("DataStreamer requires --dest-client-node-config.");

        Map<String, Integer> values = new HashMap<>();

        for (String name : STREAMER_OPTIONS) {
            if (!"allow-overwrite".equals(name) && options.containsKey(name)) {
                int val = integer(options, name, 0);
                int min = "timeout".equals(name) ? -1 : "auto-flush-frequency".equals(name) ? 0 : 1;

                if (val < min)
                    throw new IllegalArgumentException("Invalid DataStreamer setting: --" + name + '.');

                values.put(name, val);
            }
        }

        cfg.cacheData().setUseDataStreamer(true);
        cfg.cacheData().setDataStreamerConfiguration(streamer -> {
            streamer.allowOverwrite(options.containsKey("allow-overwrite"));

            if (values.containsKey("per-node-buffer-size"))
                streamer.perNodeBufferSize(values.get("per-node-buffer-size"));

            if (values.containsKey("per-node-parallel-operations"))
                streamer.perNodeParallelOperations(values.get("per-node-parallel-operations"));

            if (values.containsKey("per-thread-buffer-size"))
                streamer.perThreadBufferSize(values.get("per-thread-buffer-size"));

            if (values.containsKey("timeout"))
                streamer.timeout(values.get("timeout"));

            if (values.containsKey("auto-flush-frequency"))
                streamer.autoFlushFrequency(values.get("auto-flush-frequency"));
        });
    }

    /** */
    private static int integer(Map<String, String> options, String name, int dflt) {
        try {
            return options.containsKey(name) ? Integer.parseInt(options.get(name)) : dflt;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected an integer for --" + name + '.', e);
        }
    }

    /** */
    private static boolean bool(Map<String, String> options, String name, boolean dflt) {
        String val = options.get(name);

        if (val == null)
            return dflt;

        if (!"true".equalsIgnoreCase(val) && !"false".equalsIgnoreCase(val))
            throw new IllegalArgumentException("Expected true or false for --" + name + '.');

        return Boolean.parseBoolean(val);
    }

    /** Counts selected options. */
    private static int count(Map<String, String> options, String... names) {
        int cnt = 0;

        for (String name : names) {
            if (options.containsKey(name))
                cnt++;
        }

        return cnt;
    }

    /** Loads a connection XML context, or returns {@code null} for direct-address/custom endpoints. */
    private static GenericApplicationContext connectionContext(Map<String, String> options, String prefix) throws Exception {
        String path = options.get(prefix + "-client-node-config");

        if (path == null)
            path = options.get(prefix + "-thin-config");

        return path == null ? null : context(path);
    }

    /** Creates a thin-client connection configuration from addresses or a Spring XML bean. */
    static ClientConfiguration thinConfiguration(
        Map<String, String> options,
        String prefix,
        GenericApplicationContext ctx
    ) {
        String addrsStr = options.get(prefix + "-thin");

        if (addrsStr != null) {
            String[] addrs = Arrays.stream(addrsStr.split(",", -1)).map(String::trim).toArray(String[]::new);

            if (Arrays.stream(addrs).anyMatch(String::isEmpty))
                throw new IllegalArgumentException("Thin-client addresses must be non-empty and comma-separated.");

            return new ClientConfiguration().setAddresses(addrs);
        }

        return bean(ctx, ClientConfiguration.class, prefix + " thin-client configuration");
    }

    /** */
    static GenericApplicationContext context(String path) throws Exception {
        GenericApplicationContext ctx = new GenericApplicationContext();

        try {
            new XmlBeanDefinitionReader(ctx).loadBeanDefinitions(new UrlResource(U.resolveSpringUrl(path)));
            ctx.refresh();

            return ctx;
        }
        catch (Exception e) {
            ctx.close();

            throw e;
        }
    }

    /** */
    static IgniteConfiguration nodeConfiguration(GenericApplicationContext ctx, String role) {
        Map<String, IgniteConfiguration> beans = ctx.getBeansOfType(IgniteConfiguration.class);

        if (beans.size() != 1)
            throw new IllegalArgumentException("Expected exactly one IgniteConfiguration bean for " + role + '.');

        return new IgniteConfiguration(beans.values().iterator().next()).setClientMode(true)
            .setIgniteInstanceName("migration-" + role + '-' + UUID.randomUUID());
    }

    /** Supports both dump-reader Functions and destination-aware BiFunctions. */
    @SuppressWarnings("unchecked")
    private static void configureTransformer(
        Map<String, String> options,
        IgniteToIgniteMigrationConfiguration cfg
    ) throws Exception {
        if (!options.containsKey("cache-config-transformer"))
            return;

        Object transformer = Class.forName(options.get("cache-config-transformer")).getDeclaredConstructor().newInstance();
        Map<String, String> params = new HashMap<>();

        if (options.containsKey("cache-config-transformer-parameters")) {
            for (String param : options.get("cache-config-transformer-parameters").split(",", -1)) {
                String[] pair = param.split("=", 2);

                if (pair.length != 2 || pair[0].isEmpty() || params.putIfAbsent(pair[0], pair[1]) != null)
                    throw new IllegalArgumentException("Invalid or duplicate transformer parameter: " + param + '.');
            }
        }

        if (!params.isEmpty()) {
            try {
                transformer.getClass().getMethod("configure", Map.class)
                    .invoke(transformer, Collections.unmodifiableMap(params));
            }
            catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Transformer parameters require a public configure(Map) method: " +
                    transformer.getClass().getName() + '.');
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Failed to configure cache transformer: " +
                    transformer.getClass().getName() + '.', e);
            }
        }

        if (transformer instanceof Function) {
            cfg.cacheMigration().setCacheConfigurationTransformer(
                (Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>>)transformer);
        }
        else
            throw new IllegalArgumentException("Cache transformer must implement Function or BiFunction.");

        if (cfg.logger().isInfoEnabled())
            cfg.logger().info("Configured cache configuration transformer [class=" + transformer.getClass().getName() +
                ", parameters=" + params + "].");
    }

    /** */
    private static <T> T bean(GenericApplicationContext ctx, Class<T> type, String role) {
        Map<String, T> beans = ctx.getBeansOfType(type);

        if (beans.size() != 1)
            throw new IllegalArgumentException("Expected exactly one " + type.getSimpleName() +
                " bean for " + role + ", found " + beans.size() + '.');

        return beans.values().iterator().next();
    }

    /**
     * Overlays CLI-specified values onto an XML-loaded configuration. Only flags explicitly present
     * in the command line override XML values; defaults in {@link #configuration(Map)} do not.
     */
    static void merge(IgniteToIgniteMigrationConfiguration target,
        IgniteToIgniteMigrationConfiguration xml, Map<String, String> options) {
        if (!options.containsKey("no-binary-metadata-create") && !options.containsKey("no-binary-metdata-create"))
            target.setNoBinaryMetadataCreate(xml.isNoBinaryMetadataCreate());

        if (!options.containsKey("no-mappings-create"))
            target.setNoMappingsCreate(xml.isNoMappingsCreate());

        CacheMigrationConfiguration srcSel = xml.getCacheMigration();

        if (!options.containsKey("create-cache-if-not-exists"))
            target.cacheMigration().setCreateCacheIfNotExists(srcSel.createCacheIfNotExists());

        if (!options.containsKey("move-data-structures"))
            target.cacheMigration().setMoveDataStructures(srcSel.moveDataStructures());

        if (!options.containsKey("cache-group-names"))
            target.cacheMigration().setCacheGroupNames(new ArrayList<>(srcSel.cacheGroupNames()));

        if (!options.containsKey("cache-config-transformer"))
            target.cacheMigration().setCacheConfigurationTransformer(srcSel.cacheConfigurationTransformer());

        if (!options.containsKey("ignore-default-filter"))
            target.metastorage().setIgnoreDefaultFilter(xml.getMetastorage().isIgnoreDefaultFilter());

        target.metastorage().setFilters(new ArrayList<>(xml.getMetastorage().filters()));

        CacheDataMigrationConfiguration srcData = xml.getCacheData();

        if (!options.containsKey("skip-expire-time"))
            target.cacheData().setSkipExpireTime(srcData.skipExpireTime());

        if (!options.containsKey("skip-conflict-version") && !options.containsKey("skip-cdc-info"))
            target.cacheData().setSkipConflictVersion(srcData.skipConflictVersion());

        if (!options.containsKey("batch-size"))
            target.cacheData().setBatchSize(srcData.batchSize());

        if (!options.containsKey("read-threads"))
            target.cacheData().setReadThreads(srcData.readThreads());

        if (!options.containsKey("write-threads"))
            target.cacheData().setWriteThreads(srcData.writeThreads());

        if (!options.containsKey("read-limit-in-sec"))
            target.cacheData().setReadLimitInSec(srcData.readLimitInSec());

        if (!options.containsKey("write-limit-in-sec"))
            target.cacheData().setWriteLimitInSec(srcData.writeLimitInSec());

        if (!options.containsKey("queue-capacity"))
            target.cacheData().setQueueCapacity(srcData.queueCapacity());

        if (!options.containsKey("use-data-streamer")) {
            target.cacheData().setUseDataStreamer(srcData.useDataStreamer());
            target.cacheData().setDataStreamerConfiguration(srcData.dataStreamerConfiguration());
        }
    }

    /** */
    private static void usage() {
        X.println("Usage: ignite-to-ignite.sh <one source connection option> <one destination connection option>",
            "--src-client-node-config source.xml | --src-thin host:port,... | --src-thin-config thin.xml",
            "--dest-client-node-config destination.xml | --dest-thin host:port,... | --dest-thin-config thin.xml",
            "Connection XML contains one IgniteConfiguration (thick) or ClientConfiguration (thin) bean.",
            "Options: --config migration.xml (Spring XML with IgniteToIgniteMigrationConfiguration bean)",
            "         --cache-group-names group1,group2 --batch-size N --read-threads N --write-threads N",
            "         --read-limit-in-sec N --write-limit-in-sec N --queue-capacity N",
            "         --create-cache-if-not-exists",
            "         --no-binary-metadata-create --no-mappings-create --skip-expire-time",
            "         --skip-conflict-version (alias: --skip-cdc-info)",
            "         --ignore-default-filter",
            "         --move-data-structures true|false",
            "         --use-data-streamer [--allow-overwrite] [--per-node-buffer-size N]",
            "         --per-node-parallel-operations N --per-thread-buffer-size N --timeout N --auto-flush-frequency N",
            "         --cache-config-transformer FQN [--cache-config-transformer-parameters key=value,...]",
            "         --destination FQN (custom MigrationDestination instead of a built-in destination)",
            "         --help");
    }
}
