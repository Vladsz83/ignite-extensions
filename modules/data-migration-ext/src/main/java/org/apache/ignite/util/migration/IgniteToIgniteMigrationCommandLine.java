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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.migration.api.CacheConfigTransformer;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.impl.ThickClientMigrationOrigin;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;

import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.startup.cmdline.CommandLineStartup.isHelp;

/** Command-line entry point. Uses Ignite launchers, Spring connection configuration and Ignite logging. */
public class IgniteToIgniteMigrationCommandLine {
    /** Boolean switches. */
    private static final Set<String> FLAGS = new HashSet<>(Arrays.asList(
        "no-cache-create", "create-cache-if-not-exists", "no-binary-metadata-create", "no-binary-metdata-create",
        "no-mappings-create", "skip-expire-time", "skip-cdc-info"));

    /** Options requiring a value. */
    private static final Set<String> VALUES = new HashSet<>(Arrays.asList(
        "src-client-node-config", "dest-client-node-config", "cache-group-names", "batch-size", "read-threads",
        "write-threads", "read-limit-in-sec", "write-limit-in-sec", "queue-capacity", "cache-config-transformer",
        "cache-config-transformer-parameters", "destination"));

    /**
     * Main entry point.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        System.exit(run(args));
    }

    /** @return Process exit code; exposed separately from {@code main} for embedded invocation. */
    public static int run(String... args) {
        if (args.length == 1 && (isHelp(args[0]) || "--help".equals(args[0]))) {
            usage();
            return 0;
        }

        try {
            Map<String, String> options = parse(args);
            IgniteToIgniteMigrationConfiguration cfg = configuration(options);

            X.println("Apache Ignite Data Migration, ver. " + VER);
            X.println(COPYRIGHT);

            try (GenericApplicationContext srcCtx = context(options.get("src-client-node-config"));
                 GenericApplicationContext destCtx = options.containsKey("destination") ? null :
                     context(options.get("dest-client-node-config"))) {
                IgniteConfiguration srcCfg = nodeConfiguration(srcCtx, "source");
                IgniteConfiguration destCfg = destCtx == null ? null : nodeConfiguration(destCtx, "destination");

                configureTransformer(options, cfg, destCfg);

                MigrationDestination dest = destCfg == null ?
                    (MigrationDestination)Class.forName(options.get("destination")).getDeclaredConstructor().newInstance() :
                    ThickClientMigrationOrigin.create(destCfg);

                IgniteToIgniteMigration.start(ThickClientMigrationOrigin.create(srcCfg), dest, cfg);
            }

            return 0;
        }
        catch (Exception e) {
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
                throw new IllegalArgumentException("Expected an option, got: " + arg);

            String name = arg.substring(2);

            if (!FLAGS.contains(name) && !VALUES.contains(name))
                throw new IllegalArgumentException("Unknown or unsupported option: " + arg);

            String val = "true";

            if (VALUES.contains(name)) {
                if (++i >= args.length || args[i].isEmpty() || args[i].startsWith("--"))
                    throw new IllegalArgumentException("Missing value for " + arg);

                val = args[i];
            }

            if (res.putIfAbsent(name, val) != null)
                throw new IllegalArgumentException("Duplicate option: " + arg);
        }

        if (!res.containsKey("src-client-node-config"))
            throw new IllegalArgumentException("--src-client-node-config is required");

        if (res.containsKey("dest-client-node-config") == res.containsKey("destination"))
            throw new IllegalArgumentException("Specify exactly one of --dest-client-node-config and --destination");

        if (res.containsKey("cache-config-transformer-parameters") && !res.containsKey("cache-config-transformer"))
            throw new IllegalArgumentException("Transformer parameters require --cache-config-transformer");

        configuration(res).validate();

        return res;
    }

    /** */
    static IgniteToIgniteMigrationConfiguration configuration(Map<String, String> options) {
        IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration()
            .setNoCacheCreate(options.containsKey("no-cache-create"))
            .setCreateCacheIfNotExists(options.containsKey("create-cache-if-not-exists"))
            .setNoBinaryMetadataCreate(options.containsKey("no-binary-metadata-create") || options.containsKey("no-binary-metdata-create"))
            .setNoMappingsCreate(options.containsKey("no-mappings-create"))
            .setSkipExpireTime(options.containsKey("skip-expire-time"))
            .setSkipCdcInfo(options.containsKey("skip-cdc-info"))
            .setBatchSize(integer(options, "batch-size", 1))
            .setReadThreads(integer(options, "read-threads", 1))
            .setWriteThreads(integer(options, "write-threads", 1))
            .setReadLimitInSec(integer(options, "read-limit-in-sec", 0))
            .setWriteLimitInSec(integer(options, "write-limit-in-sec", 0))
            .setQueueCapacity(integer(options, "queue-capacity", 64));

        if (options.containsKey("cache-group-names"))
            cfg.setCacheGroupNames(options.get("cache-group-names").split(",", -1));

        return cfg;
    }

    /** */
    private static int integer(Map<String, String> options, String name, int dflt) {
        try {
            return options.containsKey(name) ? Integer.parseInt(options.get(name)) : dflt;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected an integer for --" + name, e);
        }
    }

    /** */
    private static GenericApplicationContext context(String path) throws Exception {
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
    private static IgniteConfiguration nodeConfiguration(GenericApplicationContext ctx, String role) {
        Map<String, IgniteConfiguration> beans = ctx.getBeansOfType(IgniteConfiguration.class);

        if (beans.size() != 1)
            throw new IllegalArgumentException("Expected exactly one IgniteConfiguration bean for " + role);

        return new IgniteConfiguration(beans.values().iterator().next()).setClientMode(true)
            .setIgniteInstanceName("migration-" + role + '-' + UUID.randomUUID());
    }

    /** Supports both dump-reader Functions and destination-aware BiFunctions. */
    @SuppressWarnings("unchecked")
    private static void configureTransformer(Map<String, String> options, IgniteToIgniteMigrationConfiguration cfg,
        IgniteConfiguration destCfg) throws Exception {
        if (!options.containsKey("cache-config-transformer"))
            return;

        Object transformer = Class.forName(options.get("cache-config-transformer")).getDeclaredConstructor().newInstance();
        Map<String, String> params = new HashMap<>();

        if (options.containsKey("cache-config-transformer-parameters")) {
            for (String param : options.get("cache-config-transformer-parameters").split(",", -1)) {
                String[] pair = param.split("=", 2);

                if (pair.length != 2 || pair[0].isEmpty() || params.putIfAbsent(pair[0], pair[1]) != null)
                    throw new IllegalArgumentException("Invalid or duplicate transformer parameter: " + param);
            }
        }

        if (transformer instanceof CacheConfigTransformer)
            ((CacheConfigTransformer)transformer).configure(Collections.unmodifiableMap(params));
        else if (!params.isEmpty())
            throw new IllegalArgumentException("Transformer parameters require CacheConfigTransformer");

        if (transformer instanceof BiFunction) {
            BiFunction<IgniteConfiguration, CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> fn =
                (BiFunction<IgniteConfiguration, CacheConfiguration<?, ?>, CacheConfiguration<?, ?>>)transformer;

            cfg.setCacheConfigurationTransformer(cache -> fn.apply(destCfg, cache));
        }
        else if (transformer instanceof Function)
            cfg.setCacheConfigurationTransformer((Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>>)transformer);
        else
            throw new IllegalArgumentException("Cache transformer must implement Function or BiFunction");
    }

    /** */
    private static void usage() {
        X.println("Usage: data-migration.sh --src-client-node-config source.xml --dest-client-node-config destination.xml",
            "Connection XML files must each contain one IgniteConfiguration bean.",
            "Options: --cache-group-names group1,group2 --batch-size N --read-threads N --write-threads N",
            "         --read-limit-in-sec N --write-limit-in-sec N --queue-capacity N",
            "         --no-cache-create | --create-cache-if-not-exists",
            "         --no-binary-metadata-create --no-mappings-create --skip-expire-time --skip-cdc-info",
            "         --cache-config-transformer FQN [--cache-config-transformer-parameters key=value,...]",
            "         --destination FQN (custom MigrationDestination instead of --dest-client-node-config)",
            "         --help");
    }
}
