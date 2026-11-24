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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationMigrationEntity;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationsTransferrer;
import org.apache.ignite.util.migration.category.cache.configuration.ThinClientCacheConfigurationsTransferrer;
import org.apache.ignite.util.migration.category.cache.data.CacheDataTransferrer;
import org.apache.ignite.util.migration.category.cache.data.ThinClientCacheDataTransferrer;
import org.apache.ignite.util.migration.category.metastorage.MetastorageTransferrer;
import org.apache.ignite.util.migration.category.type.BinaryMetadataTransferrer;
import org.apache.ignite.util.migration.category.type.ThinClientBinaryMetadataTransferrer;
import org.apache.ignite.util.migration.category.type.ThinClientTypeMappingsTransferrer;
import org.apache.ignite.util.migration.category.type.TypeMappingsTransferrer;

/** Connects migration endpoints and runs their category transferrers in dependency order. */
public class IgniteToIgniteMigration {
    /**
     * Runs a migration, closing both endpoints on success or failure.
     *
     * @param from Source endpoint.
     * @param to Destination endpoint.
     * @param mCfg Migration configuration.
     */
    public static void start(MigrationOrigin<?> from, MigrationDestination<?> to, IgniteToIgniteMigrationConfiguration mCfg) {
        long start = System.nanoTime();

        if (mCfg.logger().isInfoEnabled())
            mCfg.logger().info("Starting data migration [time=" + System.currentTimeMillis() + "].");

        mCfg.validate();

        try (from; to) {
            Object src = from.connect();
            Object dest = to.connect();

            if (!(src instanceof IgniteEx || src instanceof IgniteClient))
                throw new IgniteException("Migration source must expose an IgniteEx or IgniteClient connection.");

            if (mCfg.cacheData().useDataStreamer() && !(dest instanceof IgniteEx))
                throw new IgniteException("DataStreamer requires a thick destination connection.");

            if (sameCluster(src, dest))
                throw new IgniteException("Source and destination must be different clusters.");

            if (src instanceof IgniteEx src0)
                runThickPipeline(to, src0, dest, mCfg);
            else
                runThinClientPipeline(to, (IgniteClient)src, mCfg);

            if (mCfg.logger().isInfoEnabled())
                mCfg.logger().info("Data migration completed [elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "].");
        }
    }

    /** Runs the categories that can operate with a thick source connection. */
    private static void runThickPipeline(
        MigrationDestination to,
        IgniteEx src,
        Object destOrigin,
        IgniteToIgniteMigrationConfiguration mCfg
    ) {
        IgniteLogger log = mCfg.logger();

        MigrationTransformer<CacheConfigurationMigrationEntity> cacheTransformer =
            mCfg.transformer(CacheConfigurationsTransferrer.class);
        CacheConfigurationsTransferrer cacheTransferrer = new CacheConfigurationsTransferrer(src, mCfg.cacheMigration(), log);
        Map<String, String> cacheNameMap =
            cacheTransferrer.cacheNameMap(destOrigin instanceof IgniteEx, cacheTransformer);

        if (!mCfg.noBinaryMetadataCreate())
            new BinaryMetadataTransferrer(src, log).transfer(to, mCfg.transformer(BinaryMetadataTransferrer.class));

        if (!mCfg.noMappingsCreate())
            new TypeMappingsTransferrer(src, log).transfer(to, mCfg.transformer(TypeMappingsTransferrer.class));

        cacheTransferrer.transfer(to, cacheTransformer);

        if (destOrigin instanceof IgniteEx dest) {
            boolean hasSqlSchemas = dest.context().cache().cacheDescriptors().values().stream()
                .anyMatch(org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor::sql);

            new MetastorageTransferrer(src, hasSqlSchemas, log, mCfg.metastorage())
                .transfer(to, mCfg.transformer(MetastorageTransferrer.class));
        }
        else
            log.warning("Distributed metastorage is not transferred [reason='destination is not a thick connection'].");

        new CacheDataTransferrer(src, log, mCfg.cacheData(), cacheNameMap)
            .transfer(to, mCfg.transformer(CacheDataTransferrer.class));
    }

    /** Runs the categories that can operate with a thin source connection. */
    private static void runThinClientPipeline(
        MigrationDestination to,
        IgniteClient src,
        IgniteToIgniteMigrationConfiguration mCfg
    ) {
        IgniteLogger log = mCfg.logger();

        log.warning("Thin-client mode transfers binary metadata known to the thin source and Java mappings whose " +
            "classes are available to the migration process. Unavailable mappings, distributed metastorage, SQL " +
            "views, dynamic cluster settings, SQL-created flags and data-structure state are not transferred. " +
            "Thin cache configuration is limited to ClientCacheConfiguration properties.");

        MigrationTransformer<CacheConfigurationMigrationEntity> cacheTransformer =
            mCfg.transformer(CacheConfigurationsTransferrer.class);
        ThinClientCacheConfigurationsTransferrer cacheTransferrer =
            new ThinClientCacheConfigurationsTransferrer(src, mCfg.cacheMigration(), log);
        Map<String, String> cacheNameMap = cacheTransferrer.cacheNameMap(false, cacheTransformer);

        Set<Integer> transferredTypeIds = Collections.emptySet();

        if (!mCfg.noBinaryMetadataCreate()) {
            ThinClientBinaryMetadataTransferrer binaryTransferrer = new ThinClientBinaryMetadataTransferrer(src, log);

            binaryTransferrer.transfer(to, mCfg.transformer(ThinClientBinaryMetadataTransferrer.class));

            transferredTypeIds = binaryTransferrer.transferredTypeIds();
        }

        if (!mCfg.noMappingsCreate())
            new ThinClientTypeMappingsTransferrer(src, log).transfer(to,
                mCfg.transformer(ThinClientTypeMappingsTransferrer.class));

        cacheTransferrer.transfer(to, cacheTransformer);

        new ThinClientCacheDataTransferrer(src, log, mCfg.cacheData(), cacheNameMap,
            mCfg.noBinaryMetadataCreate(), mCfg.noMappingsCreate(), transferredTypeIds)
            .transfer(to, mCfg.transformer(CacheDataTransferrer.class));
    }

    /**
     * Detects matching clusters using cluster identity for thick clients and server node IDs for thin clients.
     * Returns {@code false} when either connection type is unknown, because the cluster cannot be verified.
     */
    private static boolean sameCluster(Object src, Object dest) {
        if (src instanceof IgniteEx && dest instanceof IgniteEx)
            return ((IgniteEx)src).cluster().id().equals(((IgniteEx)dest).cluster().id());

        if (!(src instanceof IgniteEx || src instanceof IgniteClient) ||
            !(dest instanceof IgniteEx || dest instanceof IgniteClient))
            return false;

        Set<java.util.UUID> srcNodes = serverNodeIds(src);
        Set<java.util.UUID> destNodes = serverNodeIds(dest);

        srcNodes.retainAll(destNodes);

        return !srcNodes.isEmpty();
    }

    /** Gets server node IDs exposed by either endpoint type. */
    private static Set<java.util.UUID> serverNodeIds(Object origin) {
        Set<java.util.UUID> res = new HashSet<>();

        if (origin instanceof IgniteEx ig) {
            for (ClusterNode node : ig.cluster().forServers().nodes())
                res.add(node.id());
        }
        else {
            for (ClusterNode node : ((IgniteClient)origin).cluster().forServers().nodes())
                res.add(node.id());
        }

        return res;
    }
}
