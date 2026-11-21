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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationsTransferrer;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCache;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCachePlan;
import org.apache.ignite.util.migration.category.cache.data.CacheDataTransferrer;
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
    public static void start(
        MigrationOrigin from,
        MigrationDestination to,
        IgniteToIgniteMigrationConfiguration mCfg
    ) {
        mCfg.validate();

        IgniteLogger log = mCfg.logger();
        long start = System.nanoTime();

        if (log.isInfoEnabled()) {
            log.info("Starting data migration [time=" + System.currentTimeMillis() + ", sourceEndpoint=" +
                from.getClass().getName() + ", destinationEndpoint=" + to.getClass().getName() +
                ", sourceCategories=" + from.supportedCategories() +
                ", destinationCategories=" + to.supportedCategories() +
                ", cacheGroups=" + mCfg.cacheSelection().getCacheGroupNames() +
                ", moveDataStructures=" + mCfg.cacheSelection().moveDataStructures() +
                ", cacheConfigTransformer=" +
                mCfg.cacheSelection().cacheConfigurationTransformer().getClass().getName() +
                ", transferrerTransformers=" + mCfg.transformerProviders() +
                ", noCacheCreate=" + mCfg.cacheConfigurations().noCacheCreate() +
                ", createCacheIfNotExists=" + mCfg.cacheConfigurations().createCacheIfNotExists() +
                ", noBinaryMetadataCreate=" + mCfg.noBinaryMetadataCreate() +
                ", noMappingsCreate=" + mCfg.noMappingsCreate() +
                ", batchSize=" + mCfg.cacheData().batchSize() + ", readThreads=" +
                mCfg.cacheData().readThreads() + ", writeThreads=" + mCfg.cacheData().writeThreads() +
                ", queueCapacity=" + mCfg.cacheData().queueCapacity() +
                ", readLimit=" + mCfg.cacheData().readLimitInSec() +
                ", writeLimit=" + mCfg.cacheData().writeLimitInSec() +
                ", skipExpireTime=" + mCfg.cacheData().skipExpireTime() +
                ", skipConflictVersion=" + mCfg.cacheData().skipConflictVersion() +
                ", useDataStreamer=" + mCfg.cacheData().useDataStreamer() +
                ", dataStreamerSettings=" + (mCfg.cacheData().useDataStreamer() ?
                    mCfg.cacheData().dataStreamerConfigurationDescription() : "disabled") +
                ", ignoreDefaultMetastorageFilter=" + mCfg.metastorage().ignoreDefaultFilter() +
                ", metastorageFilters=" + mCfg.metastorage().filters().stream()
                    .map(filter -> filter.getClass().getName()).collect(Collectors.joining(",", "[", "]")) + "].");
        }

        try (from; to) {
            Object srcOrigin = from.connect();
            Object destOrigin = to.connect();

            if (log.isInfoEnabled())
                log.info("Connected migration endpoints [source=" + srcOrigin.getClass().getSimpleName() +
                    ", destination=" + destOrigin.getClass().getSimpleName() + "].");

            runPipeline(from, to, srcOrigin, destOrigin, mCfg);

            if (log.isInfoEnabled())
                log.info("Data migration completed [elapsedMs=" +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "].");
        }
        catch (RuntimeException | Error e) {
            log.error("Data migration failed", e);

            throw e;
        }
    }

    /** Builds and runs the configured category transferrers in dependency order. */
    private static void runPipeline(
        MigrationOrigin from,
        MigrationDestination to,
        Object srcOrigin,
        Object destOrigin,
        IgniteToIgniteMigrationConfiguration mCfg
    ) {
        if (!(srcOrigin instanceof IgniteEx || srcOrigin instanceof IgniteClient) ||
            !(destOrigin instanceof IgniteEx || destOrigin instanceof IgniteClient))
            throw new IgniteException("Migration endpoints must expose an IgniteEx or IgniteClient connection.");

        if (sameCluster(srcOrigin, destOrigin))
            throw new IgniteException("Source and destination must be different clusters.");

        IgniteLogger log = mCfg.logger();
        LinkedHashMap<MigrationCategory, Class<?>> compatibleCategories = checkSupportedCategories(from, to, log);

        if (!(srcOrigin instanceof IgniteEx) || !(destOrigin instanceof IgniteEx)) {
            runThinClientPipeline(to, srcOrigin, destOrigin, compatibleCategories, mCfg);

            return;
        }

        IgniteEx src = (IgniteEx)srcOrigin;
        IgniteEx dest = (IgniteEx)destOrigin;

        // Cache selection and name mapping are needed even when cache creation is disabled.
        MigrationTransformer<MigrationCache> cacheTransformer =
            mCfg.transformer(CacheConfigurationsTransferrer.class);
        MigrationCachePlan cachePlan = new MigrationCachePlan(src, mCfg.cacheSelection(), cacheTransformer, log);

        for (MigrationCategory category : compatibleCategories.keySet()) {
            switch (category) {
                case BINARY_METADATA:
                    if (!mCfg.noBinaryMetadataCreate())
                        run(new BinaryMetadataTransferrer(src, log), to, compatibleCategories,
                            mCfg.transformer(BinaryMetadataTransferrer.class));

                    break;

                case TYPE_MAPPING:
                    if (!mCfg.noMappingsCreate())
                        run(new TypeMappingsTransferrer(src, log), to, compatibleCategories,
                            mCfg.transformer(TypeMappingsTransferrer.class));

                    break;

                case CACHE_CONFIGURATION:
                    run(new CacheConfigurationsTransferrer(srcOrigin, destOrigin, log, mCfg.cacheConfigurations(),
                        cachePlan), to, compatibleCategories, cacheTransformer);

                    break;

                case METASTORAGE_ENTRY:
                    run(new MetastorageTransferrer(src, dest, log, mCfg.metastorage()), to, compatibleCategories,
                        mCfg.transformer(MetastorageTransferrer.class));

                    break;

                case CACHE_ENTRY:
                    run(new CacheDataTransferrer(srcOrigin, destOrigin, log, mCfg.cacheData(), cachePlan,
                        mCfg.noBinaryMetadataCreate(), mCfg.noMappingsCreate()), to, compatibleCategories,
                        mCfg.transformer(CacheDataTransferrer.class));

                    break;

                default:
                    throw new IgniteException("Unsupported migration category: " + category + '.');
            }
        }
    }

    /** Runs the categories that can operate with the public thin-client cache API. */
    private static void runThinClientPipeline(
        MigrationDestination to,
        Object srcOrigin,
        Object destOrigin,
        LinkedHashMap<MigrationCategory, Class<?>> compatibleCategories,
        IgniteToIgniteMigrationConfiguration mCfg
    ) {
        if (destOrigin instanceof IgniteClient && mCfg.cacheData().useDataStreamer())
            throw new IgniteException("DataStreamer is not available through the thin-client API.");

        IgniteLogger log = mCfg.logger();

        log.warning("Thin-client mode transfers binary metadata known to the thin source and Java mappings whose " +
            "classes are available to the migration process. Unavailable mappings, distributed metastorage, SQL " +
            "views, dynamic cluster settings, SQL-created flags and data-structure state are not transferred. " +
            "Thin cache configuration is limited to ClientCacheConfiguration properties.");

        MigrationTransformer<MigrationCache> cacheTransformer =
            mCfg.transformer(CacheConfigurationsTransferrer.class);
        MigrationCachePlan cachePlan;

        if (srcOrigin instanceof IgniteEx)
            cachePlan = new MigrationCachePlan((IgniteEx)srcOrigin, mCfg.cacheSelection(), cacheTransformer, false, log);
        else
            cachePlan = new MigrationCachePlan((IgniteClient)srcOrigin, mCfg.cacheSelection(), cacheTransformer, log);

        for (MigrationCategory category : compatibleCategories.keySet()) {
            switch (category) {
                case BINARY_METADATA:
                    if (!mCfg.noBinaryMetadataCreate()) {
                        if (srcOrigin instanceof IgniteClient) {
                            run(new ThinClientBinaryMetadataTransferrer((IgniteClient)srcOrigin, log), to,
                                compatibleCategories, mCfg.transformer(ThinClientBinaryMetadataTransferrer.class));
                        }
                        else {
                            run(new BinaryMetadataTransferrer((IgniteEx)srcOrigin, log), to, compatibleCategories,
                                mCfg.transformer(BinaryMetadataTransferrer.class));
                        }
                    }

                    break;

                case TYPE_MAPPING:
                    if (!mCfg.noMappingsCreate()) {
                        if (srcOrigin instanceof IgniteClient) {
                            run(new ThinClientTypeMappingsTransferrer((IgniteClient)srcOrigin, log), to,
                                compatibleCategories, mCfg.transformer(ThinClientTypeMappingsTransferrer.class));
                        }
                        else {
                            run(new TypeMappingsTransferrer((IgniteEx)srcOrigin, log), to, compatibleCategories,
                                mCfg.transformer(TypeMappingsTransferrer.class));
                        }
                    }

                    break;

                case CACHE_CONFIGURATION:
                    run(new CacheConfigurationsTransferrer(srcOrigin, destOrigin, log, mCfg.cacheConfigurations(),
                        cachePlan), to, compatibleCategories, cacheTransformer);

                    break;

                case CACHE_ENTRY:
                    run(new CacheDataTransferrer(srcOrigin, destOrigin, log, mCfg.cacheData(), cachePlan,
                        mCfg.noBinaryMetadataCreate(), mCfg.noMappingsCreate()), to, compatibleCategories,
                        mCfg.transformer(CacheDataTransferrer.class));

                    break;

                default:
                    throw new IgniteException("Unsupported migration category in thin-client mode: " +
                        category + '.');
            }
        }
    }

    /** Verifies that the source payloads are accepted by the destination and preserves destination priority. */
    static LinkedHashMap<MigrationCategory, Class<?>> checkSupportedCategories(
        MigrationOrigin from,
        MigrationDestination to,
        IgniteLogger log
    ) {
        LinkedHashMap<MigrationCategory, Class<?>> srcCategories = from.supportedCategories();
        LinkedHashMap<MigrationCategory, Class<?>> dstCategories = to.supportedCategories();
        LinkedHashMap<MigrationCategory, Class<?>> compatibleCategories = new LinkedHashMap<>();

        validateCategoryMap(srcCategories, "source");
        validateCategoryMap(dstCategories, "destination");

        for (Map.Entry<MigrationCategory, Class<?>> dstCategory : dstCategories.entrySet()) {
            MigrationCategory category = dstCategory.getKey();
            Class<?> srcCls = srcCategories.get(category);

            if (srcCls == null) {
                if (log.isInfoEnabled())
                    log.info("Destination migration category has no source data [category=" + category +
                        ", categoryId=" + category.id() + ", destinationDataClass=" +
                        dstCategory.getValue().getName() + "].");

                continue;
            }

            Class<?> dstCls = dstCategory.getValue();

            if (!dstCls.isAssignableFrom(srcCls)) {
                throw new IgniteException("Incompatible migration category data classes [category=" + category +
                    ", categoryId=" + category.id() + ", sourceDataClass=" + srcCls.getName() +
                    ", destinationDataClass=" + dstCls.getName() + "].");
            }

            compatibleCategories.put(category, srcCls);

            if (log.isInfoEnabled())
                log.info("Compatible migration category [category=" + category + ", categoryId=" + category.id() +
                    ", dataClass=" + srcCls.getName() + ", destinationAccepts=" + dstCls.getName() +
                    ", priority=" + compatibleCategories.size() + "].");
        }

        for (Map.Entry<MigrationCategory, Class<?>> srcCategory : srcCategories.entrySet()) {
            if (!dstCategories.containsKey(srcCategory.getKey()))
                log.warning("Source migration category is not supported by destination; skipping it [category=" +
                    srcCategory.getKey() + ", categoryId=" + srcCategory.getKey().id() + ", sourceDataClass=" +
                    srcCategory.getValue().getName() + "].");
        }

        if (compatibleCategories.isEmpty())
            throw new IgniteException("Migration source and destination have no compatible categories " +
                "[source=" + srcCategories + ", destination=" + dstCategories + "].");

        return compatibleCategories;
    }

    /** Validates that a capability map has explicit categories and payload classes. */
    private static void validateCategoryMap(LinkedHashMap<MigrationCategory, Class<?>> categories, String endpoint) {
        if (categories == null)
            throw new IgniteException("Migration " + endpoint + " returned a null category map.");

        for (Map.Entry<MigrationCategory, Class<?>> entry : categories.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IgniteException("Migration " + endpoint +
                    " category map must contain non-null categories and data classes.");
            }
        }
    }

    /** Detects matching clusters using cluster identity for thick clients and server node IDs for thin clients. */
    private static boolean sameCluster(Object src, Object dest) {
        if (src instanceof IgniteEx && dest instanceof IgniteEx)
            return ((IgniteEx)src).cluster().id().equals(((IgniteEx)dest).cluster().id());

        Set<java.util.UUID> srcNodes = serverNodeIds(src);
        Set<java.util.UUID> destNodes = serverNodeIds(dest);

        srcNodes.retainAll(destNodes);

        return !srcNodes.isEmpty();
    }

    /** Gets server node IDs exposed by either endpoint type. */
    private static Set<java.util.UUID> serverNodeIds(Object origin) {
        Set<java.util.UUID> res = new HashSet<>();

        if (origin instanceof IgniteEx) {
            for (ClusterNode node : ((IgniteEx)origin).cluster().forServers().nodes())
                res.add(node.id());
        }
        else {
            for (ClusterNode node : ((IgniteClient)origin).cluster().forServers().nodes())
                res.add(node.id());
        }

        return res;
    }

    /** Runs a direct destination-aware transferrer after validating its category payload type. */
    private static <T> void run(
        CategoryTransferrer<T> transferrer,
        MigrationDestination dest,
        LinkedHashMap<MigrationCategory, Class<?>> compatibleCategories,
        MigrationTransformer<T> transformer
    ) {
        Class<?> srcDataCls = compatibleCategories.get(transferrer.category());
        Class<?> dstDataCls = dest.supportedCategories().get(transferrer.category());

        if (srcDataCls == null || !srcDataCls.equals(transferrer.dataClass()) ||
            dstDataCls == null || !dstDataCls.isAssignableFrom(transferrer.dataClass())) {
            throw new IgniteException("Migration category transferrer payload type does not match the endpoint's " +
                "[category=" + transferrer.category() + ", srcType=" + srcDataCls + ", destType=" + dstDataCls + "].");
        }

        transferrer.transfer(dest, transformer);
    }
}
