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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationsMigrationChannel;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCache;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCachePlan;
import org.apache.ignite.util.migration.category.cache.data.CacheDataMigrationChannel;
import org.apache.ignite.util.migration.category.metastorage.MetastorageMigrationChannel;
import org.apache.ignite.util.migration.category.type.BinaryMetadataMigrationChannel;
import org.apache.ignite.util.migration.category.type.TypeMappingsMigrationChannel;

/** Connects migration endpoints and runs their transfer channels in dependency order. */
public class IgniteToIgniteMigration {

    /**
     * Runs a migration, closing both endpoints on success or failure.
     *
     * @param from Source endpoint.
     * @param to Destination endpoint.
     * @param mCfg Migration configuration.
     */
    public static void start(
        MigrationOrigin<?> from,
        MigrationOrigin<?> to,
        IgniteToIgniteMigrationConfiguration mCfg
    ) throws IgniteCheckedException {
        mCfg.validate();

        IgniteLogger log = mCfg.logger();
        long start = System.nanoTime();

        if (log.isInfoEnabled()) {
            log.info("Starting data migration [time=" + System.currentTimeMillis() + ", batchSize=" +
                mCfg.cacheData().batchSize() + ", readThreads=" + mCfg.cacheData().readThreads() +
                ", writeThreads=" + mCfg.cacheData().writeThreads() + ", readLimit=" + mCfg.cacheData().readLimitInSec() +
                ", writeLimit=" + mCfg.cacheData().writeLimitInSec() + "].");
        }

        try (from; to) {
            from.connect();
            to.connect();

            // One shared pool serves every channel; sized to also cover the cache-data reader/writer workers.
            ExecutorService pool = Executors.newFixedThreadPool(
                Math.max(mCfg.migrationThreads(),
                    mCfg.cacheData().readThreads() + mCfg.cacheData().writeThreads()),
                new IgniteThreadFactory(null, "migration"));

            try {
                runPipeline(from, to, mCfg, pool);
            }
            finally {
                pool.shutdownNow();
            }

            if (log.isInfoEnabled())
                log.info("Data migration completed [elapsedMs=" +
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "].");
        }
        catch (IgniteCheckedException | RuntimeException e) {
            log.error("Data migration failed", e);

            throw e;
        }
    }

    /** Builds and runs the configured channels in dependency order. */
    private static void runPipeline(
        MigrationOrigin<?> from,
        MigrationOrigin<?> to,
        IgniteToIgniteMigrationConfiguration mCfg,
        ExecutorService pool
    ) throws IgniteCheckedException {
        // All current channels require thick client (IgniteEx) endpoints.
        if (!(from.origin() instanceof IgniteEx) || !(to.origin() instanceof IgniteEx))
            throw new IgniteCheckedException("This migration pipeline requires IgniteEx endpoints (thick client).");

        // The check above justifies the unchecked casts.
        @SuppressWarnings("unchecked")
        MigrationOrigin<IgniteEx> src = (MigrationOrigin<IgniteEx>)from;
        @SuppressWarnings("unchecked")
        MigrationOrigin<IgniteEx> dest = (MigrationOrigin<IgniteEx>)to;

        if (src.origin().cluster().id().equals(dest.origin().cluster().id()))
            throw new IgniteCheckedException("Source and destination must be different clusters.");

        IgniteLogger log = mCfg.logger();

        // Cache selection and name mapping are needed even when cache creation is disabled.
        MigrationTransformer<MigrationCache> cacheTransformer = mCfg.transformer(CacheConfigurationsMigrationChannel.class);
        MigrationCachePlan cachePlan = new MigrationCachePlan(src.origin(), mCfg.cacheSelection(), cacheTransformer);

        if (!mCfg.noBinaryMetadataCreate())
            run(new BinaryMetadataMigrationChannel(src, dest, log), pool, mCfg.transformer(BinaryMetadataMigrationChannel.class));

        if (!mCfg.noMappingsCreate())
            run(new TypeMappingsMigrationChannel(src, dest, log), pool, mCfg.transformer(TypeMappingsMigrationChannel.class));

        run(new CacheConfigurationsMigrationChannel(src, dest, log, mCfg.cacheConfigurations(), cachePlan), pool, cacheTransformer);

        run(new MetastorageMigrationChannel(src, dest, log, mCfg.metastorage()), pool,
            mCfg.transformer(MetastorageMigrationChannel.class));

        run(new CacheDataMigrationChannel(src, dest, log, mCfg.cacheData(), cachePlan), pool,
            mCfg.transformer(CacheDataMigrationChannel.class));
    }

    /**
     * Runs a single channel with its transformer. The generic signature keeps the channel item type and the
     * transformer aligned without unchecked casts.
     */
    private static <T> void run(MigrationChannel<T> channel, ExecutorService pool, MigrationTransformer<T> transformer)
        throws IgniteCheckedException {
        channel.transfer(pool, transformer);
    }
}
