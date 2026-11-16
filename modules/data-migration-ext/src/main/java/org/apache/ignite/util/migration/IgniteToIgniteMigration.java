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
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationChannel;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.impl.MigrationCachePlan;
import org.apache.ignite.util.migration.impl.ThickClientMigrationOrigin;
import org.apache.ignite.util.migration.impl.channel.BinaryMetadataMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.CacheConfigurationsMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.CacheDataMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.MetastorageMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.SqlViewsMigrationChannel;
import org.apache.ignite.util.migration.impl.channel.TypeMappingsMigrationChannel;

/** Connects migration endpoints and runs their transfer channels in dependency order. */
public class IgniteToIgniteMigration {
    /**
     * Runs a migration, closing both endpoints on success or failure.
     *
     * @param src Source endpoint.
     * @param dest Destination endpoint.
     * @param cfg Migration configuration.
     */
    public static void start(
        MigrationSource src,
        MigrationDestination dest,
        IgniteToIgniteMigrationConfiguration cfg
    ) throws IgniteCheckedException {
        cfg.validate();

        IgniteLogger log = cfg.getLogger();
        long start = System.currentTimeMillis();

        if (log.isInfoEnabled()) {
            log.info("Starting data migration [time=" + start + ", batchSize=" + cfg.getBatchSize() +
                ", readThreads=" + cfg.getReadThreads() + ", writeThreads=" + cfg.getWriteThreads() +
                ", readLimit=" + cfg.getReadLimitInSec() + ", writeLimit=" + cfg.getWriteLimitInSec() + ']');
        }

        try (src; dest) {
            src.connect();
            dest.connect();

            if (src instanceof ThickClientMigrationOrigin && dest instanceof ThickClientMigrationOrigin &&
                ((ThickClientMigrationOrigin)src).clusterId().equals(((ThickClientMigrationOrigin)dest).clusterId()))
                throw new IgniteCheckedException("Source and destination must be different clusters");

            List<MigrationChannel<?>> channels = prepareChannels(src, dest, cfg);

            for (MigrationChannel<?> channel : channels)
                channel.transfer();

            dest.finish();

            if (log.isInfoEnabled())
                log.info("Data migration completed [elapsedMs=" + (System.currentTimeMillis() - start) + ']');
        }
        catch (IgniteCheckedException | RuntimeException e) {
            log.error("Data migration failed", e);

            throw e;
        }
    }

    /** Prepares the configured channels in dependency order. */
    private static List<MigrationChannel<?>> prepareChannels(
        MigrationSource src,
        MigrationDestination dest,
        IgniteToIgniteMigrationConfiguration cfg
    ) {
        List<MigrationChannel<?>> channels = new ArrayList<>();

        if (!cfg.isNoBinaryMetadataCreate())
            channels.add(new BinaryMetadataMigrationChannel(src, dest, cfg));

        if (!cfg.isNoMappingsCreate())
            channels.add(new TypeMappingsMigrationChannel(src, dest, cfg));

        channels.add(new MetastorageMigrationChannel(src, dest, cfg));

        // Cache selection and name mapping are needed even when cache creation is disabled.
        MigrationCachePlan cachePlan = new MigrationCachePlan(src, cfg,
            cfg.getTransformer(CacheConfigurationsMigrationChannel.class));

        channels.add(new CacheConfigurationsMigrationChannel(src, dest, cfg, cachePlan));
        channels.add(new SqlViewsMigrationChannel(src, dest, cfg, cachePlan));
        channels.add(new CacheDataMigrationChannel(src, dest, cfg, cachePlan));

        return channels;
    }
}
