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

package org.apache.ignite.util.migration.category.cache.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientAffinityConfiguration;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;

/** Converts the subset of cache configuration exposed by the thin-client API. */
public final class ThinClientCacheConfigurationAdapter {
    /** Utility class. */
    private ThinClientCacheConfigurationAdapter() {
        // No-op.
    }

    /**
     * Converts a thin-client configuration to a detached full configuration. Properties absent from
     * {@link ClientCacheConfiguration} retain Ignite defaults.
     *
     * @param cfg Thin-client cache configuration.
     * @return Full cache configuration containing the properties available through the thin client.
     */
    public static CacheConfiguration<?, ?> toCacheConfiguration(ClientCacheConfiguration cfg) {
        CacheConfiguration<Object, Object> res = new CacheConfiguration<>();

        res.setName(cfg.getName());
        res.setAtomicityMode(cfg.getAtomicityMode());
        res.setBackups(cfg.getBackups());
        res.setCacheMode(cfg.getCacheMode());
        res.setGroupName(cfg.getGroupName());
        res.setEagerTtl(cfg.isEagerTtl());
        res.setPartitionLossPolicy(cfg.getPartitionLossPolicy());
        res.setReadFromBackup(cfg.isReadFromBackup());
        res.setWriteSynchronizationMode(cfg.getWriteSynchronizationMode());
        res.setCopyOnRead(cfg.isCopyOnRead());
        res.setDataRegionName(cfg.getDataRegionName());
        res.setStatisticsEnabled(cfg.isStatisticsEnabled());
        res.setSqlEscapeAll(cfg.isSqlEscapeAll());
        res.setSqlIndexMaxInlineSize(cfg.getSqlIndexMaxInlineSize());
        res.setSqlSchema(cfg.getSqlSchema());
        QueryEntity[] entities = copyQueryEntities(cfg.getQueryEntities());

        res.setQueryEntities(entities == null ? null : Arrays.asList(entities));

        if (cfg.getKeyConfiguration() != null)
            res.setKeyConfiguration(cfg.getKeyConfiguration().clone());

        if (cfg.getExpiryPolicy() != null) {
            ExpiryPolicy expiry = cfg.getExpiryPolicy();

            res.setExpiryPolicyFactory((Factory<ExpiryPolicy>)() -> expiry);
        }

        ClientAffinityConfiguration aff = cfg.getAffinityConfiguration();

        if (aff != null && aff.getPartitions() > 0)
            res.setAffinity(new RendezvousAffinityFunction(aff.isExcludeNeighbors(), aff.getPartitions()));

        return res;
    }

    /**
     * Converts a full configuration to the subset supported by thin-client cache creation.
     * Custom affinity functions cannot be represented by the thin-client configuration; the default affinity
     * function is used instead, with the source partition count retained when available.
     *
     * @param cfg Full cache configuration.
     * @return Thin-client cache configuration.
     */
    public static ClientCacheConfiguration toClientCacheConfiguration(CacheConfiguration<?, ?> cfg) {
        ClientCacheConfiguration res = new ClientCacheConfiguration()
            .setName(cfg.getName())
            .setAtomicityMode(cfg.getAtomicityMode())
            .setBackups(cfg.getBackups())
            .setCacheMode(cfg.getCacheMode())
            .setGroupName(cfg.getGroupName())
            .setEagerTtl(cfg.isEagerTtl())
            .setPartitionLossPolicy(cfg.getPartitionLossPolicy())
            .setReadFromBackup(cfg.isReadFromBackup())
            .setWriteSynchronizationMode(cfg.getWriteSynchronizationMode())
            .setCopyOnRead(cfg.isCopyOnRead())
            .setStatisticsEnabled(cfg.isStatisticsEnabled())
            .setSqlEscapeAll(cfg.isSqlEscapeAll())
            .setSqlIndexMaxInlineSize(cfg.getSqlIndexMaxInlineSize())
            .setSqlSchema(cfg.getSqlSchema())
            .setQueryEntities(copyQueryEntities(cfg.getQueryEntities()));

        if (cfg.getDataRegionName() != null)
            res.setDataRegionName(cfg.getDataRegionName());

        if (cfg.getKeyConfiguration() != null)
            res.setKeyConfiguration(cfg.getKeyConfiguration().clone());

        Factory<ExpiryPolicy> expiryFactory = cfg.getExpiryPolicyFactory();

        if (expiryFactory != null)
            res.setExpiryPolicy(expiryFactory.create());

        if (cfg.getAffinity() instanceof RendezvousAffinityFunction) {
            RendezvousAffinityFunction aff = (RendezvousAffinityFunction)cfg.getAffinity();

            res.setAffinityConfiguration(new ClientAffinityConfiguration()
                .setPartitions(aff.getPartitions())
                .setExcludeNeighbors(aff.isExcludeNeighbors()));
        }

        return res;
    }

    /** Makes detached copies of query entities. */
    private static QueryEntity[] copyQueryEntities(QueryEntity[] entities) {
        if (entities == null)
            return null;

        List<QueryEntity> res = new ArrayList<>(entities.length);

        for (QueryEntity entity : entities)
            res.add(new QueryEntity(entity));

        return res.toArray(new QueryEntity[0]);
    }

    /** Makes detached copies of full cache configuration query entities. */
    private static QueryEntity[] copyQueryEntities(Collection<QueryEntity> entities) {
        if (entities == null)
            return null;

        List<QueryEntity> res = new ArrayList<>(entities.size());

        for (QueryEntity entity : entities)
            res.add(new QueryEntity(entity));

        return res.toArray(new QueryEntity[0]);
    }
}
