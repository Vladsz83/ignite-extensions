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

package org.apache.ignite.util.migration.endpoint;

import java.util.LinkedHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.client.thin.ClientBinary;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationMigration;
import org.apache.ignite.util.migration.category.cache.configuration.ThinClientCacheConfigurationsTransferrer;
import org.apache.ignite.util.migration.category.cache.data.CacheEntryMigration;
import org.apache.ignite.util.migration.category.type.MigrationTypeMapping;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/** Thin-client migration endpoint. */
public class ThinClientMigrationEndpoint implements MigrationDestination {
    /** Client configuration. */
    private final ClientConfiguration cfg;

    /** Connected client. */
    private IgniteClient client;

    /** Whether this endpoint has been closed. */
    private boolean closed;

    /**
     * @param cfg Thin-client configuration.
     */
    private ThinClientMigrationEndpoint(ClientConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * @param cfg Thin-client configuration.
     * @return Migration endpoint.
     */
    public static ThinClientMigrationEndpoint create(ClientConfiguration cfg) {
        if (cfg == null)
            throw new IllegalArgumentException("Client configuration must not be null.");

        return new ThinClientMigrationEndpoint(cfg);
    }

    /** {@inheritDoc} */
    /** {@inheritDoc} */
    @Override public LinkedHashMap<MigrationCategory, Class<?>> supportedCategories() {
        LinkedHashMap<MigrationCategory, Class<?>> categories = new LinkedHashMap<>();

        categories.put(MigrationCategory.BINARY_METADATA, BinaryMetadata.class);
        categories.put(MigrationCategory.TYPE_MAPPING, MigrationTypeMapping.class);
        categories.put(MigrationCategory.CACHE_CONFIGURATION, CacheConfigurationMigration.class);
        categories.put(MigrationCategory.CACHE_ENTRY, CacheEntryMigration.class);

        return categories;
    }

    /** {@inheritDoc} */
    @Override public void accept(MigrationCategory category, Object entity) {
        if (client == null)
            throw new IgniteException("Migration destination is not connected.");

        Object binary = client.binary();

        if (!(binary instanceof ClientBinary))
            throw new IgniteException("Thin client does not expose its binary metadata handler.");

        if (category == MigrationCategory.BINARY_METADATA) {
            if (!(entity instanceof BinaryMetadata))
                throw new IgniteException("Expected BinaryMetadata for migration category: " + category);

            BinaryMetadata metadata = (BinaryMetadata)entity;

            ((ClientBinary)binary).binaryContext().updateMetadata(metadata.typeId(), metadata, false);

            return;
        }

        if (category == MigrationCategory.TYPE_MAPPING) {
            if (!(entity instanceof MigrationTypeMapping))
                throw new IgniteException("Expected MigrationTypeMapping for migration category: " + category);

            MigrationTypeMapping mapping = (MigrationTypeMapping)entity;

            if (mapping.platformId() != JAVA_ID) {
                IgniteLogger log = cfg.getLogger();

                if (log != null)
                    log.warning("Thin client cannot register a non-Java platform mapping; leaving it unmapped " +
                        "[platformId=" + mapping.platformId() + ", typeName=" + mapping.typeName() + "].");

                return;
            }

            ((ClientBinary)binary).binaryContext().registerUserClassName(
                mapping.typeId(), mapping.typeName(), false, false, mapping.platformId());

            return;
        }

        if (category == MigrationCategory.CACHE_CONFIGURATION) {
            if (!(entity instanceof CacheConfigurationMigration))
                throw new IgniteException("Expected CacheConfigurationMigration for migration category: " + category);

            acceptCacheConfiguration((CacheConfigurationMigration)entity);

            return;
        }

        throw new IgniteException("Destination does not accept migration category: " + category);
    }

    /** Creates a cache through the thin-client API using the policy carried in the entity. */
    private void acceptCacheConfiguration(CacheConfigurationMigration cache) {
        if (client == null)
            throw new IgniteException("Migration destination is not connected.");

        String name = cache.configuration().getName();
        boolean exists = client.cacheNames().contains(name);

        if (cache.noCacheCreate()) {
            if (!exists)
                throw new IgniteException("Destination cache does not exist [cache=" + name + "].");

            return;
        }

        if (exists && !cache.createCacheIfNotExists())
            throw new CacheExistsException("Destination cache already exists [cache=" + name + "].");

        if (exists)
            return;

        if (cache.configuration().getAffinity() != null &&
            !(cache.configuration().getAffinity() instanceof RendezvousAffinityFunction)) {
            IgniteLogger log = cfg.getLogger();

            if (log != null)
                log.warning("Thin destination cannot represent the cache's custom affinity; using default rendezvous " +
                    "affinity [cache=" + name + "].");
        }

        ClientCacheConfiguration clientCfg = ThinClientCacheConfigurationsTransferrer
            .toClientCacheConfiguration(cache.configuration());

        try {
            if (cache.createCacheIfNotExists())
                client.getOrCreateCache(clientCfg);
            else
                client.createCache(clientCfg);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Failed to create destination cache through the thin client [cache=" + name +
                "].", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized IgniteClient connect() {
        if (closed)
            throw new IgniteException("Migration thin client is already closed.");

        if (client == null) {
            client = Ignition.startClient(cfg);

            if (!client.cluster().state().active())
                throw new IgniteException("Migration requires an active cluster.");
        }

        return client;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (!closed) {
            closed = true;

            if (client != null)
                client.close();
        }
    }
}
