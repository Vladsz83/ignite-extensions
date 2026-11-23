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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.category.cache.configuration.CacheConfigurationMigration;
import org.apache.ignite.util.migration.category.cache.data.CacheEntryMigration;
import org.apache.ignite.util.migration.category.type.MigrationTypeMapping;

/** Thick client (in-JVM) migration endpoint. */
public class ThickClientMigrationEndpoint implements MigrationDestination {
    /** */
    private final IgniteConfiguration cfg;

    /** */
    private IgniteEx ig;

    /** */
    private boolean closed;

    /** */
    private ThickClientMigrationEndpoint(IgniteConfiguration cfg) {
        this.cfg = new IgniteConfiguration(cfg);
    }

    /**
     * @param cfg Client node configuration.
     * @return Migration endpoint.
     */
    public static ThickClientMigrationEndpoint create(IgniteConfiguration cfg) {
        if (!Boolean.TRUE.equals(cfg.isClientMode()))
            throw new IgniteException("Migration requires a client-mode in IgniteConfiguration.");

        return new ThickClientMigrationEndpoint(cfg);
    }

    /** {@inheritDoc} */
    /** {@inheritDoc} */
    @Override public LinkedHashMap<MigrationCategory, Class<?>> supportedCategories() {
        LinkedHashMap<MigrationCategory, Class<?>> categories = new LinkedHashMap<>();

        categories.put(MigrationCategory.BINARY_METADATA, BinaryMetadata.class);
        categories.put(MigrationCategory.TYPE_MAPPING, MigrationTypeMapping.class);
        categories.put(MigrationCategory.CACHE_CONFIGURATION, CacheConfigurationMigration.class);
        categories.put(MigrationCategory.METASTORAGE_ENTRY, Map.Entry.class);
        categories.put(MigrationCategory.CACHE_ENTRY, CacheEntryMigration.class);

        return categories;
    }

    /** {@inheritDoc} */
    @Override public void accept(MigrationCategory category, Object entity) {
        if (ig == null)
            throw new IgniteException("Migration destination is not connected.");

        switch (category) {
            case BINARY_METADATA:
                if (!(entity instanceof BinaryMetadata))
                    throw new IgniteException("Expected BinaryMetadata for migration category: " + category);

                BinaryMetadata metadata = (BinaryMetadata)entity;

                ig.context().cacheObjects().binaryContext().updateMetadata(metadata.typeId(), metadata, false);

                return;

            case TYPE_MAPPING:
                if (!(entity instanceof MigrationTypeMapping))
                    throw new IgniteException("Expected MigrationTypeMapping for migration category: " + category);

                MigrationTypeMapping mapping = (MigrationTypeMapping)entity;

                ig.context().cacheObjects().binaryContext().registerUserClassName(
                    mapping.typeId(), mapping.typeName(), false, false, mapping.platformId());

                return;

            case CACHE_CONFIGURATION:
                if (!(entity instanceof CacheConfigurationMigration))
                    throw new IgniteException("Expected CacheConfigurationMigration for migration category: " + category);

                acceptCacheConfiguration((CacheConfigurationMigration)entity);

                return;

            case METASTORAGE_ENTRY:
                if (!(entity instanceof Map.Entry))
                    throw new IgniteException("Expected Map.Entry for migration category: " + category);

                Map.Entry<?, ?> entry = (Map.Entry<?, ?>)entity;

                if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof Serializable))
                    throw new IgniteException("Expected a string key and serializable value for migration category: " + category);

                try {
                    ig.context().distributedMetastorage().write((String)entry.getKey(), (Serializable)entry.getValue());
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to write metastorage entry [key=" + entry.getKey() + "].", e);
                }

                return;

            default:
                throw new IgniteException("Destination does not accept migration category: " + category);
        }
    }

    /** Creates or validates a cache definition using the policy carried by the migration entity. */
    private void acceptCacheConfiguration(CacheConfigurationMigration cache) {
        if (ig == null)
            throw new IgniteException("Migration destination is not connected.");

        String name = cache.configuration().getName();

        if (cache.noCacheCreate()) {
            if (ig.context().cache().cacheDescriptor(name) == null)
                throw new IgniteException("Destination cache does not exist [cache=" + name + "].");

            try {
                ig.context().cache().getOrStartCache(name);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to open migration cache [cache=" + name + "].", e);
            }

            return;
        }

        try {
            boolean created = ig.context().cache().dynamicStartCache(cache.configuration(), name, null,
                cache.type(), cache.sql(), false, true, true).get();

            if (!created && !cache.createCacheIfNotExists())
                throw new CacheExistsException("Destination cache already exists [cache=" + name + "].");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to create destination cache [cache=" + name + "].", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized IgniteEx connect() {
        if (closed)
            throw new IgniteException("Migration client node is already closed.");

        if (ig == null) {
            ig = (IgniteEx)Ignition.start(cfg);

            if (!ig.cluster().state().active())
                throw new IgniteException("Migration requires an active cluster.");
        }

        return ig;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (!closed) {
            closed = true;

            if (ig != null)
                ig.close();
        }
    }
}
