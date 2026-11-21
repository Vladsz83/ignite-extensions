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
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCache;
import org.apache.ignite.util.migration.category.cache.data.MigrationEntry;
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
    public static ThickClientMigrationEndpoint create(IgniteConfiguration cfg) throws IgniteCheckedException {
        if (!Boolean.TRUE.equals(cfg.isClientMode()))
            throw new IgniteCheckedException("Migration requires a client-mode in IgniteConfiguration.");

        return new ThickClientMigrationEndpoint(cfg);
    }

    /** {@inheritDoc} */
    /** {@inheritDoc} */
    @Override public LinkedHashMap<MigrationCategory, Class<?>> supportedCategories() {
        LinkedHashMap<MigrationCategory, Class<?>> categories = new LinkedHashMap<>();

        categories.put(MigrationCategory.BINARY_METADATA, BinaryMetadata.class);
        categories.put(MigrationCategory.TYPE_MAPPING, MigrationTypeMapping.class);
        categories.put(MigrationCategory.CACHE_CONFIGURATION, MigrationCache.class);
        categories.put(MigrationCategory.METASTORAGE_ENTRY, Map.Entry.class);
        categories.put(MigrationCategory.CACHE_ENTRY, MigrationEntry.class);

        return categories;
    }

    /** {@inheritDoc} */
    @Override public void accept(MigrationCategory category, Object data) throws IgniteCheckedException {
        if (ig == null)
            throw new IgniteCheckedException("Migration destination is not connected.");

        try {
            switch (category) {
                case BINARY_METADATA:
                    if (!(data instanceof BinaryMetadata))
                        throw new IgniteCheckedException("Expected BinaryMetadata for migration category: " + category);

                    BinaryMetadata metadata = (BinaryMetadata)data;

                    ig.context().cacheObjects().binaryContext().updateMetadata(metadata.typeId(), metadata, false);

                    return;

                case TYPE_MAPPING:
                    if (!(data instanceof MigrationTypeMapping))
                        throw new IgniteCheckedException("Expected MigrationTypeMapping for migration category: " + category);

                    MigrationTypeMapping mapping = (MigrationTypeMapping)data;

                    ig.context().cacheObjects().binaryContext().registerUserClassName(
                        mapping.typeId(), mapping.typeName(), false, false, mapping.platformId());

                    return;

                default:
                    throw new IgniteCheckedException("Destination does not accept migration category: " + category);
            }
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to accept migration data [category=" + category + "].", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized IgniteEx connect() throws IgniteCheckedException {
        if (closed)
            throw new IgniteCheckedException("Migration client node is already closed.");

        if (ig != null)
            throw new IgniteCheckedException("Migration client node is already connected.");

        try {
            ig = (IgniteEx)Ignition.start(cfg);

            if (!ig.cluster().state().active())
                throw new IgniteCheckedException("Migration requires an active cluster.");

            return ig;
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to start migration client node.", e);
        }
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
