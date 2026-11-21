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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.client.thin.ClientBinary;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCache;
import org.apache.ignite.util.migration.category.cache.data.MigrationEntry;
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
        categories.put(MigrationCategory.CACHE_CONFIGURATION, MigrationCache.class);
        categories.put(MigrationCategory.CACHE_ENTRY, MigrationEntry.class);

        return categories;
    }

    /** {@inheritDoc} */
    @Override public void accept(MigrationCategory category, Object data) throws IgniteCheckedException {
        if (client == null)
            throw new IgniteCheckedException("Migration destination is not connected.");

        try {
            Object binary = client.binary();

            if (!(binary instanceof ClientBinary))
                throw new IgniteCheckedException("Thin client does not expose its binary metadata handler.");

            if (category == MigrationCategory.BINARY_METADATA) {
                if (!(data instanceof BinaryMetadata))
                    throw new IgniteCheckedException("Expected BinaryMetadata for migration category: " + category);

                BinaryMetadata metadata = (BinaryMetadata)data;

                ((ClientBinary)binary).binaryContext().updateMetadata(metadata.typeId(), metadata, false);

                return;
            }

            if (category == MigrationCategory.TYPE_MAPPING) {
                if (!(data instanceof MigrationTypeMapping))
                    throw new IgniteCheckedException("Expected MigrationTypeMapping for migration category: " + category);

                MigrationTypeMapping mapping = (MigrationTypeMapping)data;

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

            throw new IgniteCheckedException("Destination does not accept migration category: " + category);
        }
        catch (IgniteCheckedException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to accept migration data [category=" + category + "].", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized IgniteClient connect() throws IgniteCheckedException {
        if (closed)
            throw new IgniteCheckedException("Migration thin client is already closed.");

        if (client != null)
            throw new IgniteCheckedException("Migration thin client is already connected.");

        try {
            client = Ignition.startClient(cfg);

            if (!client.cluster().state().active())
                throw new IgniteCheckedException("Migration requires an active cluster.");

            return client;
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException("Failed to start migration thin client.", e);
        }
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
