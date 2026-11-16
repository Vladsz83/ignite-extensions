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

package org.apache.ignite.util.migration.api;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryMetadata;

/** Write side of the migration. The runner may invoke {@link #write} concurrently. */
public interface MigrationDestination extends MigrationOrigin {
    /** @param meta Binary metadata to register. */
    void binaryMetadata(BinaryMetadata meta);

    /** @param mapping Type mapping to register. */
    void typeMapping(MigrationTypeMapping mapping);

    /**
     * Creates or validates a destination cache before any records are written.
     *
     * @param cache Cache definition.
     * @param cfg Migration configuration.
     */
    void cache(MigrationCache cache, IgniteToIgniteMigrationConfiguration cfg);

    /**
     * @param key Distributed metadata key.
     * @param val Distributed metadata value.
     */
    void metastorage(String key, Serializable val) throws IgniteCheckedException;

    /**
     * @param key SQL view key.
     * @param val SQL view definition.
     */
    void sqlView(String key, Serializable val) throws IgniteCheckedException;

    /**
     * Accepts one batch. Buffered destinations must acknowledge all pending writes in {@link #finish()}.
     *
     * @param cache Destination cache name.
     * @param entries Entries to write.
     * @param cfg Migration configuration.
     */
    void write(String cache, List<MigrationEntry> entries, IgniteToIgniteMigrationConfiguration cfg)
        throws IgniteCheckedException;

    /** Completes and acknowledges all buffered writes before the migration is reported as successful. */
    default void finish() throws IgniteCheckedException {
        // No-op.
    }
}
