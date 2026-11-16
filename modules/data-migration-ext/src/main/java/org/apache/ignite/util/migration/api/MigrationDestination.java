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
    /** Registers a schema without changing its type ID. */
    void binaryMetadata(BinaryMetadata meta);

    /** Registers a mapping without changing its type ID. */
    void typeMapping(MigrationTypeMapping mapping);

    /** Creates or validates a destination cache before any records are written. */
    void cache(MigrationCache cache, IgniteToIgniteMigrationConfiguration cfg);

    /** Writes a selected distributed metadata item. */
    void metastorage(String key, Serializable val) throws IgniteCheckedException;

    /** Writes one batch synchronously; failures must be propagated to the runner. */
    void write(String cache, List<MigrationEntry> entries, IgniteToIgniteMigrationConfiguration cfg)
        throws IgniteCheckedException;
}
