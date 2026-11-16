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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.binary.BinaryMetadata;

/** Read side of the migration. Partition cursors may be opened concurrently. */
public interface MigrationSource extends MigrationOrigin {
    /** @return Binary schemas. */
    Collection<BinaryMetadata> binaryMetadata();

    /** @return Platform mappings. */
    Collection<MigrationTypeMapping> typeMappings();

    /** @return Current cache definitions, including dynamically changed SQL entities. */
    Collection<MigrationCache> caches();

    /**
     * @param cfg Migration configuration.
     * @return Distributed settings selected by the configured filter, excluding SQL views.
     */
    Map<String, Serializable> metastorage(IgniteToIgniteMigrationConfiguration cfg) throws IgniteCheckedException;

    /**
     * @param cfg Migration configuration.
     * @return SQL view definitions selected by the configured metadata filter.
     */
    Map<String, Serializable> sqlViews(IgniteToIgniteMigrationConfiguration cfg) throws IgniteCheckedException;

    /**
     * @param cacheNames Source to destination cache names.
     * @return Partition tasks interleaved by primary server.
     */
    List<MigrationPartition> partitions(Map<String, String> cacheNames);

    /**
     * @param part Source partition.
     * @param cfg Migration configuration.
     * @return Cursor to be closed by the caller.
     */
    QueryCursor<MigrationEntry> read(MigrationPartition part, IgniteToIgniteMigrationConfiguration cfg);
}
