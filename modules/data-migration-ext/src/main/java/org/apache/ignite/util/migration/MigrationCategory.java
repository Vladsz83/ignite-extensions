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

/** Categories of entities transferred between migration endpoints. Category IDs are stable and must never be reused. */
public enum MigrationCategory {
    /** Fallback category for custom or not-yet-classified migration data. */
    OTHER(0),

    /** Binary type definitions and schemas. */
    BINARY_METADATA(1),

    /** Platform-specific type ID to name mappings. */
    TYPE_MAPPING(2),

    /** Cache and SQL cache definitions. */
    CACHE_CONFIGURATION(3),

    /** Distributed metastorage entries. */
    METASTORAGE_ENTRY(4),

    /** Cache records, including their key, value and available entry metadata. */
    CACHE_ENTRY(5);

    /** Stable category ID for serialization and compatibility. */
    private final short id;

    /**
     * @param id Stable category ID. New categories must use IDs greater than all existing categories.
     */
    MigrationCategory(int id) {
        this.id = (short)id;
    }

    /** @return Stable category ID. */
    public int id() {
        return id;
    }
}
