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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheType;

/** A cache definition, retaining the SQL-created flag and the original source name. */
public class MigrationCache {
    /** */
    private final String srcName;

    /** */
    private final CacheConfiguration<?, ?> cfg;

    /** */
    private final boolean sql;

    /** */
    private final CacheType type;

    /**
     * @param srcName Source cache name.
     * @param cfg Cache configuration.
     * @param sql SQL-created flag.
     */
    public MigrationCache(String srcName, CacheConfiguration<?, ?> cfg, boolean sql) {
        this(srcName, cfg, sql, CacheType.USER);
    }

    /**
     * @param srcName Source cache name.
     * @param cfg Cache configuration.
     * @param sql SQL-created flag.
     * @param type Cache type.
     */
    public MigrationCache(String srcName, CacheConfiguration<?, ?> cfg, boolean sql, CacheType type) {
        this.srcName = srcName;
        this.cfg = cfg;
        this.sql = sql;
        this.type = type;
    }

    /** @return Source cache name. */
    public String sourceName() {
        return srcName;
    }

    /** @return Cache configuration. */
    public CacheConfiguration<?, ?> configuration() {
        return cfg;
    }

    /** @return Whether the cache was created through SQL. */
    public boolean sql() {
        return sql;
    }

    /** @return Cache type. */
    public CacheType type() {
        return type;
    }
}
