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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheType;

/** A cache definition, retaining the SQL-created flag and the original source name. */
public class CacheConfigurationMigration {
    /** Original source cache name, retained if a transformer renames the destination cache. */
    private final String srcName;

    /** Cache configuration used to create or validate the cache on the destination. */
    private final CacheConfiguration<?, ?> cfg;

    /** Whether the source cache was created by SQL DDL, such as {@code CREATE TABLE}. */
    private final boolean sql;

    /** Ignite cache type, such as a user cache or a data structures cache. */
    private final CacheType type;

    /** Whether destination cache creation is disabled for this migration. */
    private final boolean noCacheCreate;

    /** Whether the destination's existing cache is accepted. */
    private final boolean createCacheIfNotExists;

    /**
     * @param srcName Source cache name.
     * @param cfg Cache configuration.
     * @param sql Whether the source cache was created by SQL DDL.
     * @param type Cache type.
     */
    public CacheConfigurationMigration(String srcName, CacheConfiguration<?, ?> cfg, boolean sql, CacheType type) {
        this(srcName, cfg, sql, type, false, false);
    }

    /**
     * @param srcName Source cache name.
     * @param cfg Cache configuration.
     * @param sql Whether the source cache was created by SQL DDL.
     * @param type Cache type.
     * @param noCacheCreate Whether destination cache creation is disabled.
     * @param createCacheIfNotExists Whether an existing destination cache is accepted.
     */
    public CacheConfigurationMigration(String srcName, CacheConfiguration<?, ?> cfg, boolean sql, CacheType type,
        boolean noCacheCreate, boolean createCacheIfNotExists) {
        this.srcName = srcName;
        this.cfg = cfg;
        this.sql = sql;
        this.type = type;
        this.noCacheCreate = noCacheCreate;
        this.createCacheIfNotExists = createCacheIfNotExists;
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

    /** @return Whether destination cache creation is disabled. */
    public boolean noCacheCreate() {
        return noCacheCreate;
    }

    /** @return Whether an existing destination cache is accepted. */
    public boolean createCacheIfNotExists() {
        return createCacheIfNotExists;
    }
}
