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

package org.apache.ignite.util.migration.category.cache.data;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/** Value and metadata read from the same stored row. Keys and values remain binary. */
public class MigrationEntry implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Object key;

    /** */
    private final Object val;

    /** Absolute expiration timestamp, or zero for an eternal entry. */
    private final long expireTime;

    /** Original version, including the other-cluster version when present. */
    private final GridCacheVersion ver;

    /**
     * @param key Key.
     * @param val Value.
     * @param expireTime Expiration timestamp, or zero for an eternal entry.
     * @param ver Source version.
     */
    public MigrationEntry(Object key, Object val, long expireTime, GridCacheVersion ver) {
        this.key = Objects.requireNonNull(key, "key");
        this.val = Objects.requireNonNull(val, "value");
        this.expireTime = expireTime;
        this.ver = ver;
    }

    /** @return Key. */
    public Object key() {
        return key;
    }

    /** @return Value. */
    public Object value() {
        return val;
    }

    /** @return Absolute expiration timestamp. */
    public long expireTime() {
        return expireTime;
    }

    /** @return Source version. */
    public GridCacheVersion version() {
        return ver;
    }
}
