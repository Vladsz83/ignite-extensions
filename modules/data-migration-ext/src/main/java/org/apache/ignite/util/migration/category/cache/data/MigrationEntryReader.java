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

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Scan transformer reading value, version and expiry from one row. Ordinary ScanQuery entries do not expose
 * this metadata. A second local row lookup avoids combining a scanned value with metadata from a later update.
 * Requires this extension on source servers or peer class loading.
 */
public class MigrationEntryReader implements IgniteClosure<Cache.Entry<Object, Object>, MigrationEntry> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final String cacheName;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** @param cacheName Source cache name. */
    public MigrationEntryReader(String cacheName) {
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public MigrationEntry apply(Cache.Entry<Object, Object> entry) {
        GridCacheContext<Object, Object> ctx = ((IgniteEx)ignite).cachex(cacheName).context();

        ctx.shared().database().checkpointReadLock();

        try {
            CacheDataRow row = ctx.offheap().read(ctx, ctx.toCacheKeyObject(entry.getKey()));

            if (row == null || row.expireTime() > 0 && row.expireTime() <= System.currentTimeMillis())
                return null;

            return new MigrationEntry(
                CacheObjectUtils.unwrapBinaryIfNeeded(ctx.cacheObjectContext(), row.key(), true, false),
                CacheObjectUtils.unwrapBinaryIfNeeded(ctx.cacheObjectContext(), row.value(), true, false),
                row.expireTime(), row.version());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to read migration row [cache=" + cacheName + "].", e);
        }
        finally {
            ctx.shared().database().checkpointReadUnlock();
        }
    }
}
