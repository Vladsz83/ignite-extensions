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

package org.apache.ignite.util.migration.category.metastorage;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.util.migration.MigrationOrigin;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractMigrationChannel;
import org.jetbrains.annotations.Nullable;

/**
 * Transfers all distributed metastorage entries. A default key filter skips entries that are not
 * portable to the destination cluster unless disabled in the configuration: the cluster identity,
 * graceful-stop node ids, and SQL views when the destination has no SQL schema. Configured key
 * filters and a transformer can additionally skip or transform entries.
 */
public class MetastorageMigrationChannel extends AbstractMigrationChannel<IgniteEx, IgniteEx, Map.Entry<String, Serializable>> {
    /** */
    private static final String VIEW_PREFIX = "ignite.internal.sql.view.";

    /** */
    private static final String CLUSTER_ID_TAG_KEY = "ignite.internal.cluster.id.tag";

    /** */
    private static final String GRACEFUL_SHUTDOWN_KEY = "ignite.internal.graceful.shutdown";

    /**
     * Keys that are not portable to another cluster: the destination adopts the cluster ID from the
     * identity key on startup, and the graceful-stop set holds source node ids.
     */
    private static final MetastorageKeyFilterTransformer NOT_PORTABLE_KEY_FILTER =
        new MetastorageKeyFilterTransformer(
            new HashSet<>(Arrays.asList(CLUSTER_ID_TAG_KEY, GRACEFUL_SHUTDOWN_KEY)),
            Collections.emptyList()
        );

    /** */
    private final MetastorageMigrationConfiguration cfg;

    /** */
    private Boolean hasSqlSchemas;

    /**
     * @param from Migration source.
     * @param to Migration destination.
     * @param log Logger.
     * @param cfg Metastorage transfer settings.
     */
    public MetastorageMigrationChannel(
        MigrationOrigin<IgniteEx> from,
        MigrationOrigin<IgniteEx> to,
        IgniteLogger log,
        MetastorageMigrationConfiguration cfg
    ) {
        super(from, to, log);
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(
        ExecutorService pool,
        @Nullable MigrationTransformer<Map.Entry<String, Serializable>> transformer
    ) throws IgniteCheckedException {
        from.origin().context().distributedMetastorage().iterate("", (key, val) -> {
            Map.Entry<String, Serializable> candidate = new SimpleImmutableEntry<>(key, val);

            // The default filter runs first, on the original key, so a later filter or transformer
            // cannot re-enable copying of a non-portable key (e.g. by renaming it).
            if (!cfg.ignoreDefaultFilter()) {
                // SQL views are not portable unless the destination has a SQL schema to attach them to.
                if (key.startsWith(VIEW_PREFIX) && !hasSqlSchemas()) {
                    info("Skipped distributed metastorage entry [key='" + key + "', reason='no SQL schema on destination'].");

                    return;
                }

                if (NOT_PORTABLE_KEY_FILTER.transform(candidate) == null) {
                    info("Skipped distributed metastorage entry [key='" + key + "', reason='not portable to the destination'].");

                    return;
                }
            }

            for (MetastorageKeyFilterTransformer filter : cfg.filters()) {
                if (filter.transform(candidate) == null) {
                    info("Skipped distributed metastorage entry [key='" + key + "', reason='filtered out'].");

                    return;
                }
            }

            if (transformer != null)
                candidate = transformer.transform(candidate);

            if (candidate == null) {
                info("Skipped distributed metastorage entry [key='" + key + "', reason='filtered out'].");

                return;
            }

            try {
                to.origin().context().distributedMetastorage().write(candidate.getKey(), candidate.getValue());

                info("Transferred distributed metadata: " + candidate.getKey() + '.');
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(
                    "Failed to transfer distributed metastorage entry [name=" + candidate.getKey() + ", error=" + e.getMessage() + "].",
                    e
                );
            }
        });
    }

    /** Whether any SQL schema exists on the destination (computed once). */
    private boolean hasSqlSchemas() {
        if (hasSqlSchemas == null)
            hasSqlSchemas = to.origin().context().cache().cacheDescriptors().values().stream()
                .anyMatch(DynamicCacheDescriptor::sql);

        return hasSqlSchemas;
    }
}
