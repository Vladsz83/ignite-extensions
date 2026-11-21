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
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransfer;
import org.jetbrains.annotations.Nullable;

/**
 * Transfers all distributed metastorage entries. A default key filter skips entries that are not
 * portable to the destination cluster unless disabled in the configuration: the cluster identity,
 * graceful-stop node ids, and SQL views when the destination has no SQL schema. Configured key
 * filters and a transformer can additionally skip or transform entries.
 */
public class MetastorageTransferrer extends AbstractCategoryTransfer<Map.Entry<String, Serializable>> {
    /** Source node. */
    private final IgniteEx from;

    /** Destination node. */
    private final IgniteEx to;

    /** */
    private static final String VIEW_PREFIX = "ignite.internal.sql.view.";

    /** */
    private static final String CLUSTER_ID_TAG_KEY = "ignite.internal.cluster.id.tag";

    /** */
    private static final String GRACEFUL_SHUTDOWN_KEY = "ignite.internal.graceful.shutdown";

    /** */
    private final MetastorageMigrationConfiguration cfg;

    /** */
    private Boolean hasSqlSchemas;

    /** Lazily computed; includes the SQL view prefix only when the destination has no SQL schema. */
    private MetastorageKeyFilterTransformer notPortableKeyFilter;

    /**
     * @param from Migration source.
     * @param to Migration destination.
     * @param log Logger.
     * @param cfg Metastorage transfer settings.
     */
    public MetastorageTransferrer(
        IgniteEx from,
        IgniteEx to,
        IgniteLogger log,
        MetastorageMigrationConfiguration cfg
    ) {
        super(log);

        this.from = from;
        this.to = to;
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public MigrationCategory category() {
        return MigrationCategory.METASTORAGE_ENTRY;
    }

    /** {@inheritDoc} */
    @Override public Class<?> dataClass() {
        return Map.Entry.class;
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(
        MigrationDestination dest,
        @Nullable MigrationTransformer<Map.Entry<String, Serializable>> transformer
    ) throws IgniteCheckedException {
        MetastorageKeyFilterTransformer notPortableFilter =
            cfg.ignoreDefaultFilter() ? null : notPortableKeyFilter();

        from.context().distributedMetastorage().iterate("", (key, val) -> {
            Map.Entry<String, Serializable> candidate = new SimpleImmutableEntry<>(key, val);

            // The default filter runs first, on the original key, so a later filter or transformer
            // cannot re-enable copying of a non-portable key (e.g. by renaming it).
            if (notPortableFilter != null && notPortableFilter.transform(candidate) == null) {
                info("Skipped distributed metastorage entry [key='" + key + "', reason='not portable to the destination'].");

                return;
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
                to.context().distributedMetastorage().write(candidate.getKey(), candidate.getValue());

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
            hasSqlSchemas = to.context().cache().cacheDescriptors().values().stream()
                .anyMatch(DynamicCacheDescriptor::sql);

        return hasSqlSchemas;
    }

    /**
     * Lazily builds the not-portable key filter. The SQL view prefix is included only when the
     * destination has no SQL schema, because views cannot be attached in that case.
     */
    private MetastorageKeyFilterTransformer notPortableKeyFilter() {
        if (notPortableKeyFilter == null) {
            List<String> prefixes = hasSqlSchemas()
                ? Collections.emptyList()
                : Collections.singletonList(VIEW_PREFIX);

            notPortableKeyFilter = new MetastorageKeyFilterTransformer(
                Arrays.asList(CLUSTER_ID_TAG_KEY, GRACEFUL_SHUTDOWN_KEY),
                prefixes
            );
        }

        return notPortableKeyFilter;
    }

}
