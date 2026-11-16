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

package org.apache.ignite.util.migration.impl.channel;

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.impl.MigrationCachePlan;

/** Reads and transfers SQL view definitions after their underlying tables have been created. */
public class SqlViewsMigrationChannel extends AbstractMigrationChannel<Map.Entry<String, Serializable>> {
    /** */
    private final MigrationCachePlan cachePlan;

    /**
     * @param src Connected source.
     * @param dest Connected destination.
     * @param cfg Migration configuration.
     * @param cachePlan Cache selection for this migration.
     */
    public SqlViewsMigrationChannel(
        MigrationSource src,
        MigrationDestination dest,
        IgniteToIgniteMigrationConfiguration cfg,
        MigrationCachePlan cachePlan
    ) {
        super(src, dest, cfg);

        this.cachePlan = cachePlan;
    }

    /** {@inheritDoc} */
    @Override protected void transfer0() throws IgniteCheckedException {
        if (!cachePlan.keepViews()) {
            info("Skipped SQL views during partial or renamed cache migration");

            return;
        }

        for (Map.Entry<String, Serializable> item : src.sqlViews(cfg).entrySet()) {
            Map.Entry<String, Serializable> entry = transform(new SimpleImmutableEntry<>(item));

            if (entry != null) {
                dest.sqlView(entry.getKey(), entry.getValue());

                info("Transferred SQL view: " + entry.getKey());
            }
        }
    }
}
