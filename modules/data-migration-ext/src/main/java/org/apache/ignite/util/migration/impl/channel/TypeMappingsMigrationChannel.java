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

import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.api.MigrationTypeMapping;

/** Transfers platform-specific type mappings without regenerating their IDs. */
public class TypeMappingsMigrationChannel extends AbstractMigrationChannel<MigrationTypeMapping> {
    /**
     * @param src Connected source.
     * @param dest Connected destination.
     * @param cfg Migration configuration.
     */
    public TypeMappingsMigrationChannel(
        MigrationSource src,
        MigrationDestination dest,
        IgniteToIgniteMigrationConfiguration cfg
    ) {
        super(src, dest, cfg);
    }

    /** {@inheritDoc} */
    @Override protected void transfer0() {
        for (MigrationTypeMapping item : src.typeMappings()) {
            MigrationTypeMapping mapping = transform(item);

            if (mapping != null) {
                dest.typeMapping(mapping);

                info("Transferred type mapping: " + mapping.typeName());
            }
        }
    }
}
