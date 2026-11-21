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

package org.apache.ignite.util.migration.category.type;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.jetbrains.annotations.Nullable;

/** Provides type mappings from a connected source. */
public class TypeMappingsTransferrer extends AbstractCategoryTransferrer<IgniteEx, MigrationTypeMapping> {
    /**
     * @param from Migration source.
     * @param log Migration logger.
     */
    public TypeMappingsTransferrer(IgniteEx from, IgniteLogger log) {
        super(from, MigrationCategory.TYPE_MAPPING, MigrationTypeMapping.class, log);
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(
        MigrationDestination dest,
        @Nullable MigrationTransformer<MigrationTypeMapping> transformer
    ) throws IgniteCheckedException {
        List<Map<Integer, MappedName>> mappings = from.context().marshallerContext().getCachedMappings();

        for (int platform = 0; platform < mappings.size(); platform++) {
            for (Map.Entry<Integer, MappedName> entry : mappings.get(platform).entrySet()) {
                MigrationTypeMapping mapping =
                    new MigrationTypeMapping((byte)platform, entry.getKey(), entry.getValue().className());

                transferEntity(dest, transformer, mapping);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected String entityDescription(MigrationTypeMapping mapping) {
        return "type mapping [name='" + mapping.typeName() + "', typeId=" + mapping.typeId() +
            ", platformId=" + mapping.platformId() + "]";
    }
}
