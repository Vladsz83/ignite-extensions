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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.jetbrains.annotations.Nullable;

/** Provides binary metadata migration by an Ignite node (might be a client node). */
public class BinaryMetadataTransferrer extends AbstractCategoryTransferrer<IgniteEx, BinaryMetadata> {
    /**
     * @param from Migration source.
     * @param log Logger.
     */
    public BinaryMetadataTransferrer(IgniteEx from, IgniteLogger log) {
        super(from, MigrationCategory.BINARY_METADATA, BinaryMetadata.class, log);
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(MigrationDestination<?> dest, @Nullable MigrationTransformer<BinaryMetadata> transformer) {
        BinaryContext bCtxFrom = src.context().cacheObjects().binaryContext();

        for (BinaryType type : bCtxFrom.metadata()) {
            BinaryMetadata metadata = bCtxFrom.metadata0(type.typeId());

            if (metadata != null)
                transferEntity(dest, transformer, metadata);
        }
    }

    /** {@inheritDoc} */
    @Override protected String entityDescription(BinaryMetadata metadata) {
        return "binary metadata [name='" + metadata.typeName() + "', typeId=" + metadata.typeId() + "]";
    }
}
