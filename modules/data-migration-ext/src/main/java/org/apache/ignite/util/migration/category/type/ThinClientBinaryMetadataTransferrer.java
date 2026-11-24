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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.client.thin.ClientBinary;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.jetbrains.annotations.Nullable;

/** Transfers binary metadata available to a thin-client source. */
public class ThinClientBinaryMetadataTransferrer extends AbstractCategoryTransferrer<IgniteClient, BinaryMetadata> {
    /** Binary type IDs accepted at the destination by this transfer. */
    private final Set<Integer> transferredTypeIds = ConcurrentHashMap.newKeySet();

    /**
     * @param from Connected thin-client source.
     * @param log Logger.
     */
    public ThinClientBinaryMetadataTransferrer(IgniteClient from, IgniteLogger log) {
        super(from, MigrationCategory.BINARY_METADATA, BinaryMetadata.class, log);
    }

    /** @return Binary type IDs accepted at the destination by this transfer. */
    public Set<Integer> transferredTypeIds() {
        return transferredTypeIds;
    }

    /** {@inheritDoc} */
    @Override protected void onEntityAccepted(MigrationDestination<?> dest, BinaryMetadata entity) {
        transferredTypeIds.add(entity.typeId());
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(
        MigrationDestination<?> dest,
        @Nullable MigrationTransformer<BinaryMetadata> transformer
    ) {
        Object binary0 = src.binary();

        if (!(binary0 instanceof ClientBinary cliBinary))
            throw new IgniteException("Thin source does not expose its binary metadata handler.");

        for (BinaryType type : cliBinary.types()) {
            BinaryMetadata metadata = cliBinary.binaryContext().metadata0(type.typeId());

            if (metadata != null)
                transferEntity(dest, transformer, metadata);
        }
    }

    /** {@inheritDoc} */
    @Override protected String entityDescription(BinaryMetadata metadata) {
        return "binary metadata [name='" + metadata.typeName() + "', typeId=" + metadata.typeId() + "]";
    }
}
