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

import org.apache.ignite.IgniteCheckedException;
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
    /**
     * @param from Connected thin-client source.
     * @param log Migration logger.
     */
    public ThinClientBinaryMetadataTransferrer(IgniteClient from, IgniteLogger log) {
        super(from, MigrationCategory.BINARY_METADATA, BinaryMetadata.class, log);
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(MigrationDestination dest, @Nullable MigrationTransformer<BinaryMetadata> transformer)
        throws IgniteCheckedException {
        ClientBinary binary = clientBinary();

        for (BinaryType type : binary.types()) {
            BinaryMetadata metadata = binary.binaryContext().metadata0(type.typeId());

            if (metadata != null)
                transferEntity(dest, transformer, metadata);
        }
    }

    /** Gets the thin client's binary handler. */
    private ClientBinary clientBinary() throws IgniteCheckedException {
        Object binary = from.binary();

        if (!(binary instanceof ClientBinary))
            throw new IgniteCheckedException("Thin source does not expose its binary metadata handler.");

        return (ClientBinary)binary;
    }

    /** {@inheritDoc} */
    @Override protected String entityDescription(BinaryMetadata metadata) {
        return "binary metadata [name='" + metadata.typeName() + "', typeId=" + metadata.typeId() + "]";
    }
}
