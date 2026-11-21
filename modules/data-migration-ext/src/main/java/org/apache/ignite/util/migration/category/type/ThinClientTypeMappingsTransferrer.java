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

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.thin.ClientBinary;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractCategoryTransferrer;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/** Transfers Java mappings discoverable from a thin-client source. */
public class ThinClientTypeMappingsTransferrer extends AbstractCategoryTransferrer<IgniteClient, MigrationTypeMapping> {
    /**
     * @param from Connected thin-client source.
     * @param log Logger.
     */
    public ThinClientTypeMappingsTransferrer(IgniteClient from, IgniteLogger log) {
        super(from, MigrationCategory.TYPE_MAPPING, MigrationTypeMapping.class, log);
    }

    /** {@inheritDoc} */
    @Override protected void doTransfer(
        MigrationDestination dest,
        @Nullable MigrationTransformer<MigrationTypeMapping> transformer
    ) {
        Object binary0 = src.binary();

        if (!(binary0 instanceof ClientBinary cliBinary))
            throw new IgniteException("Thin source does not expose its type mappings handler.");

        ClassLoader ldr = Thread.currentThread().getContextClassLoader();

        if (ldr == null)
            ldr = getClass().getClassLoader();

        for (BinaryType type : cliBinary.types()) {
            String typeName = type.typeName();

            try {
                Class<?> cls = Class.forName(typeName, false, ldr);

                transferEntity(dest, transformer, new MigrationTypeMapping(JAVA_ID, type.typeId(), cls.getName()));
            }
            catch (ClassNotFoundException ignored) {
                warn("Thin source does not have a Java class mapping for binary type; skipping it " +
                    "[typeName=" + typeName + ", typeId=" + type.typeId() + "].");
            }
        }

    }

    /** {@inheritDoc} */
    @Override protected String entityDescription(MigrationTypeMapping mapping) {
        return "type mapping [name='" + mapping.typeName() + "', typeId=" + mapping.typeId() +
            ", platformId=" + mapping.platformId() + "]";
    }
}
