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

package org.apache.ignite.util.migration.category;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.util.migration.CategoryTransferrer;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.jetbrains.annotations.Nullable;

/**
 * Common source, category metadata, and transfer logging for category transferrers.
 *
 * @param <SRC> Type of the connection used to read from the source cluster.
 * @param <ENTITY> Type of a single item transferred in this migration category.
 */
public abstract class AbstractCategoryTransferrer<SRC, ENTITY> implements CategoryTransferrer<ENTITY> {
    /** Connected source. */
    protected final SRC src;

    /** Category transferred by this implementation. */
    private final MigrationCategory category;

    /** Runtime class of the transferred entities. */
    private final Class<? super ENTITY> dataCls;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * @param src Connected source.
     * @param category Category transferred by this implementation.
     * @param dataCls Runtime class of the transferred entities.
     * @param log Logger.
     */
    protected AbstractCategoryTransferrer(
        SRC src,
        MigrationCategory category,
        Class<? super ENTITY> dataCls,
        IgniteLogger log
    ) {
        this.src = src;
        this.category = category;
        this.dataCls = dataCls;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public final MigrationCategory category() {
        return category;
    }

    /** {@inheritDoc} */
    @Override public final Class<? super ENTITY> dataClass() {
        return dataCls;
    }

    /** {@inheritDoc} */
    @Override public final void transfer(MigrationDestination dest, @Nullable MigrationTransformer<ENTITY> transformer) {
        long start = System.nanoTime();

        info("Starting category transfer [category=" + category() + ", transferrer=" +
            getClass().getSimpleName() + "].");

        doTransfer(dest, transformer);

        info("Finished category transfer [category=" + category() + ", transferrer=" + getClass().getSimpleName() +
            ", elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "].");
    }

    /** Performs the transfer implementation for this category. */
    protected abstract void doTransfer(MigrationDestination dest, @Nullable MigrationTransformer<ENTITY> transformer);

    /** Applies the transformer and writes one entity directly to the destination. */
    protected final void transferEntity(
        MigrationDestination dest,
        @Nullable MigrationTransformer<ENTITY> transformer, ENTITY entity
    ) {
        if (entity == null)
            return;

        ENTITY transformed = transformer == null ? entity : transformer.transform(entity);

        if (transformed == null) {
            info("Skipped " + entityDescription(entity) + " [reason='filtered out'].");

            return;
        }

        dest.accept(category(), transformed);

        info("Transferred " + entityDescription(transformed) + '.');
    }

    /** Returns the category-specific description for an entity transfer log. */
    protected String entityDescription(ENTITY entity) {
        return dataCls.getSimpleName();
    }

    /** Logs an informational message if enabled. */
    protected void info(String msg) {
        if (log.isInfoEnabled())
            log.info(msg);
    }

    /** Logs a warning. */
    protected void warn(String msg) {
        log.warning(msg);
    }
}
