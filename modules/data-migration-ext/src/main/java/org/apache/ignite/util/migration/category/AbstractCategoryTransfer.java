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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.util.migration.CategoryTransferrer;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.jetbrains.annotations.Nullable;

/** Common logging support for direct-transfer category implementations. */
public abstract class AbstractCategoryTransfer<E> implements CategoryTransferrer<E> {
    /** */
    protected final IgniteLogger log;

    /**
     * @param log Migration logger.
     */
    protected AbstractCategoryTransfer(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public final void transfer(MigrationDestination dest, @Nullable MigrationTransformer<E> transformer) {
        long start = System.nanoTime();

        info("Starting category transfer [category=" + category() + ", transferrer=" +
            getClass().getSimpleName() + "].");

        try {
            doTransfer(dest, transformer);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to transfer category [category=" + category() + ", error=" +
                e.getMessage() + "].", e);
        }

        info("Finished category transfer [category=" + category() + ", transferrer=" + getClass().getSimpleName() +
            ", elapsedMs=" +
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "].");
    }

    /**
     * Transfers this transferrer's entities using its bound direct-write implementation.
     *
     * @param dest Destination endpoint.
     * @param transformer Item transformer, or null to transfer items unchanged.
     */
    protected abstract void doTransfer(MigrationDestination dest, @Nullable MigrationTransformer<E> transformer)
        throws IgniteCheckedException;

    /** */
    protected void info(String msg) {
        if (log.isInfoEnabled())
            log.info(msg);
    }

    /** */
    protected void warn(String msg) {
        log.warning(msg);
    }
}
