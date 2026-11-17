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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.util.migration.MigrationChannel;
import org.apache.ignite.util.migration.MigrationOrigin;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.jetbrains.annotations.Nullable;

/**
 * Common endpoints and logging for migration channels.
 *
 * @param <FROM> Source origin type.
 * @param <TO> Destination origin type.
 * @param <E> Entity type.
 */
public abstract class AbstractMigrationChannel<FROM, TO, E> implements MigrationChannel<E> {
    /** */
    protected final MigrationOrigin<FROM> from;

    /** */
    protected final MigrationOrigin<TO> to;

    /** */
    protected final IgniteLogger log;

    /**
     * @param from Connected source endpoint.
     * @param to Connected destination endpoint.
     * @param log Migration logger.
     */
    protected AbstractMigrationChannel(
        MigrationOrigin<FROM> from,
        MigrationOrigin<TO> to,
        IgniteLogger log
    ) {
        this.from = from;
        this.to = to;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public final void transfer(
        ExecutorService pool,
        @Nullable MigrationTransformer<E> transformer
    ) throws IgniteCheckedException {
        long start = System.nanoTime();

        info("Starting migration channel: " + getClass().getSimpleName() + '.');

        doTransfer(pool, transformer);

        info("Finished migration channel [channel=" + getClass().getSimpleName() + ", elapsedMs=" +
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "].");
    }

    /**
     * Transfers this channel's entities.
     *
     * @param pool Shared executor the channel may draw worker threads from.
     * @param transformer Item transformer, or null to transfer items unchanged.
     */
    protected abstract void doTransfer(ExecutorService pool, @Nullable MigrationTransformer<E> transformer)
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
