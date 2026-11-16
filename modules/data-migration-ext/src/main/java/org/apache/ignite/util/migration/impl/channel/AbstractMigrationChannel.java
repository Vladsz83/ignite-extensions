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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationChannel;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationSource;

/**
 * Common endpoints, configuration, transformations and logging for migration channels.
 *
 * @param <T> Transferred entity type.
 */
public abstract class AbstractMigrationChannel<T> implements MigrationChannel<T> {
    /** */
    protected final MigrationSource src;

    /** */
    protected final MigrationDestination dest;

    /** */
    protected final IgniteToIgniteMigrationConfiguration cfg;

    /** */
    protected final IgniteLogger log;

    /**
     * @param src Connected source.
     * @param dest Connected destination.
     * @param cfg Migration configuration.
     */
    protected AbstractMigrationChannel(
        MigrationSource src,
        MigrationDestination dest,
        IgniteToIgniteMigrationConfiguration cfg
    ) {
        this.src = src;
        this.dest = dest;
        this.cfg = cfg;
        log = cfg.getLogger();
    }

    /** {@inheritDoc} */
    @Override public final void transfer() throws IgniteCheckedException {
        long start = System.currentTimeMillis();

        info("Starting migration channel: " + getClass().getSimpleName());

        transfer0();

        info("Finished migration channel [channel=" + getClass().getSimpleName() + ", elapsedMs=" +
            (System.currentTimeMillis() - start) + ']');
    }

    /** Transfers this channel's entities. */
    protected abstract void transfer0() throws IgniteCheckedException;

    /** */
    protected T transform(T item) {
        return cfg.transform(this, item);
    }

    /** */
    protected void info(String msg) {
        if (log.isInfoEnabled())
            log.info(msg);
    }
}
