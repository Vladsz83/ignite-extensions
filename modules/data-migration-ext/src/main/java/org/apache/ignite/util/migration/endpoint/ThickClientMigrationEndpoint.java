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

package org.apache.ignite.util.migration.endpoint;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.util.migration.MigrationOrigin;

/** Thick client (in-JVM) migration endpoint. */
public class ThickClientMigrationEndpoint implements MigrationOrigin<IgniteEx> {
    /** */
    private final IgniteConfiguration cfg;

    /** */
    private IgniteEx ig;

    /** */
    private boolean closed;

    /** */
    private ThickClientMigrationEndpoint(IgniteConfiguration cfg) {
        this.cfg = new IgniteConfiguration(cfg);
    }

    /**
     * @param cfg Client node configuration.
     * @return Migration endpoint.
     */
    public static ThickClientMigrationEndpoint create(IgniteConfiguration cfg) throws IgniteCheckedException {
        if (!Boolean.TRUE.equals(cfg.isClientMode()))
            throw new IgniteCheckedException("Migration requires a client-mode in IgniteConfiguration.");

        return new ThickClientMigrationEndpoint(cfg);
    }

    /** {@inheritDoc} */
    @Override public IgniteEx origin() {
        return ig;
    }

    /** {@inheritDoc} */
    @Override public synchronized void connect() throws IgniteCheckedException {
        if (closed)
            throw new IgniteCheckedException("Migration client node is already closed.");

        if (ig != null)
            throw new IgniteCheckedException("Migration client node is already connected.");

        try {
            ig = (IgniteEx)Ignition.start(cfg);

            if (!ig.cluster().state().active())
                throw new IgniteCheckedException("Migration requires an active cluster.");
        }
        catch (RuntimeException e) {
            throw new IgniteCheckedException("Failed to start migration client node.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        if (!closed) {
            closed = true;

            if (ig != null)
                ig.close();
        }
    }
}
