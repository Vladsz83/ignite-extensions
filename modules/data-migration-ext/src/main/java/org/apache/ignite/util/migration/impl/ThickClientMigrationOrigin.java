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

package org.apache.ignite.util.migration.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.util.migration.api.MigrationOrigin;
import org.jetbrains.annotations.Nullable;

/** */
public class ThickClientMigrationOrigin implements MigrationOrigin {
    /** */
    private static final String ERR_TPL = "Failed to %s the thin client [err='%s'].";

    /** */
    private final IgniteConfiguration cfg;

    /** */
    private Ignite ig;

    /** */
    private boolean closed;

    /** */
    private ThickClientMigrationOrigin(IgniteConfiguration cfg) {
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public synchronized void connect() throws IgniteCheckedException {
        if (closed)
            throw err("start", "already closed", null);

        try {
            ig = Ignition.start(cfg);
        } catch (Throwable t){
            throw err("start", X.cause(t, null).getMessage(), t);
        }
    }

    /** */
    public static ThickClientMigrationOrigin create(IgniteConfiguration cfg) throws IgniteCheckedException {
        if (!cfg.isClientMode())
            throw err("create", "the configuration is not a client", null);

        return new ThickClientMigrationOrigin(cfg);
    }

    /** */
    private static IgniteCheckedException err(String action, String reason, @Nullable Throwable cause) {
        return new IgniteCheckedException(ERR_TPL.formatted(action, reason), cause);
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
