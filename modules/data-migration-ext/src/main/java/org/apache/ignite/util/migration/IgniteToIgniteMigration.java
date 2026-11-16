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

package org.apache.ignite.util.migration;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.util.migration.api.IgniteToIgniteMigrationConfiguration;
import org.apache.ignite.util.migration.api.MigrationChannel;
import org.apache.ignite.util.migration.api.MigrationDestination;
import org.apache.ignite.util.migration.api.MigrationSource;
import org.apache.ignite.util.migration.impl.BinaryMigrationChannel;
import org.apache.ignite.util.migration.impl.ClientMigrationDestination;
import org.apache.ignite.util.migration.impl.ClientMigrationSource;

/** */
public class IgniteToIgniteMigration {
    /** */
    private final IgniteToIgniteMigrationConfiguration mCfg;

    /** */
    public IgniteToIgniteMigration(IgniteToIgniteMigrationConfiguration cfg) {
        mCfg = cfg;
    }

    /** */
    public void start() throws IgniteCheckedException {
        // TODO
        validate(mCfg);

        MigrationSource src = createSrc(mCfg);
        MigrationDestination dest = createDest(mCfg);

        Collection<MigrationChannel> channels = createMigrationChannels();

        try (src) {
            src.connect();

            try (dest) {
                dest.connect();

                for(MigrationChannel channel : channels)
                    channel.migrate(src, dest);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to start destination cluster connection [error='" + e.getMessage() + "']", e);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to start source cluster connection [error='" + e.getMessage() + "']", e);
        }
    }

    /** */
    private Collection<MigrationChannel> createMigrationChannels() {
        return Collections.singleton(new BinaryMigrationChannel());
    }

    /** */
    private static MigrationDestination createDest(IgniteToIgniteMigrationConfiguration cfg) {
        if (cfg.destinationClientConfiguration() != null)
            return new ClientMigrationDestination(cfg.destinationClientConfiguration());

        throw new IgniteException("Failed to create destination client [error='no configuration provided'].");
    }

    /** */
    private static MigrationSource createSrc(IgniteToIgniteMigrationConfiguration cfg) {
        if (cfg.sourceClientConfiguration() != null)
            return new ClientMigrationSource(cfg.sourceClientConfiguration());

        throw new IgniteException("Failed to create destination client [error='no configuration provided'].");
    }

    /** */
    private static void validate(IgniteToIgniteMigrationConfiguration cfg) throws IgniteCheckedException {
        // TODO
    }
}
