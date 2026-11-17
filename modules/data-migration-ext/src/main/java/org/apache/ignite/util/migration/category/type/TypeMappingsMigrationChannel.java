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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.util.migration.MigrationOrigin;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractMigrationChannel;
import org.jetbrains.annotations.Nullable;

/** Transfers the type mappings. */
public class TypeMappingsMigrationChannel extends AbstractMigrationChannel<IgniteEx, IgniteEx, MigrationTypeMapping> {
    /**
     * @param from Migration source.
     * @param to Migration destination.
     * @param log Logger.
     */
    public TypeMappingsMigrationChannel(
        MigrationOrigin<IgniteEx> from,
        MigrationOrigin<IgniteEx> to,
        IgniteLogger log
    ) {
        super(from, to, log);
    }

    /**
     * Transfers the type mappings.
     *
     * <p>
     * Registers each mapping as a pool task. A new destination mapping is a blocking cluster round-trip, so the pool
     * overlaps N sequential RTTs into roughly N/poolSize; speedup is sub-linear because the destination processes these
     * discovery events on a single coordinator thread.
     */
    @Override protected void doTransfer(ExecutorService pool, @Nullable MigrationTransformer<MigrationTypeMapping> transformer)
        throws IgniteCheckedException {
        List<Map<Integer, MappedName>> mappings = from.origin().context().marshallerContext().getCachedMappings();

        int total = count(mappings);

        CountDownLatch done = new CountDownLatch(total);
        AtomicInteger cnt = new AtomicInteger();
        AtomicReference<Throwable> error = new AtomicReference<>();

        BinaryContext bCtxTo = to.origin().context().cacheObjects().binaryContext();

        for (int platform = 0; platform < mappings.size(); platform++) {
            byte platform0 = (byte)platform;

            for (Map.Entry<Integer, MappedName> entry : mappings.get(platform).entrySet()) {
                if (error.get() != null) {
                    done.countDown();

                    continue;
                }

                int typeId = entry.getKey();
                String name = entry.getValue().className();

                pool.submit(() -> {
                    try {
                        MigrationTypeMapping mapping = new MigrationTypeMapping(platform0, typeId, name);

                        if (transformer != null)
                            mapping = transformer.transform(mapping);

                        if (mapping == null) {
                            info("Skipped type mapping [name='" + name + "', reason='filtered out', " +
                                "progress=" + cnt.incrementAndGet() + '/' + total + "].");

                            return;
                        }

                        bCtxTo.registerUserClassName(mapping.typeId(), mapping.typeName(), false, false,
                            mapping.platformId());

                        info("Transferred type mapping [name='" + mapping.typeName() + "', progress=" +
                            cnt.incrementAndGet() + '/' + total + "].");
                    }
                    catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                    finally {
                        done.countDown();
                    }
                });
            }
        }

        try {
            done.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Migration interrupted.", e);
        }

        if (error.get() != null) {
            throw new IgniteCheckedException(
                "Failed to register type mapping [error='" + error.get().getMessage() + "].",
                error.get()
            );
        }
    }

    /** */
    private static int count(List<Map<Integer, MappedName>> mappings) {
        int total = 0;
        for (Map<Integer, MappedName> m : mappings)
            total += m.size();

        return total;
    }
}
