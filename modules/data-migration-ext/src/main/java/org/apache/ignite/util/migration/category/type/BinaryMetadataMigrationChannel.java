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

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.util.migration.MigrationOrigin;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.apache.ignite.util.migration.category.AbstractMigrationChannel;
import org.jetbrains.annotations.Nullable;

/** Transfers the binary metadata. */
public class BinaryMetadataMigrationChannel extends AbstractMigrationChannel<IgniteEx, IgniteEx, BinaryMetadata> {
    /**
     * @param from Migration source.
     * @param to Migration destination.
     * @param log Logger.
     */
    public BinaryMetadataMigrationChannel(
        MigrationOrigin<IgniteEx> from,
        MigrationOrigin<IgniteEx> to,
        IgniteLogger log
    ) {
        super(from, to, log);
    }

    /**
     * Transfers the binary metadata.
     *
     * <p>A new destination registration is a blocking cluster round-trip, so the pool overlaps N sequential RTTs into
     * roughly N/poolSize; speedup is sub-linear because the destination processes these discovery events on a single
     * coordinator thread.
     */
    @Override protected void doTransfer(ExecutorService pool, @Nullable MigrationTransformer<BinaryMetadata> transformer)
        throws IgniteCheckedException {
        BinaryContext bCtxFrom = from.origin().context().cacheObjects().binaryContext();

        Collection<BinaryType> toTransfer = bCtxFrom.metadata();
        int total = toTransfer.size();

        CountDownLatch done = new CountDownLatch(total);
        AtomicInteger cnt = new AtomicInteger();
        AtomicReference<Throwable> error = new AtomicReference<>();

        BinaryContext bCtxTo = to.origin().context().cacheObjects().binaryContext();

        for (BinaryType bt : toTransfer) {
            if (error.get() != null) {
                done.countDown();

                continue;
            }

            int typeId = bt.typeId();

            pool.submit(() -> {
                try {
                    BinaryMetadata bMeta = bCtxFrom.metadata0(typeId);

                    if (bMeta == null) {
                        info("Skipped binary metadata [typeId=" + typeId + "', reason='no metadata on source', " +
                            "progress=" + cnt.incrementAndGet() + '/' + total + "].");

                        return;
                    }

                    String name = bMeta.typeName();

                    bMeta = transformer == null ? bMeta : transformer.transform(bMeta);

                    if (bMeta == null) {
                        info("Skipped binary metadata [name='" + name + "', reason='filtered out', " +
                            "progress=" + cnt.incrementAndGet() + '/' + total + "].");

                        return;
                    }

                    bCtxTo.updateMetadata(bMeta.typeId(), bMeta, false);

                    info("Transferred binary metadata [name='" + bMeta.typeName() + "', progress=" +
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

        try {
            done.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Migration interrupted.", e);
        }

        if (error.get() != null) {
            throw new IgniteCheckedException(
                "Failed to register binary metadata [error='" + error.get().getMessage() + "].",
                error.get()
            );
        }
    }
}
