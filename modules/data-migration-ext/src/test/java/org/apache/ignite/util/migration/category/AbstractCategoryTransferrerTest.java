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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** The common category transferrer performs source reads, transformation, and destination writes synchronously. */
public class AbstractCategoryTransferrerTest {
    /** Items are transformed and accepted on the transfer caller's thread without an intermediate queue. */
    @Test public void testDirectTransferUsesSingleThread() {
        Thread transferThread = Thread.currentThread();
        AtomicReference<Thread> srcThread = new AtomicReference<>();
        AtomicReference<Thread> transformerThread = new AtomicReference<>();
        AtomicReference<Thread> dstThread = new AtomicReference<>();
        TestTransferrer transferrer = new TestTransferrer(srcThread);
        TestDestination dest = new TestDestination(dstThread);
        MigrationTransformer<String> transformer = item -> {
            transformerThread.set(Thread.currentThread());

            return item.toUpperCase();
        };

        transferrer.transfer(dest, transformer);

        assertEquals(Arrays.asList("FIRST", "SECOND"), dest.accepted);
        assertSame(transferThread, srcThread.get());
        assertSame(transferThread, transformerThread.get());
        assertSame(transferThread, dstThread.get());
    }

    /** Minimal direct category transferrer used to verify synchronous execution. */
    private static class TestTransferrer extends AbstractCategoryTransferrer<Object, String> {
        /** Captured source read thread. */
        private final AtomicReference<Thread> sourceThread;

        /** */
        TestTransferrer(AtomicReference<Thread> sourceThread) {
            super(new Object(), MigrationCategory.OTHER, String.class, new JavaLogger());

            this.sourceThread = sourceThread;
        }

        /** {@inheritDoc} */
        @Override protected void doTransfer(MigrationDestination dest, @Nullable MigrationTransformer<String> transformer)
            throws IgniteCheckedException {
            sourceThread.set(Thread.currentThread());
            transferEntity(dest, transformer, "first");
            transferEntity(dest, transformer, "second");
        }

        /** {@inheritDoc} */
        @Override protected String entityDescription(String entity) {
            return "test item [value=" + entity + "]";
        }
    }

    /** Minimal category destination used to verify synchronous execution. */
    private static class TestDestination implements MigrationDestination {
        /** Accepted values. */
        private final List<String> accepted = new ArrayList<>();

        /** Destination write thread. */
        private final AtomicReference<Thread> writeThread;

        /** */
        TestDestination(AtomicReference<Thread> writeThread) {
            this.writeThread = writeThread;
        }

        /** {@inheritDoc} */
        @Override public LinkedHashMap<MigrationCategory, Class<?>> supportedCategories() {
            LinkedHashMap<MigrationCategory, Class<?>> categories = new LinkedHashMap<>();
            categories.put(MigrationCategory.OTHER, String.class);

            return categories;
        }

        /** {@inheritDoc} */
        @Override public void accept(MigrationCategory category, Object data) {
            writeThread.set(Thread.currentThread());
            accepted.add((String)data);
        }

        /** {@inheritDoc} */
        @Override public Object connect() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No connection required in this test.
        }
    }
}
