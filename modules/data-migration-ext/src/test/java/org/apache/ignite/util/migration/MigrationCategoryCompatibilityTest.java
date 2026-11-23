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

import java.util.LinkedHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.logger.java.JavaLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Source/destination category maps are checked before transfer starts. */
public class MigrationCategoryCompatibilityTest {
    /** Destination iteration order is retained and compatible payload superclasses are accepted. */
    @Test public void testCompatibleTypesAndDestinationPriority() throws Exception {
        TestSource src = new TestSource();
        src.categories.put(MigrationCategory.CACHE_ENTRY, String.class);
        src.categories.put(MigrationCategory.CACHE_CONFIGURATION, Integer.class);

        TestDestination dest = new TestDestination();
        dest.categories.put(MigrationCategory.CACHE_CONFIGURATION, Number.class);
        dest.categories.put(MigrationCategory.CACHE_ENTRY, Object.class);

        LinkedHashMap<MigrationCategory, Class<?>> res =
            IgniteToIgniteMigration.checkSupportedCategories(src, dest, new JavaLogger());

        assertEquals(MigrationCategory.CACHE_CONFIGURATION, res.keySet().iterator().next());
        assertEquals(Integer.class, res.get(MigrationCategory.CACHE_CONFIGURATION));
        assertEquals(String.class, res.get(MigrationCategory.CACHE_ENTRY));
    }

    /** An agreed category with incompatible payload classes fails before transfer starts. */
    @Test public void testIncompatibleTypesFail() {
        TestSource src = new TestSource();
        src.categories.put(MigrationCategory.CACHE_ENTRY, String.class);

        TestDestination dest = new TestDestination();
        dest.categories.put(MigrationCategory.CACHE_ENTRY, Integer.class);

        IgniteCheckedException err = assertThrows(IgniteCheckedException.class, () ->
            IgniteToIgniteMigration.checkSupportedCategories(src, dest, new JavaLogger()));

        assertTrue(err.getMessage().contains("Incompatible migration category data classes"));
        assertTrue(err.getMessage().contains(String.class.getName()));
        assertTrue(err.getMessage().contains(Integer.class.getName()));
    }

    /** Endpoints with no shared data categories are rejected. */
    @Test public void testNoCommonCategoriesFail() {
        TestSource src = new TestSource();
        src.categories.put(MigrationCategory.BINARY_METADATA, String.class);

        TestDestination dest = new TestDestination();
        dest.categories.put(MigrationCategory.TYPE_MAPPING, Integer.class);

        assertThrows(IgniteCheckedException.class, () ->
            IgniteToIgniteMigration.checkSupportedCategories(src, dest, new JavaLogger()));
    }

    /** Test source endpoint. */
    private static class TestSource implements MigrationOrigin {
        /** */
        private final LinkedHashMap<MigrationCategory, Class<?>> categories = new LinkedHashMap<>();

        /** {@inheritDoc} */
        @Override public LinkedHashMap<MigrationCategory, Class<?>> supportedCategories() {
            return new LinkedHashMap<>(categories);
        }

        /** {@inheritDoc} */
        @Override public Object connect() {
            // No connection required in this test.
            return this;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No connection required in this test.
        }
    }

    /** Test destination endpoint. */
    private static class TestDestination implements MigrationDestination {
        /** */
        private final LinkedHashMap<MigrationCategory, Class<?>> categories = new LinkedHashMap<>();

        /** {@inheritDoc} */
        @Override public LinkedHashMap<MigrationCategory, Class<?>> supportedCategories() {
            return new LinkedHashMap<>(categories);
        }

        /** {@inheritDoc} */
        @Override public void accept(MigrationCategory category, Object entity) {
            // No transfer required in this test.
        }

        /** {@inheritDoc} */
        @Override public Object connect() {
            // No connection required in this test.
            return this;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No connection required in this test.
        }
    }
}
