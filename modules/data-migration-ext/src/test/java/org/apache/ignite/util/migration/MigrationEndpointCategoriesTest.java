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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.util.migration.category.cache.configuration.MigrationCache;
import org.apache.ignite.util.migration.category.cache.data.MigrationEntry;
import org.apache.ignite.util.migration.category.type.MigrationTypeMapping;
import org.apache.ignite.util.migration.endpoint.ThickClientMigrationEndpoint;
import org.apache.ignite.util.migration.endpoint.ThinClientMigrationEndpoint;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Built-in endpoints report their supported categories in a deterministic priority order. */
public class MigrationEndpointCategoriesTest {
    /** Thick endpoints support every built-in transfer category in dependency order. */
    @Test public void testThickEndpointCategories() throws Exception {
        MigrationOrigin src = ThickClientMigrationEndpoint.create(new IgniteConfiguration().setClientMode(true));
        MigrationDestination dest = (MigrationDestination)src;

        LinkedHashMap<MigrationCategory, Class<?>> categories = src.supportedCategories();

        assertTrue(categories instanceof LinkedHashMap);
        assertEquals(Arrays.asList(MigrationCategory.BINARY_METADATA, MigrationCategory.TYPE_MAPPING,
            MigrationCategory.CACHE_CONFIGURATION, MigrationCategory.METASTORAGE_ENTRY, MigrationCategory.CACHE_ENTRY),
            new ArrayList<>(categories.keySet()));
        assertEquals(BinaryMetadata.class, categories.get(MigrationCategory.BINARY_METADATA));
        assertEquals(MigrationTypeMapping.class, categories.get(MigrationCategory.TYPE_MAPPING));
        assertEquals(MigrationCache.class, categories.get(MigrationCategory.CACHE_CONFIGURATION));
        assertEquals(java.util.Map.Entry.class, categories.get(MigrationCategory.METASTORAGE_ENTRY));
        assertEquals(MigrationEntry.class, categories.get(MigrationCategory.CACHE_ENTRY));
        assertEquals(src.supportedCategories(), dest.supportedCategories());
    }

    /** Thin endpoints support their local binary metadata, Java mappings, cache definitions and cache records. */
    @Test public void testThinEndpointCategories() {
        MigrationOrigin src = ThinClientMigrationEndpoint.create(new ClientConfiguration());
        MigrationDestination dest = (MigrationDestination)src;

        LinkedHashMap<MigrationCategory, Class<?>> categories = src.supportedCategories();

        assertTrue(categories instanceof LinkedHashMap);
        assertEquals(Arrays.asList(MigrationCategory.BINARY_METADATA, MigrationCategory.TYPE_MAPPING,
            MigrationCategory.CACHE_CONFIGURATION, MigrationCategory.CACHE_ENTRY),
            new ArrayList<>(categories.keySet()));
        assertEquals(BinaryMetadata.class, categories.get(MigrationCategory.BINARY_METADATA));
        assertEquals(MigrationTypeMapping.class, categories.get(MigrationCategory.TYPE_MAPPING));
        assertEquals(MigrationCache.class, categories.get(MigrationCategory.CACHE_CONFIGURATION));
        assertEquals(MigrationEntry.class, categories.get(MigrationCategory.CACHE_ENTRY));
        assertEquals(src.supportedCategories(), dest.supportedCategories());
    }
}
