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

import java.io.Serializable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.util.migration.category.metastorage.MetastorageKeyFilterTransformer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Entries are skipped by exact key match or key prefix; other entries pass through unchanged. */
public class MetastorageKeyFilterTransformerTest {
    /** Exact key match skips the entry. */
    @Test
    public void testExactKey() {
        MetastorageKeyFilterTransformer filter = new MetastorageKeyFilterTransformer(
            Collections.singleton("ignored.key"),
            Collections.<String>emptyList());

        assertNull(filter.transform(entry("ignored.key", "value")));
        assertEquals(entry("kept.key", "value"), filter.transform(entry("kept.key", "value")));
    }

    /** A key that starts with a configured prefix is skipped. */
    @Test
    public void testKeyPrefix() {
        MetastorageKeyFilterTransformer filter = new MetastorageKeyFilterTransformer(
            Collections.<String>emptyList(),
            Collections.singleton("secret."));

        assertNull(filter.transform(entry("secret.password", "value")));
        assertEquals(entry("secrets", "value"), filter.transform(entry("secrets", "value")));
    }

    /** Matching is case-insensitive for both exact keys and prefixes. */
    @Test
    public void testCaseInsensitive() {
        MetastorageKeyFilterTransformer filter = new MetastorageKeyFilterTransformer(
            Collections.singleton("Ignored.KEY"),
            Collections.singleton("Secret."));

        assertNull(filter.transform(entry("IGNORED.key", "value")));
        assertNull(filter.transform(entry("Secret.Password", "value")));
        assertEquals(entry("kept", "value"), filter.transform(entry("kept", "value")));
    }

    /** */
    private static Map.Entry<String, Serializable> entry(String key, String value) {
        return new SimpleImmutableEntry<>(key, value);
    }
}
