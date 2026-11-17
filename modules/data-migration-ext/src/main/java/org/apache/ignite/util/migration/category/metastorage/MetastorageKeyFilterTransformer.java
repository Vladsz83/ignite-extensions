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

package org.apache.ignite.util.migration.category.metastorage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.util.migration.MigrationTransformer;
import org.jetbrains.annotations.Nullable;

/**
 * Skips metastorage entries whose key matches a configured name or starts with a configured prefix.
 * Matching is case-insensitive.
 */
public final class MetastorageKeyFilterTransformer implements MigrationTransformer<Map.Entry<String, Serializable>> {
    /** */
    private final Set<String> keys;

    /** */
    private final List<String> keyPrefixes;

    /**
     * @param keys Keys that skip an entry on exact match, case-insensitive.
     * @param keyPrefixes Key prefixes that skip an entry when the key starts with them, case-insensitive.
     */
    public MetastorageKeyFilterTransformer(Collection<String> keys, Collection<String> keyPrefixes) {
        Set<String> loweredKeys = new HashSet<>();

        for (String key : keys)
            loweredKeys.add(key.toLowerCase(Locale.ROOT));

        this.keys = Collections.unmodifiableSet(loweredKeys);

        List<String> loweredPrefixes = new ArrayList<>();

        for (String prefix : keyPrefixes)
            loweredPrefixes.add(prefix.toLowerCase(Locale.ROOT));

        this.keyPrefixes = Collections.unmodifiableList(loweredPrefixes);
    }

    /** {@inheritDoc} */
    @Override @Nullable public Map.Entry<String, Serializable> transform(Map.Entry<String, Serializable> entry) {
        String key = entry.getKey().toLowerCase(Locale.ROOT);

        if (keys.contains(key))
            return null;

        for (String prefix : keyPrefixes) {
            if (key.startsWith(prefix))
                return null;
        }

        return entry;
    }
}
