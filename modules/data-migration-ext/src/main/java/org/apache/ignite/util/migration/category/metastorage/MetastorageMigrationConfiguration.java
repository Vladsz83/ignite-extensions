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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Metastorage migration settings. */
public class MetastorageMigrationConfiguration {
    /** */
    private boolean ignoreDefaultFilter;

    /** */
    private final List<MetastorageKeyFilterTransformer> filters = new ArrayList<>();

    /** @return Whether the built-in default key filter is disabled. */
    public boolean ignoreDefaultFilter() {
        return ignoreDefaultFilter;
    }

    /**
     * @param val Whether to skip the built-in default key filter, which otherwise always excludes
     *     entries that are not portable to another cluster.
     * @return {@code this} for chaining.
     */
    public MetastorageMigrationConfiguration ignoreDefaultFilter(boolean val) {
        ignoreDefaultFilter = val;

        return this;
    }

    /**
     * Adds a key filter that skips entries by key name or prefix, applied after the default filter
     * and before a channel transformer.
     *
     * @param filter Key filter to apply.
     * @return {@code this} for chaining.
     */
    public MetastorageMigrationConfiguration filter(MetastorageKeyFilterTransformer filter) {
        filters.add(Objects.requireNonNull(filter));

        return this;
    }

    /** @return Configured key filters, in the order they were added. */
    public List<MetastorageKeyFilterTransformer> filters() {
        return Collections.unmodifiableList(filters);
    }

    /** @return Whether the built-in default key filter is disabled. */
    public boolean isIgnoreDefaultFilter() {
        return ignoreDefaultFilter;
    }

    /** @param val Whether to skip the built-in default key filter. */
    public void setIgnoreDefaultFilter(boolean val) {
        ignoreDefaultFilter = val;
    }

    /** @return Configured key filters. */
    public List<MetastorageKeyFilterTransformer> getFilters() {
        return filters;
    }

    /** @param filters Configured key filters, replacing any previously set filters. */
    public void setFilters(List<MetastorageKeyFilterTransformer> filters) {
        this.filters.clear();

        for (MetastorageKeyFilterTransformer f : filters)
            this.filters.add(Objects.requireNonNull(f));
    }
}
