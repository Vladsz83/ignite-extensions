/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util.migration.category.cache.configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.util.migration.MigrationCategory;
import org.apache.ignite.util.migration.MigrationDestination;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests forwarding of cache configurations to the destination receiver. */
public class ThinClientCacheConfigurationsTransferrerTest {
    /** A transferrer uses the supplied destination without connecting it. */
    @Test public void testAcceptsPreparedCache() {
        CacheConfigurationMigration cache = new CacheConfigurationMigration("src", new CacheConfiguration<>("dest"), false, CacheType.USER);
        MigrationDestination dest = new MigrationDestination() {
            @Override public void accept(MigrationCategory category, Object entity) {
                assertEquals(MigrationCategory.CACHE_CONFIGURATION, category);
                assertEquals("src", ((CacheConfigurationMigration)entity).sourceName());
                assertEquals("dest", ((CacheConfigurationMigration)entity).configuration().getName());
            }

            @Override public LinkedHashMap<MigrationCategory, Class<?>> supportedCategories() {
                return new LinkedHashMap<>();
            }

            @Override public Object connect() {
                throw new AssertionError("Transferrer must not connect the destination.");
            }

            @Override public void close() {
                // No-op.
            }
        };

        AbstractCacheConfigurationsTransferrer<Object> transferrer =
            new AbstractCacheConfigurationsTransferrer<Object>(new Object(), new CacheMigrationConfiguration(), new JavaLogger()) {
                @Override protected Collection<CacheConfigurationMigration> sourceCaches() {
                    return Collections.singletonList(cache);
                }
            };

        transferrer.prepareCaches(null, false, item -> item);
        transferrer.transfer(dest, null);
    }
}
