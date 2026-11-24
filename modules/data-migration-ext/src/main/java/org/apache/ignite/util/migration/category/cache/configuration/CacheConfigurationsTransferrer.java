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

package org.apache.ignite.util.migration.category.cache.configuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;

/** Transfers cache configurations to a thick-client destination. */
public class CacheConfigurationsTransferrer extends AbstractCacheConfigurationsTransferrer<IgniteEx> {
    /**
     * @param src Connected source node.
     * @param cfg Cache migration settings.
     * @param log Logger.
     */
    public CacheConfigurationsTransferrer(IgniteEx src, CacheMigrationConfiguration cfg, IgniteLogger log) {
        super(src, cfg, log);
    }

    /** {@inheritDoc} */
    @Override protected List<CacheConfigurationMigrationEntity> sourceCaches() {
        List<CacheConfigurationMigrationEntity> res = new ArrayList<>();

        for (DynamicCacheDescriptor desc : src.context().cache().cacheDescriptors().values()) {
            if (desc.cacheType() != CacheType.USER && desc.cacheType() != CacheType.DATA_STRUCTURES)
                continue;

            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(desc.cacheConfiguration());

            cacheCfg.clearQueryEntities();
            cacheCfg.setQueryEntities(desc.schema().entities());

            res.add(new CacheConfigurationMigrationEntity(cacheCfg.getName(), cacheCfg, desc.sql(), desc.cacheType(), false));
        }

        res.sort(Comparator.comparing(CacheConfigurationMigrationEntity::sourceName));

        return res;
    }
}
