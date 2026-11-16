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

package org.apache.ignite.util.migration.api;

import java.util.Map;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/** Optional parameterized, destination-aware cache configuration transformer with a public no-argument constructor. */
public interface CacheConfigTransformer
    extends BiFunction<IgniteConfiguration, CacheConfiguration<?, ?>, CacheConfiguration<?, ?>> {
    /**
     * Called once before applying the transformer. The destination configuration is null for custom destinations.
     *
     * @param parameters Transformer parameters.
     */
    void configure(Map<String, String> parameters);
}
