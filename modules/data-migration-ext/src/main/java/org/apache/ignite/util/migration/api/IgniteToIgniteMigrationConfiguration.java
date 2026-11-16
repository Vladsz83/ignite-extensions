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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteToIgniteMigrationConfiguration {
    /** Configuration of the thick client to the source cluster. */
    private @Nullable IgniteConfiguration srcClientCfg;

    /** Configuration of the thick client to the destination cluster. */
    private @Nullable IgniteConfiguration destClientCfg;

    /** Creates migration configuration base on 2 clients. */
    public IgniteToIgniteMigrationConfiguration(IgniteConfiguration srcClientCfg,  IgniteConfiguration destClientCfg) {
        this.srcClientCfg = srcClientCfg;
        this.destClientCfg = destClientCfg;
    }

    /** */
    public @Nullable IgniteConfiguration sourceClientConfiguration() {
        return srcClientCfg;
    }

    /** */
    public @Nullable IgniteConfiguration destinationClientConfiguration() {
        return destClientCfg;
    }
}
