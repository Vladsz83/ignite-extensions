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

import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Transfers one kind of entity between connected endpoints. Channel instances belong to a single migration.
 *
 * @param <T> Type of entities to transfer.
 */
public interface MigrationChannel<T> {
    /**
     * Transfers entities.
     *
     * @param pool Shared executor the channel may draw worker threads from.
     * @param transformer Item transformer, or null to transfer items unchanged.
     */
    void transfer(ExecutorService pool, @Nullable MigrationTransformer<T> transformer) throws IgniteCheckedException;
}
