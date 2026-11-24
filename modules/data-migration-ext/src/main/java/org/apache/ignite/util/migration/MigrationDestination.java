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

import java.util.List;

/** Destination side of a migration. Receiver operations are associated with {@link MigrationCategory}. */
public interface MigrationDestination<T> extends MigrationOrigin<T> {
    /**
     * Accepts one transferred entity.
     *
     * @param category Data category.
     * @param entity Entity to accept.
     */
    void accept(MigrationCategory category, Object entity);

    /**
     * Accepts a batch of transferred entities. The default implementation forwards each entity to
     * {@link #accept(MigrationCategory, Object)}.
     *
     * @param category Data category.
     * @param entities Entities to accept.
     */
    default void acceptAll(MigrationCategory category, List<Object> entities) {
        for (Object entity : entities)
            accept(category, entity);
    }
}
