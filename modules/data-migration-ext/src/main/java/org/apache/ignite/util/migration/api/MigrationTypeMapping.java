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

/** Platform-specific type mapping; the numeric ID must not be regenerated at the destination. */
public class MigrationTypeMapping {
    /** */
    private final byte platformId;

    /** */
    private final int typeId;

    /** */
    private final String name;

    /** Creates a mapping. */
    public MigrationTypeMapping(byte platformId, int typeId, String name) {
        this.platformId = platformId;
        this.typeId = typeId;
        this.name = name;
    }

    /** @return Platform ID. */
    public byte platformId() {
        return platformId;
    }

    /** @return Type ID. */
    public int typeId() {
        return typeId;
    }

    /** @return Class name. */
    public String typeName() {
        return name;
    }
}
