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

/** A source partition and its destination cache. */
public class MigrationPartition {
    /** */
    private final String srcCache;

    /** */
    private final String destCache;

    /** */
    private final int part;

    /**
     * @param srcCache Source cache name.
     * @param destCache Destination cache name.
     * @param part Partition number.
     */
    public MigrationPartition(String srcCache, String destCache, int part) {
        this.srcCache = srcCache;
        this.destCache = destCache;
        this.part = part;
    }

    /** @return Source cache name. */
    public String sourceCache() {
        return srcCache;
    }

    /** @return Destination cache name. */
    public String destinationCache() {
        return destCache;
    }

    /** @return Source partition number. */
    public int partition() {
        return part;
    }
}
