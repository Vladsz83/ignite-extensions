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

package org.apache.ignite.util.migration.impl;

import java.util.HashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.datastructures.GridCacheCountDownLatchValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheLockState;
import org.apache.ignite.internal.processors.datastructures.GridCacheSemaphoreState;

/** Adapts volatile data structure state to the destination cluster incarnation. */
final class MigrationDataStructures {
    /** */
    private MigrationDataStructures() {
        // No-op.
    }

    /** Rebase cluster timestamps without transferring source node/thread ownership. */
    static Object value(Object val, long gridStartTime) throws IgniteCheckedException {
        if (val instanceof GridCacheCountDownLatchValue) {
            GridCacheCountDownLatchValue latch = (GridCacheCountDownLatchValue)val;
            GridCacheCountDownLatchValue res = new GridCacheCountDownLatchValue(latch.initialCount(),
                latch.autoDelete(), gridStartTime);

            res.set(latch.get());

            return res;
        }

        if (val instanceof GridCacheSemaphoreState) {
            GridCacheSemaphoreState sem = (GridCacheSemaphoreState)val;

            if (sem.getWaiters() != null && sem.getWaiters().values().stream().anyMatch(cnt -> cnt != 0))
                throw new IgniteCheckedException("Cannot migrate a semaphore with acquired permits");

            GridCacheSemaphoreState res = new GridCacheSemaphoreState(sem.getCount(), new HashMap<>(),
                sem.isFailoverSafe(), gridStartTime);

            res.setBroken(sem.isBroken());

            return res;
        }

        if (val instanceof GridCacheLockState) {
            GridCacheLockState lock = (GridCacheLockState)val;

            if (lock.get() != 0 || !lock.getNodes().isEmpty() ||
                lock.getConditionMap().values().stream().anyMatch(nodes -> !nodes.isEmpty()) ||
                lock.getSignals() != null && lock.getSignals().values().stream().anyMatch(signals -> !signals.isEmpty())) {
                throw new IgniteCheckedException("Cannot migrate a lock with an owner, waiters or pending signals");
            }

            GridCacheLockState res = new GridCacheLockState(0, null, 0, lock.isFailoverSafe(), lock.isFair(), gridStartTime);

            res.setConditionMap(new HashMap<>(lock.getConditionMap()));

            return res;
        }

        return val;
    }
}
