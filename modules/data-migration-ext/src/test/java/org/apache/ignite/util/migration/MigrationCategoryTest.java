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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Stable category IDs are unique, ordered and do not depend on enum ordinals. */
public class MigrationCategoryTest {
    /** Existing IDs are fixed; future categories must be appended with larger values. */
    @Test public void testStableIds() {
        MigrationCategory[] categories = MigrationCategory.values();
        int[] ids = {0, 1, 2, 3, 4, 5};

        assertEquals(ids.length, categories.length);

        for (int i = 0; i < ids.length; i++) {
            assertEquals(ids[i], categories[i].id());

            if (i > 0)
                assertTrue(categories[i].id() > categories[i - 1].id());
        }
    }
}
