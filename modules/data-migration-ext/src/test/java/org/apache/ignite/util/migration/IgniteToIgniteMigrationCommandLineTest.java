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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.apache.ignite.util.migration.IgniteToIgniteMigrationCommandLine.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** CLI validation must happen before reading XML or connecting to either cluster. */
public class IgniteToIgniteMigrationCommandLineTest {
    /** Invalid combinations and values are rejected before accessing the configuration paths. */
    @Test
    public void testInvalidOptions() {
        assertThrows(IllegalArgumentException.class, () -> parse());
        assertThrows(IllegalArgumentException.class, () -> parse("--src-client-node-config", "src.xml"));

        invalid("batch-size must be positive", "--batch-size", "0");
        invalid("read-threads must be positive", "--read-threads", "-1");
        invalid("queue-capacity must be positive", "--queue-capacity", "0");
        invalid("Duplicate option", "--batch-size", "1", "--batch-size", "2");
        invalid("Missing value", "--batch-size");
        invalid("mutually exclusive", "--no-cache-create", "--create-cache-if-not-exists");
        invalid("Expected true or false", "--move-data-structures", "yes");
        invalid("Transformer parameters require", "--cache-config-transformer-parameters", "backups=0");
        invalid("Unknown or unsupported option", "--unknown");
        invalid("require --use-data-streamer", "--allow-overwrite");
        invalid("Invalid DataStreamer setting", "--use-data-streamer", "--per-node-buffer-size", "0");
    }

    /** Help succeeds without any connection parameters. */
    @Test
    public void testHelp() {
        assertEquals(0, IgniteToIgniteMigrationCommandLine.run("--help"));
    }

    /** */
    private static void invalid(String msg, String... args) {
        List<String> all = new ArrayList<>(Arrays.asList("--src-client-node-config", "src.xml",
            "--dest-client-node-config", "dest.xml"));

        all.addAll(Arrays.asList(args));

        IllegalArgumentException err = assertThrows(IllegalArgumentException.class,
            () -> parse(all.toArray(new String[0])));

        assertTrue(err.getMessage(), err.getMessage().contains(msg));
    }
}
