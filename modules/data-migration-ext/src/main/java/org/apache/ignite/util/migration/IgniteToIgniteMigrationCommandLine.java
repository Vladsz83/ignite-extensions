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

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.startup.cmdline.CommandLineStartup.isHelp;

/** */
public class IgniteToIgniteMigrationCommandLine {
    /**
     * Main entry point.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (!IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_QUIET)) {
            X.println("Apache Ignite Data Migration Command Line Startup, ver. " + VER);
            X.println(COPYRIGHT);
            X.println();
        }

        // TODO
        if (args.length > 1)
            exit("Too many arguments.", true, -1);

        if (args.length > 0 && isHelp(args[0]))
            exit(null, true, 0);

        if (args.length > 0 && args[0].isEmpty())
            exit("Empty argument.", true, 1);

        // TODO
        if (args.length > 0 && args[0].charAt(0) == '-')
            exit("Invalid arguments: " + args[0], true, -1);

        try {
//            IgniteToIgniteMigration ldr = IgniteToIgniteMigration.startWith(args[0]);
//
//            ldr.run();
        }
        catch (Throwable e) {
            e.printStackTrace();

            String note = "";

            if (X.hasCause(e, ClassNotFoundException.class))
                note = "\nNote! You may use 'USER_LIBS' environment variable to specify your classpath.";

            exit("Failed to run app: " + e.getMessage() + note, false, -1);
        }
    }

    /** */
    private static void exit(@Nullable String errMsg, boolean showUsage, int exitCode) {
        if (errMsg != null)
            X.error(errMsg);

        // TODO
        if (showUsage) {
            X.error(
                "Usage:",
                "    ignite-to-ignite.{sh|bat} [?]|[path]",
                "    Where:",
                "    ?, /help, -help, - show this message.",
                "    -v               - verbose mode (quiet by default).",
                "    path             - path to Spring XML configuration file.",
                "                       Path can be absolute or relative to IGNITE_HOME.",
                " ",
                "Spring file should contain the following bean definition:",
                "1. 'org.apache.ignite.configuration.IgniteConfiguration' or " +
                    "'org.apache.ignite.configuration.ClientConfiguration' to connect to Apache Ignite cluster.",
                "2. 'org.apache.ignite.cdc.KafkaToIgniteCdcStreamerConfiguration' to configure streamer.");
        }

        System.exit(exitCode);
    }
}
