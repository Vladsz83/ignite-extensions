<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Ignite data migration

A one-shot cluster-to-cluster migration utility. It connects to two independent
Ignite clusters using client nodes. Source and destination server counts,
consistent IDs, persistence settings and affinity configurations may differ.
Data is read directly from the source and written to the destination.

This implementation provides the client-node path and DataStreamer loading.
Thin-client connections are the next implementation stage described in the ticket plan.

## Build and install

From the extensions repository root:

```shell
mvn -B -ntp -f modules/data-migration-ext/pom.xml package -Pcheckstyle
```

Use a matching Ignite 2.20.0-SNAPSHOT installation. Copy the module JAR and its
runtime dependencies from `target/libs` to an installation library directory,
for example `$IGNITE_HOME/libs/data-migration-ext`. Copy `bin/data-migration.sh`
to `$IGNITE_HOME/bin` and the example XML files to `$IGNITE_HOME/config/migration`.
The `extension-release` Maven profile also builds a binary ZIP with this layout.

The launcher delegates to Ignite's `control.sh`, reusing its classpath and Java
module options. Set JVM settings through `CONTROL_JVM_OPTS` and additional custom
classes through `USER_LIBS`. Logging is initialized through Ignite's logger setup
and the source configuration's `gridLogger`.

On source servers, install this extension JAR or enable peer class loading on
the servers and source client. The scan transformer accesses Ignite internals,
so it must run with the corresponding permissions when security is enabled.
The `peerClassLoadingEnabled` settings in each XML must match its cluster.

## Run

Each connection XML must contain exactly one `IgniteConfiguration` bean.
The utility starts it in client mode with a unique instance name. The supplied
examples connect to two local clusters on discovery ports 47500 and 47600;
replace their discovery addresses for your environment.

```shell
bin/data-migration.sh \
  --src-client-node-config config/migration/source.xml \
  --dest-client-node-config config/migration/destination.xml \
  --skip-conflict-version \
  --batch-size 256 --read-threads 4 --write-threads 4
```

`--skip-conflict-version` is appropriate for a destination without conflict resolution.
Expiration timestamps are still copied exactly. To retain source versions,
omit this flag and configure conflict resolution for the destination caches on
both its servers and the migration client node. Ignite otherwise discards incoming
conflict versions; the utility rejects this mode when the client has no resolver.
The resolver's selection rules also apply to existing destination entries.

Source and destination must be active, distinct clusters. Read permission is
required on selected source caches and write/create permissions on the destination.
Selected distributed metadata operations also require their usual cluster permissions.

For a transactionally consistent copy, stop source writes externally for the
duration of migration. Scan queries do not provide snapshot isolation. Synchronize
server clocks to preserve absolute expiration timestamps. Source scans follow
the cache's configured access-expiration behavior.

## Options

| Option | Default | Meaning |
|---|---|---|
| `--src-client-node-config path` | Required | Source Spring XML. |
| `--dest-client-node-config path` | Required for built-in destination | Destination Spring XML. |
| `--destination class` | None | Public no-argument `MigrationDestination`, instead of destination XML. |
| `--cache-group-names a,b` | All | Source groups; an ungrouped cache is selected by its cache name. |
| `--no-cache-create` | false | Require all selected destination caches to exist. |
| `--create-cache-if-not-exists` | false | Accept existing destination caches and create missing ones. |
| `--no-binary-metadata-create` | false | Skip explicit binary metadata registration. |
| `--no-mappings-create` | false | Skip explicit type mapping registration. |
| `--batch-size N` | 1 | Maximum entries in a write batch. |
| `--read-threads N` | 1 | Partition reader threads. |
| `--write-threads N` | 1 | Writer threads. |
| `--queue-capacity N` | 64 | Maximum queued batches; a full queue blocks readers. |
| `--read-limit-in-sec N` | 0 | Global read rate in entries/second; zero is unlimited. |
| `--write-limit-in-sec N` | 0 | Global write submission rate; zero is unlimited. |
| `--skip-expire-time` | false | Ignore source expiration timestamps. |
| `--skip-conflict-version` | false | Ignore source conflict versions. |
| `--move-data-structures true\|false` | true | Include data structure backing caches. |
| `--cache-config-transformer class` | Identity | Public no-argument Function, BiFunction or `CacheConfigTransformer`. |
| `--cache-config-transformer-parameters k=v,...` | None | Parameters passed to `CacheConfigTransformer.configure`. |
| `--help` | | Print usage and exit successfully. |

`--no-binary-metdata-create` is accepted as an alias for the spelling in the ticket.
`--no-cache-create` and `--create-cache-if-not-exists` are mutually exclusive.
Without either option, an existing selected destination cache is an error.
The utility does not delete destination records absent from the source.

## DataStreamer

Add `--use-data-streamer` to use `IgniteDataStreamer`. Its settings are:

- `--allow-overwrite`
- `--per-node-buffer-size N`
- `--per-node-parallel-operations N`
- `--per-thread-buffer-size N`
- `--timeout N` (milliseconds, `-1` for unlimited)
- `--auto-flush-frequency N` (milliseconds, `0` to disable)

Unset values use Ignite defaults. Streamer settings require client-node destination
configuration and `--use-data-streamer`.

DataStreamer cannot carry entry versions or absolute expiration timestamps. A batch
with retained metadata fails before it is submitted. Use `--skip-conflict-version` and,
for expiring source data, `--skip-expire-time` to select this mode deliberately.
All streamer buffers are flushed and acknowledged before successful completion.

## Configuration and channels

The stages run in dependency order: binary metadata, platform mappings, selected
distributed settings, cache/SQL configuration, SQL views, and cache records. Metadata IDs,
binary schemas, current QueryEntities, SQL-created flags and indexes are retained.
Binary metadata and type mappings are cluster-wide and are copied even when only
some caches are selected.

Each transfer is an implementation of `AbstractMigrationChannel<T>` in
`org.apache.ignite.util.migration.impl.channel`:

| Channel | Entities |
|---|---|
| `BinaryMetadataMigrationChannel` | Binary metadata. |
| `TypeMappingsMigrationChannel` | Platform type mappings. |
| `MetastorageMigrationChannel` | Selected distributed settings, applied before cache creation. |
| `CacheConfigurationsMigrationChannel` | Cache and SQL configurations. |
| `SqlViewsMigrationChannel` | SQL view definitions, read and applied after cache creation. |
| `CacheDataMigrationChannel` | Cache records. |

`MigrationChannel<T>` defines the transfer contract. The abstract base shares
endpoints, configuration, typed transformations and logging. `IgniteToIgniteMigration`
owns the connections, builds an ordered `List<MigrationChannel<?>>` in `prepareChannels`,
and calls `transfer()` on each channel in a loop. Disabled binary metadata and type
mapping channels are omitted from the list. Channel selection is centralized here
so additional endpoint-specific implementations can be selected as they are added.

Each channel reads and writes its own entities through the endpoints. The metastorage
channel uses `src.metastorage` / `dest.metastorage` for settings. The SQL-view channel
independently uses `src.sqlViews` / `dest.sqlView` after the underlying tables exist.

Cache configuration, SQL-view and data channels share a `MigrationCachePlan` describing
selected cache definitions and name mappings. This helper prepares the selection once
on first use and performs no destination writes. Channels consume the plan directly;
they do not receive other channels or callbacks into them. Cache selection runs even
with `--no-cache-create`.
The data channel owns the reader/writer queue, rate limits and worker cleanup.

Cache definitions are detached copies. A transformer can change backups, affinity,
data region or cache name. Returning `null` excludes the cache and its records.
The cache configuration channel keeps source-to-destination names separately;
the destination recalculates key partitions. Group members must still have mutually
compatible configurations.

`Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>>` is compatible with
the dump-reader transformer model. A `BiFunction` also receives the destination
`IgniteConfiguration` (null for custom destinations). `CacheConfigTransformer`
adds the parameter map. Put custom classes and their dependencies on the utility's classpath.

Programmatic configuration uses `IgniteToIgniteMigrationConfiguration`:

```java
IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration()
    .setSkipConflictVersion(true)
    .setBatchSize(256)
    .setCacheConfigurationTransformer(cache -> cache.setBackups(0))
    .setMetastorageFilter(key -> key.startsWith("application."));

IgniteToIgniteMigration.start(
    ThickClientMigrationOrigin.create(srcCfg),
    ThickClientMigrationOrigin.create(destCfg),
    cfg
);
```

`setTransformer` configures a typed filter/transformer by channel class. Item types are
`BinaryMetadata`, `MigrationTypeMapping`, `MigrationCache`, and `MigrationEntry` for
their corresponding channels. Metastorage and SQL-view channels use
`Map.Entry<String, Serializable>`. Their transformers are independent: the metastorage
transformer handles settings, and the SQL-view transformer handles view definitions.
Returning null skips an item; record transformers must be thread-safe and retain
unique destination keys across concurrently processed partitions.

```java
cfg.setTransformer(CacheDataMigrationChannel.class,
    entry -> entry.key() instanceof Integer ? entry : null);
```

Distributed metastorage defaults to SQL properties (`distrConf-sql.*`) and SQL views
(`ignite.internal.sql.view.*`). Views are copied only for a complete, unrenamed user-cache
migration, so filtered migrations do not install views referencing excluded tables.
Local metastorage, authentication users, encryption keys, cluster identity, baseline
topology and deployment files are not copied.

Data structure cache/group names are preserved because collection metadata references
them. Atomics, sequences, references, stamped values, sets, queues, countdown latches,
idle semaphores and idle locks are supported. Volatile structures are rebased to the
destination cluster's start time. Locks with owners/waiters/signals and semaphores
with acquired permits are rejected: runtime node/thread ownership cannot be migrated.
Atomic sequences continue from their reserved high-water mark and can skip unused numbers.

## Completion and errors

Partitions are interleaved by primary server and scanned explicitly on their current
primary. A topology/query failure terminates the migration rather than silently switching
to a backup. Readers and writers use a bounded queue and separate global rate limiters.

The first failure stops workers, closes active cursors and connections, and produces a
nonzero process exit code. Writes already acknowledged are retained; there is no rollback
or resume offset. Logs include stage/cache/partition information, partition durations
and counts, and a record summary every minute. Buffered writer counts represent submissions;
the final successful completion is emitted only after the destination acknowledges its buffers.

WAL control is external. For a clean persistent destination, operators can disable WAL
before migration and enable it and checkpoint afterward, as described in the ticket.

## Starting point for debugging

`IgniteToIgniteMigrationTest.testSimpleMigration` creates two separate clusters and
migrates two small caches: `String -> String` and a mixed `Integer -> Object` cache
containing integers and `CustomData` values. `CustomData` has `name`, `age`, and
`city` fields; the test reads the deserialized object on the destination and checks
every field alongside the integer and string entries.

```shell
mvn -B -ntp -f modules/data-migration-ext/pom.xml test \
  "-Dtest=IgniteToIgniteMigrationTest#testSimpleMigration" -DFORK_COUNT_SET_TO_1
```
