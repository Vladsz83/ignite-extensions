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
| `--destination class` | None | Public no-argument `MigrationOrigin`, instead of destination XML. |
| `--cache-group-names a,b` | All | Source groups; an ungrouped cache is selected by its cache name. |
| `--no-cache-create` | false | Require all selected destination caches to exist. |
| `--create-cache-if-not-exists` | false | Accept existing destination caches and create missing ones. |
| `--no-binary-metadata-create` | false | Skip explicit binary metadata registration. |
| `--no-mappings-create` | false | Skip explicit type mapping registration. |
| `--ignore-default-filter` | false | Also copy distributed metastorage entries that are not portable to the destination cluster (cluster identity, graceful-shutdown, SQL views). |
| `--batch-size N` | 1 | Maximum entries in a write batch. |
| `--read-threads N` | 1 | Partition reader threads. |
| `--write-threads N` | 1 | Writer threads. |
| `--migration-threads N` | 4 | Thread count of the shared executor used by all channels. |
| `--queue-capacity N` | 64 | Maximum queued batches; a full queue blocks readers. |
| `--read-limit-in-sec N` | 0 | Global read rate in entries/second; zero is unlimited. |
| `--write-limit-in-sec N` | 0 | Global write submission rate; zero is unlimited. |
| `--skip-expire-time` | false | Ignore source expiration timestamps. |
| `--skip-conflict-version` | false | Ignore source conflict versions. |
| `--move-data-structures true\|false` | true | Include data structure backing caches. |
| `--cache-config-transformer class` | Identity | Public no-argument `Function` or `BiFunction` (destination-aware). |
| `--cache-config-transformer-parameters k=v,...` | None | Invoked reflectively via a public `configure(Map)` method on the transformer. |
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

The stages run in dependency order: binary metadata, platform mappings,
cache/SQL configuration, distributed settings, and cache records.
Metadata IDs, binary schemas, current QueryEntities, SQL-created flags and indexes
are retained. Binary metadata and type mappings are cluster-wide and are copied even
when only some caches are selected.

Each transfer is an implementation of `AbstractMigrationChannel<FROM, TO, E>`: the origin
types and the transferred entity type are declared per channel, and `from()`/`to()`
return the raw origin connections. Channels and their entity types are grouped by
category under `org.apache.ignite.util.migration.category`:

| Package | Channels | Entities |
|---|---|---|
| `category.type` | `BinaryMetadataMigrationChannel`, `TypeMappingsMigrationChannel` | Binary metadata, platform type mappings (`MigrationTypeMapping`). |
| `category.metastorage` | `MetastorageMigrationChannel` | Distributed settings and SQL view definitions. |
| `category.cache` | `CacheConfigurationsMigrationChannel`, `CacheDataMigrationChannel` | Cache configurations (`MigrationCache`), cache records (`MigrationEntry`). |

The client-node endpoint `ThickClientMigrationEndpoint` lives in
`org.apache.ignite.util.migration.endpoint`. It implements `MigrationOrigin<IgniteEx>`,
whose `origin()` returns the raw in-JVM connection. `IgniteToIgniteMigration.prepareChannels`
performs a one-time `instanceof IgniteEx` check on both origins (justifying an unchecked
`MigrationOrigin<IgniteEx>` view) and validates that they resolve to different clusters
before any transfer starts. This is the extension point for future origin types (for
example a thin-client origin): channels that accept them are parameterized accordingly.

`MigrationChannel<T>` defines the transfer contract. The abstract base shares the two
origins, configuration, a shared executor, typed transformations and logging.
`IgniteToIgniteMigration` owns the connections, builds an ordered `List<MigrationChannel<?>>`
in `prepareChannels`, and calls `transfer()` on each channel in a loop. Disabled binary
metadata and type mapping channels are omitted from the list. Channel selection is
centralized here so additional endpoint-specific implementations can be selected as they
are added.

Every channel draws its worker threads from a single shared `ExecutorService` owned by
`IgniteToIgniteMigration`. Its size is `max(migrationThreads, readThreads + writeThreads)`
so the cache-data reader/writer workers always have enough threads even when
`--migration-threads` is set low. Channels that parallelize a transfer submit tasks to this
pool and wait for their own work to finish before returning, so the orchestrator shuts the
pool down only after the last channel completes.

Each channel reads and writes its own entities through the origins. The metastorage
channel iterates the source distributed metastorage and writes keys to the destination.
A default key filter skips entries that are not portable to the destination cluster:
the cluster identity key, whose cluster ID destination nodes would adopt on startup,
graceful-shutdown coordination, which holds source node ids, and SQL view definitions,
which reference schemas that may be absent on the destination. The filter can be
disabled with `--ignore-default-filter`.

Cache configuration and data channels share a `MigrationCachePlan` describing
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
`IgniteConfiguration` (null for custom destinations). A public
`configure(Map<String, String>)` method receives the
`--cache-config-transformer-parameters` map. Put custom classes and their
dependencies on the utility's classpath.

Programmatic configuration uses `IgniteToIgniteMigrationConfiguration`:

```java
IgniteToIgniteMigrationConfiguration cfg = new IgniteToIgniteMigrationConfiguration();

cfg.cacheData().skipConflictVersion(true).batchSize(256);
cfg.cacheSelection().cacheConfigurationTransformer(cache -> cache.setBackups(0));
cfg.metastorage().filter(new MetastorageKeyFilterTransformer(
    Arrays.asList("secret.key"), Collections.singletonList("secret.")));

IgniteToIgniteMigration.start(
    ThickClientMigrationEndpoint.create(srcCfg),
    ThickClientMigrationEndpoint.create(destCfg),
    cfg
);
```

The top-level configuration aggregates shared settings (`logger`, `migrationThreads`,
channel-enable flags) and per-channel sub-configurations reached through
`cacheData()`, `cacheSelection()`, `cacheConfigurations()` and `metastorage()`. Each
sub-config exposes only the settings its channel consumes.

`transformer` configures a typed filter/transformer by channel class. It is applied by
`CacheConfigurationsMigrationChannel` (item type `MigrationCache`) and
`MetastorageMigrationChannel` (item type `Map.Entry<String, Serializable>`, sees all
keys after the default and configured key filters). Returning null skips an item.

```java
cfg.transformer(CacheConfigurationsMigrationChannel.class,
    cache -> "excluded".equals(cache.sourceName()) ? null : cache);
```

Distributed metastorage is copied to the destination, since it holds portable
cluster-wide dynamic configuration. The default key filter skips the cluster identity
key, whose cluster ID destination nodes would adopt on startup, graceful-shutdown
coordination, which holds source node ids, and SQL view definitions;
`--ignore-default-filter` copies them all. Additional
`MetastorageKeyFilterTransformer`s can be added with `metastorage().filter(...)` to
skip further entries by exact key or prefix, matched case-insensitively.
Local metastorage, authentication users, encryption keys, baseline topology and
deployment files are not copied.

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
