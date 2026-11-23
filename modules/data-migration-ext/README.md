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
Ignite clusters using thick or thin clients. Source and destination server counts,
consistent IDs, persistence settings and affinity configurations may differ.
Data is read directly from the source and written to the destination.

Thin-client mode transfers cache configurations exposed through the thin API and
cache records using `ScanQuery` and ordinary cache writes. It does not transfer
explicit binary metadata/type mappings, distributed metastorage, SQL views, or
data-structure caches. Entry versions and exact expiration timestamps are not
available through thin-client scans. Thin-client cache configuration is a subset;
custom affinity functions fall back to the default rendezvous affinity.

## Build and install

From the extensions repository root:

```shell
mvn -B -ntp -f modules/data-migration-ext/pom.xml package -Pcheckstyle
```

Use a matching Ignite 2.20.0-SNAPSHOT installation. Copy the module JAR and its
runtime dependencies from `target/libs` to an installation library directory,
for example `$IGNITE_HOME/libs/data-migration-ext`. Copy `bin/ignite-to-ignite.sh`
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

Thick-client connection XML must contain exactly one `IgniteConfiguration` bean;
the utility starts it in client mode with a unique instance name. Thin-client
connection XML must contain exactly one `ClientConfiguration` bean. Alternatively,
pass comma-separated thin-client addresses directly. The supplied thick-client
examples use discovery ports 47500 and 47600; replace their addresses for your
environment.

```shell
bin/ignite-to-ignite.sh \
  --src-thin 127.0.0.1:10800 \
  --dest-thin-config config/migration/destination-thin.xml \
  --cache-group-names accounts,reference \
  --batch-size 256 --read-threads 4 --write-threads 4
```

Thick-to-thick mode remains available:

```shell
bin/ignite-to-ignite.sh \
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
If CDC replication is enabled on the destination, configure the same `dcId` on both
clusters to avoid re-sending migrated records through an additional CDC cycle.

For a transactionally consistent copy, stop source writes externally for the
duration of migration. Scan queries do not provide snapshot isolation. Synchronize
server clocks to preserve absolute expiration timestamps. Source scans follow
the cache's configured access-expiration behavior.

## Options

| Option | Default | Meaning |
|---|---|---|
| `--src-client-node-config path` | One source option required | Source Spring XML with `IgniteConfiguration`. |
| `--src-thin host:port,...` | One source option required | Source thin-client addresses. |
| `--src-thin-config path` | One source option required | Spring XML with `ClientConfiguration`. |
| `--dest-client-node-config path` | One destination option required | Destination Spring XML with `IgniteConfiguration`. |
| `--dest-thin host:port,...` | One destination option required | Destination thin-client addresses. |
| `--dest-thin-config path` | One destination option required | Spring XML with `ClientConfiguration`. |
| `--destination class` | None | Public no-argument `MigrationDestination`, instead of a built-in destination. |
| `--cache-group-names a,b` | All | Source groups; an ungrouped cache is selected by its cache name. |
| `--no-cache-create` | false | Require all selected destination caches to exist. |
| `--create-cache-if-not-exists` | false | Accept existing destination caches and create missing ones. |
| `--no-binary-metadata-create` | false | Skip explicit binary metadata registration. |
| `--no-mappings-create` | false | Skip explicit type mapping registration. |
| `--ignore-default-filter` | false | Also copy distributed metastorage entries that are not portable to the destination cluster (cluster identity, graceful-shutdown, and SQL views even when the destination has no SQL schema). |
| `--batch-size N` | 256 | Maximum entries in a write batch. |
| `--read-threads N` | 4 | Number of cache-data reader threads in its dedicated pool. |
| `--write-threads N` | 4 | Number of cache-data writer threads in its dedicated pool. |
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
`--skip-cdc-info` is accepted as an alias for `--skip-conflict-version`.
`--no-cache-create` and `--create-cache-if-not-exists` are mutually exclusive.
Without either option, an existing selected destination cache is an error.
The utility does not delete destination records absent from the source.

### Thin-client limitations

When either endpoint uses a thin client, the pipeline transfers selected cache
configuration and records. A thin source also transfers binary metadata known to its
local binary context and Java mappings whose classes are loadable by the migration
process. Metadata and mappings unavailable to the thin client cannot be enumerated;
non-Java mappings are not exposed. Distributed metastorage, SQL views, and data-structure
state are skipped. Cache-data transfer also registers metadata and Java mappings found
in transferred entries unless `--no-binary-metadata-create` or `--no-mappings-create`
disables it.
A thin source's ScanQuery does not return entry versions or expiry; those cannot be
preserved in thin-source migrations. For a thick source and thin
destination, the cache-data transferrer reads row metadata on source primaries
and uses Ignite's internal thin-client `TcpClientCache.putAllConflict` path to retain
versions and absolute expiry. That path requires conflict resolution on the destination
servers. If source conflict versions are skipped, remaining TTL is applied per entry
through the thin client's expiry policy. `--use-data-streamer` is rejected for a thin
destination. Thin-source scans use public ScanQuery and do not expose the
primary-by-partition scheduling used by the thick-source path.

Only properties available in `ClientCacheConfiguration` can be copied to or from
a thin endpoint. The SQL-created flag is not exposed and is not preserved. A custom
cache transformer still receives a `CacheConfiguration` reconstructed from those
available properties; custom affinity functions cannot be represented by the thin
API and fall back to default rendezvous affinity. A
`BiFunction` transformer receives `null` for the destination Ignite configuration
when the destination is thin.

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

## Configuration and transferrers

For thick-to-thick migration, stages run in dependency order: binary metadata,
platform mappings, cache/SQL configuration, distributed settings, and cache records.
Metadata IDs, binary schemas, current QueryEntities, SQL-created flags and indexes
are retained. Binary metadata and type mappings are cluster-wide and are copied even
when only some caches are selected.

Each endpoint implements `MigrationOrigin` and reports its category capabilities. Its
`connect()` method returns the raw client connection; orchestration passes that connection
to transferrer constructors. Transferrers and their entity types are grouped by category
under `org.apache.ignite.util.migration.category`:

| Package | Transferrers | Entities |
|---|---|---|
| `category.type` | `BinaryMetadataTransferrer`, `TypeMappingsTransferrer`, `ThinClientBinaryMetadataTransferrer`, `ThinClientTypeMappingsTransferrer` | Binary metadata, platform type mappings (`MigrationTypeMapping`). |
| `category.metastorage` | `MetastorageTransferrer` | Distributed settings and SQL view definitions. |
| `category.cache` | `CacheConfigurationsTransferrer`, `CacheDataTransferrer` | Cache configurations (`CacheConfigurationMigration`), cache records (`CacheEntryMigration`). |

`ThickClientMigrationEndpoint` and `ThinClientMigrationEndpoint` live in
`org.apache.ignite.util.migration.endpoint` and implement `MigrationDestination`, which extends
the common `MigrationOrigin` contract. Each endpoint provides a `LinkedHashMap` of supported
categories to payload classes; the destination map order declares processing priority. Its
`connect()` returns the raw connection. The destination also accepts individual category
entities. Thick endpoints provide metadata, mappings, cache configuration, metastorage entries
and cache entries. Thin endpoints provide locally available metadata and Java mappings, cache
configuration, and cache entries.
Different clusters are checked by cluster UUID for thick clients and overlapping server node IDs
when a thin client is involved.

`CategoryTransferrer<T>` defines a category, payload type, and `transfer(destination, transformer)`
operation. Every category uses this contract. Metadata and mapping transferrers read, transform,
and send each item directly to `MigrationDestination.accept` on the transfer thread, without an
intermediate collection; a `null` transformer result skips that item. No category provider is exposed as an `Iterable`. `IgniteToIgniteMigration`
selects compatible categories and runs them in destination priority order. Category IDs are stable;
add new enum values only with IDs greater than all existing values, and never reuse an ID.

Cache data migration owns separate reader and writer pools, each defaulting to four threads.
They are closed by the transferrer after its work finishes; there is no shared migration-wide
executor.

Each transferrer reads and writes its own entities through the origins. The metastorage
transferrer scans source distributed metastorage through its callback API and writes keys to the destination.
A default key filter skips entries that are not portable to the destination cluster:
the cluster identity key, whose cluster ID destination nodes would adopt on startup,
graceful-shutdown coordination, which holds source node ids, and SQL view definitions
when the destination has no SQL schema. The filter can be disabled with
`--ignore-default-filter`, which copies all of these entries unconditionally.

Cache configuration and data transferrers share a `MigrationCachePlan` describing
selected cache definitions and name mappings. This helper prepares the selection once
on first use and performs no destination writes. Transferrers consume the plan directly;
they do not receive other transferrers or callbacks into them. Cache selection runs even
with `--no-cache-create`.
The cache-data transferrer owns the reader/writer queue, rate limits and worker cleanup.

Cache definitions are detached copies. A transformer can change backups, affinity,
data region or cache name. Returning `null` excludes the cache and its records.
The cache configuration transferrer keeps source-to-destination names separately;
the destination recalculates key partitions. Group members must still have mutually
compatible configurations.

`Function<CacheConfiguration<?, ?>, CacheConfiguration<?, ?>>` is compatible with
the dump-reader transformer model. A `BiFunction` also receives the destination
`IgniteConfiguration` (null for thin/custom destinations). In thin-source mode,
the source configuration contains only the properties exposed by the thin API. A public
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

The top-level configuration aggregates shared settings (`logger`, category transformers)
and per-category sub-configurations reached through
`cacheData()`, `cacheSelection()`, `cacheConfigurations()` and `metastorage()`. Each
sub-config exposes only the settings its transferrer consumes.

`transformer` configures a typed filter/transformer by transferrer class. Binary metadata,
type mappings, cache configurations, metastorage entries, and cache entries all support
transformers. The metastorage transformer sees keys after the default and configured key
filters. Returning `null` skips an item.

```java
cfg.transformer(CacheConfigurationsTransferrer.class,
    cache -> "excluded".equals(cache.sourceName()) ? null : cache);
```

Distributed metastorage is copied to the destination, since it holds portable
cluster-wide dynamic configuration. The default key filter skips the cluster identity
key, whose cluster ID destination nodes would adopt on startup, graceful-shutdown
coordination, which holds source node ids, and SQL view definitions when the
destination has no SQL schema; `--ignore-default-filter` copies them all. Additional
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

WAL control is external. For a clean persistent destination, disable WAL before the
migration and enable it and checkpoint afterward:

```shell
./control.sh --wal --disable
# Run the migration.
./control.sh --wal --enable && ./control.sh --checkpoint
```

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
