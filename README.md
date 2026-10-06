# Flink-embed-service

A runnable Java 21 + Gradle application that embeds **Apache Flink 2.2** as a regular library dependency. Flink runs in-process as a local MiniCluster, so no Flink cluster is needed.

The application contains 21 use cases. Each one demonstrates one Flink feature area: it reads sample data from a file, processes it with Flink, and stores the result in an **H2 in-memory database**.

- Feature catalog and how to test each feature: [docs/FEATURES.md](docs/FEATURES.md)
- Product requirements: [docs/PRD.md](docs/PRD.md)
- Docker image, Docker Hub and Kubernetes (Docker Desktop): [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)

## Prerequisites

- Network access on the first build. Gradle, a JDK 21 (if not installed) and the dependencies are downloaded automatically.
- Any JDK 8+ to start the Gradle wrapper. The wrapper then runs the build and the app on **JDK 21**, through Gradle toolchains and `gradle/gradle-daemon-jvm.properties`. Nothing needs to be installed by hand.

## Quick start

```bash
./gradlew run                      # list use cases
./gradlew run --args="uc01"        # run one use case and print its result tables
./gradlew run --args="uc03 uc09"   # run several
./gradlew run --args="all"         # run all 21 (about 3 minutes; UC-21 keeps the Web UI open for 30 s)
./gradlew test                     # run the test suite (one test class per use case)
```

On Windows, use `gradlew.bat ...` (or `.\gradlew ...` in PowerShell).

Example output:

```
=== uc01: DataStream basics: map, flatMap, filter, keyBy, reduce ===
  finished in 3125 ms
  table customer_totals (3 rows)
  customer_id | order_count | total_quantity | gross_amount
  ---------------------------------------------------------
  C1          | 4           | 7              | 2475.00
  C2          | 3           | 6              | 1290.00
  C3          | 3           | 6              | 380.00
```

### Options

| Option | Effect |
|---|---|
| `-Dflinkdemo.h2.console=true` | After the run, open the H2 web console at http://localhost:8082 (JDBC URL `jdbc:h2:mem:flinkdemo`, user `sa`, empty password). Press Enter to exit. |
| `-Dflinkdemo.webui.port=8081` | Port of the Flink Web UI in UC-21 |
| `-Dflinkdemo.webui.seconds=30` | How long UC-21 keeps its job and Web UI running |
| `-Dflinkdemo.data.dir=data` | Folder with the input files |
| `-Dflinkdemo.webui.bind=localhost` | Bind address of the Flink Web UI (`0.0.0.0` in containers) |
| `-Dflinkdemo.keepalive=true` | After the run, keep the JVM and H2 console up until stopped (containers) |
| `-Dflinkdemo.h2.allowOthers=true` | Let the H2 console accept non-localhost connections (containers; it has no login) |

For example: `./gradlew run --args="uc21" -Dflinkdemo.webui.seconds=300`. Then open http://localhost:8081.

## Use cases

| ID | Feature | Input |
|---|---|---|
| uc01 | DataStream basics: map, flatMap, filter, keyBy, reduce | orders.csv |
| uc02 | Event time, watermarks, late events, idleness | clicks.jsonl |
| uc03 | Windows: tumbling, sliding, session, global + trigger/evictor, late side output | clicks.jsonl |
| uc04 | Keyed state: ValueState, ListState, MapState, state TTL | clicks.jsonl |
| uc05 | Broadcast state: dynamic rules | txns.csv, rules.csv |
| uc06 | ProcessFunction + event-time timers | clicks.jsonl |
| uc07 | Side outputs (dead-letter table) | orders.csv |
| uc08 | Joins: interval, window, connect + CoProcessFunction | orders.csv, payments.csv |
| uc09 | Flink SQL: DDL, window TVF, Top-N, deduplication | orders.csv, clicks.jsonl |
| uc10 | SQL temporal join (versioned table) + lookup join (custom connector) | orders.csv, rates.csv, customers.csv |
| uc11 | DataStream <-> Table conversion, Table API, changelog semantics | orders.csv |
| uc12 | UDFs: scalar, table, aggregate | users.csv |
| uc13 | CEP: pattern matching with timeouts | auth_events.csv |
| uc14 | Async I/O: ordered results, retries | txns.csv |
| uc15 | Checkpointing, restart strategy, failure recovery, exactly-once | orders.csv |
| uc16 | RocksDB state backend, stop-with-savepoint, resume from savepoint | data/savepoint/*.jsonl |
| uc17 | STREAMING vs BATCH execution mode | orders.csv |
| uc18 | Parallelism, partitioning, operator chaining, slot sharing | sensors.csv |
| uc19 | Metrics, custom metric reporter, accumulators | sensors.csv |
| uc20 | Custom Source (FLIP-27) and custom Sink V2 | sensors.csv, users.csv |
| uc21 | Configuration, Web UI, REST API | generated |

See [docs/FEATURES.md](docs/FEATURES.md) for each use case's expected output and ways to experiment with it.

## Using Flink as a Gradle dependency

Flink is declared like any other library in the version catalog [gradle/libs.versions.toml](gradle/libs.versions.toml) and pulled in through one bundle:

```groovy
dependencies {
    implementation libs.bundles.flink   // Flink runtime, connectors, Table API, CEP, RocksDB
    implementation libs.h2
    implementation libs.jackson.databind
    testImplementation libs.flink.test.utils
}
```

| Module | Purpose |
|---|---|
| `flink-streaming-java`, `flink-clients` | DataStream API and local (embedded) execution |
| `flink-runtime-web` | Web UI for the local cluster (UC-21) |
| `flink-connector-files`, `flink-connector-base`, `flink-connector-datagen` | FileSource and DataGen source |
| `flink-csv`, `flink-json` | Formats for the SQL filesystem connector |
| `flink-table-api-java-bridge`, `flink-table-planner-loader`, `flink-table-runtime` | Table API and SQL |
| `flink-cep` | Complex event processing |
| `flink-statebackend-rocksdb` | RocksDB state backend |
| `flink-connector-jdbc-core` | JDBC sink |

Notes:

- **Flink version.** The docs' "stable" version is 2.3.0. However, the newest JDBC connector release is `4.1.0-2.2`, built for Flink 2.2. So 2.2.1 is the newest Flink with a matching JDBC connector. To upgrade, change `flink` and `flinkJdbc` in the version catalog together.
- **Java 21.** Flink marks Java 21 support as experimental (Java 17 is the default). It works for everything in this project.
- **JVM flags.** On Java 17+, Flink needs reflective access to some JDK internals. The same `--add-opens` / `--add-exports` list that the Flink distribution uses (`env.java.opts.all`) is set for `run` and `test` in [build.gradle](build.gradle). If you embed Flink in another app, copy that list to its JVM options.
- **Fat JAR.** Running inside a Flink cluster would need a shadow JAR with the Flink modules marked as provided. This project runs Flink embedded and does not build one.
- **Harmless console messages.**
  - `Error extracting native library 'libopenlineage_sql_java'` comes from the JDBC connector's optional lineage support.
  - The stack trace in UC-15 is the intentionally injected failure.

## Project layout

```
build.gradle, settings.gradle, gradle/libs.versions.toml   Gradle build (Groovy DSL) + version catalog
data/                                                       sample input files
src/main/java/com/shakti/flinkdemo/
  App.java                       CLI: run use cases, print H2 tables
  common/                        H2 access, environments, file sources, watermarks, JDBC + changelog sinks, SQL DDL
  model/                         Java records for the input data (Flink serializes records as POJOs)
  usecases/ucNN_*/               one package per use case (UseCases.java is the registry)
src/main/resources/
  log4j2.properties              quiet Flink logging
  META-INF/services/             custom Table connector + metric reporter registration
src/test/java/.../usecases/      one test class per use case, asserting exact H2 rows
docs/                            PRD.md, FEATURES.md, DEPLOYMENT.md
Dockerfile, .dockerignore        container image (multi-stage, JRE 21, non-root)
k8s/                             Kubernetes Job + explorer Deployment/Service (kustomize)
```

## Adding a use case

1. Create `src/main/java/com/shakti/flinkdemo/usecases/ucNN_name/UcNNName.java` implementing `UseCase`. In `run()`:
   - Create the output tables with `Db.recreate(...)`.
   - Build the job on `Envs.local(parallelism)`.
   - Read input with `Sources.lines(...)` or `Sources.events(...)`.
   - Write with `JdbcSinks.sink(...)` (or `H2ChangelogSink` for Table results).
   - Call `env.execute(...)`.
2. Register it in `usecases/UseCases.java`.
3. Add `src/test/java/com/shakti/flinkdemo/usecases/UcNNNameTest.java`. It should call `run()` and assert rows with `Db.rows("SELECT ...")`.
4. Document it in `docs/FEATURES.md`.

## Determinism

Tests assert exact rows, so the jobs are built to be deterministic:

- Event-time jobs parse and assign watermarks with parallelism 1, using a per-event watermark generator (`common/Watermarks`).
- Updating results use idempotent `MERGE` upserts.
- Where timing matters (UC-14 retries, UC-15 failure recovery, UC-16 savepoints), the code explains what it controls and why.
