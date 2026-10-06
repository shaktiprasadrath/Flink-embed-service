# PRD: Flink Embedded Feature-Showcase Service

Status: **APPROVED 2026-10-05** (H2, Groovy DSL, package `com.shakti.flinkdemo`, JDK 21 toolchain auto-provisioning)  
Owner: shakti  
Date: 2026-10-05  
Reference: https://nightlies.apache.org/flink/flink-docs-stable/docs/

## 1. Problem / Goal

Provide a runnable Java 21 + Gradle application that embeds Apache Flink (local MiniCluster, no external cluster) and demonstrates every major Flink feature through small, isolated, testable use cases. Each use case reads data from a file, processes it with Flink, and persists results into an in-memory database.

Goal: a developer clones the repo, runs one Gradle command, and sees each Flink feature working end to end. A feature catalog doc explains each feature and how to test it.

## 2. Non-Goals

- No external Flink cluster, Kubernetes, YARN, or Docker deployment.
- No Kafka or other external infrastructure (file source only).
- No production hardening (security, HA, autoscaling).
- No UI.

## 3. Tech Stack

| Item | Choice | Note |
|---|---|---|
| Language | Java 21 | Gradle toolchain pins 21 |
| Build | Gradle 8.14.3 (Groovy DSL) + wrapper | Toolchain and Gradle daemon JVM pinned to 21, auto-provisioned |
| Flink | 2.2.1 | Docs "stable" is 2.3.0, but the newest JDBC connector is 4.1.0-2.2, so 2.2.1 is the newest Flink with a matching JDBC connector. Java 21 support is marked experimental by Flink (since 2.0) |
| In-memory DB | H2 (`jdbc:h2:mem:`) | Flink JDBC connector as sink; plain JDBC for verification queries |
| Tests | JUnit 5 + AssertJ, embedded MiniCluster | One test class per use case |
| Logging | SLF4J + Log4j2 | |
| Input | CSV / JSONL files under `data/` | orders, payments, rates, customers, clicks, txns, rules, auth events, sensors, users |

Dependencies managed through Gradle version catalog (`gradle/libs.versions.toml`), Flink as one of several dependencies.

UC-10 lookup join: the JDBC connector has no H2 dialect, so the lookup join uses a small custom `LookupTableSource` connector (`h2-lookup`), which also demonstrates the Table connector SPI.

## 4. Functional Requirements

### 4.1 Core pipeline (shared)
- FR-1: Read input from file using Flink `FileSource` (bounded; continuous-monitor variant for streaming demo).
- FR-2: Process with Flink job.
- FR-3: Persist output to H2 in-memory DB through JDBC sink.
- FR-4: Each use case runnable independently via CLI: `./gradlew run --args="<use-case-id>"` and `./gradlew run --args="all"`.
- FR-5: After each run, app queries H2 and prints result rows for verification.

### 4.2 Feature use cases

| ID | Feature area | Use case | Input | Output table |
|---|---|---|---|---|
| UC-01 | DataStream basics: map, flatMap, filter, keyBy, reduce | Clean and aggregate orders per customer | orders.csv | `customer_totals` |
| UC-02 | Event time + watermarks (bounded out-of-orderness, idle sources) | Late-event handling on clickstream | clicks.jsonl | `click_events` |
| UC-03 | Windows: tumbling, sliding, session, global + triggers/evictors | Page views per window type | clicks.jsonl | `window_tumbling`, `window_sliding`, `window_session` |
| UC-04 | Keyed state: ValueState, ListState, MapState, state TTL | Running per-user session counter, dedup | clicks.jsonl | `user_state` |
| UC-05 | Broadcast state | Dynamic rules applied to transactions | txns.csv + rules.csv | `flagged_txns` |
| UC-06 | ProcessFunction + timers (event/processing time) | Inactivity alert after N seconds | clicks.jsonl | `inactivity_alerts` |
| UC-07 | Side outputs | Route invalid / late records to dead-letter table | orders.csv (with bad rows) | `orders_valid`, `orders_dead_letter` |
| UC-08 | Joins: window join, interval join, connect/co-process | Orders joined with payments | orders.csv, payments.csv | `order_payments` |
| UC-09 | Table API + Flink SQL (DDL, aggregations, Top-N, dedup, windowing TVFs) | SQL analytics on orders | orders.csv | `sql_daily_sales`, `sql_top_products` |
| UC-10 | Temporal table join + lookup join | Enrich orders with currency rates | orders.csv, rates.csv | `orders_enriched` |
| UC-11 | Changelog / retraction semantics (DataStream <-> Table conversion) | Updating aggregates | orders.csv | `changelog_demo` |
| UC-12 | UDFs: scalar, table, aggregate | Custom parsing and scoring functions | users.csv | `udf_results` |
| UC-13 | CEP (pattern matching) | Detect login-fail x3 then success | auth_events.csv | `cep_matches` |
| UC-14 | Async I/O | Enrich events via simulated async lookup | txns.csv | `async_enriched` |
| UC-15 | Checkpointing + exactly-once + restart strategies + failure injection | Job fails mid-run, recovers, no duplicate rows in DB | orders.csv | `exactly_once_orders` |
| UC-16 | State backends (HashMap vs RocksDB) + savepoint stop/resume | Stop with savepoint, restore, continue | clicks.jsonl | `savepoint_demo` |
| UC-17 | Batch execution mode (`RuntimeExecutionMode.BATCH`) | Same job streaming vs batch, compare results | orders.csv | `batch_vs_stream` |
| UC-18 | Parallelism, partitioning, operator chaining, slot sharing | Show effect of `setParallelism`, `rebalance`, `keyBy`, `disableChaining` | sensors.csv | `parallelism_demo` |
| UC-19 | Metrics + accumulators + custom metrics | Custom counter/gauge/histogram in operators | sensors.csv | `metrics_demo` |
| UC-20 | Custom source/sink (new Source API, SinkV2) | Custom file-tail source and H2 sink | any | `custom_io_demo` |
| UC-21 | Configuration + Web UI (local env with REST UI) | Run with UI on localhost:8081 | any | n/a |

Scope note: the list covers features usable in an embedded/local setup. Features requiring a real cluster (HA, K8s, autoscaler, reactive mode) are documented as "out of scope — needs cluster" in the catalog.

### 4.3 Documentation deliverables
- DOC-1: `README.md` updated — overview, prerequisites (JDK 21), build/run/test commands, project layout, how to add a use case.
- DOC-2: `docs/FEATURES.md` — feature catalog. Per feature: what it is, Flink doc link, use case ID, class path, sample input, expected output rows, exact command to run, exact test command, how to verify in H2.
- DOC-3: this PRD kept in `docs/PRD.md`.

## 5. Non-Functional Requirements

- NFR-1: Java 21 enforced via Gradle toolchain; build fails on older JDK.
- NFR-2: Full test suite runs on a laptop in < 5 minutes, no network, no Docker.
- NFR-3: Each use case deterministic: fixed input files, assertion on exact DB rows.
- NFR-4: Use case code self-contained; shared code limited to a `common` package (H2 setup, file paths, runner).
- NFR-5: Fat/shadow JAR NOT required; run via `./gradlew run`. Flink JDK-module `--add-opens` flags for Java 21 configured in Gradle `run`/`test` tasks.

## 6. Proposed Project Layout

```
build.gradle.kts, settings.gradle.kts, gradle/libs.versions.toml, gradlew
src/main/java/.../
  App.java                  # CLI entry: run <uc-id>|all
  common/                   # H2 init, JDBC sink factory, file paths, runner
  usecases/uc01_basics/ ... uc21_webui/
data/                       # sample input files
src/test/java/.../          # one test per use case
docs/PRD.md, docs/FEATURES.md
```

## 7. Acceptance Criteria

1. `./gradlew build` succeeds on JDK 21; fails with clear message on other JDKs.
2. `./gradlew run --args="all"` runs UC-01..UC-21 and prints H2 result rows per use case.
3. `./gradlew test` passes; each use case has at least one test asserting exact output rows.
4. UC-15 test proves exactly-once: injected failure, recovery, row count equals expected with zero duplicates.
5. `docs/FEATURES.md` lists every feature in 4.2 with run + test + verify instructions that work copy-paste.
6. README updated per DOC-1.
7. Out-of-scope cluster-only features explicitly listed.

## 8. Milestones

1. Scaffold: Gradle, Java 21 toolchain, Flink + H2 deps, common runner, UC-01 end to end.
2. UC-02..UC-08 (DataStream core).
3. UC-09..UC-12 (Table/SQL/UDF).
4. UC-13..UC-20 (CEP, async, fault tolerance, batch, metrics, custom IO).
5. UC-21, docs, README, final verification.

Progress reported at each milestone. Work stays on branch `feature/flink-operator-demo`; commits made only when the owner asks.

## 9. Risks / Open Questions

1. **Flink version vs Java 21**: confirm current stable Flink officially supports Java 21 and what flags/modules needed. Will verify in docs before coding.
2. **Local tooling**: machine has JDK 11 and Gradle 7.3.3. Need JDK 21 installed; Gradle wrapper (8.5+) will be added. Toolchain auto-download may be needed — OK?
3. **RocksDB on Windows** for UC-16: native lib via `flink-statebackend-rocksdb`; fallback = HashMap backend only if it fails.
4. **JDBC connector version** must match Flink version; may lag latest Flink release.
5. **CEP, Table/SQL, JDBC** are separate Flink modules; dependency footprint is large (acceptable?).

## 10. Decisions Needed From You

1. Approve use case list (add / remove any UC)?
2. H2 as the in-memory DB OK (vs HSQLDB / SQLite in-memory)?
3. Gradle Kotlin DSL OK (vs Groovy DSL)?
4. Base package name (suggest `com.shakti.flinkdemo`)?
5. Install JDK 21 locally via Gradle toolchain auto-provisioning OK?

**No implementation starts until you approve this PRD.**
