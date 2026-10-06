# Flink Feature Catalog

Every Flink feature covered by this project, the use case that demonstrates it, and how to run, test and verify it.

- Flink version: **2.2.1** (embedded MiniCluster), Java 21
- Flink docs: https://nightlies.apache.org/flink/flink-docs-stable/docs/ (links below point to the stable docs; a few pages may differ slightly from 2.2)
- All input files are in [`data/`](../data). All output goes to the H2 in-memory database `jdbc:h2:mem:flinkdemo`.

## How to test any use case

| Goal | Command |
|---|---|
| List use cases | `./gradlew run` |
| Run one use case and print its result tables | `./gradlew run --args="uc03"` |
| Run several | `./gradlew run --args="uc01 uc09 uc13"` |
| Run all | `./gradlew run --args="all"` |
| Run one use case's automated test | `./gradlew test --tests "*Uc03*"` |
| Run all tests | `./gradlew test` |
| Browse H2 in a web console after the run | `./gradlew run --args="uc03" -Dflinkdemo.h2.console=true` then open http://localhost:8082 (JDBC URL `jdbc:h2:mem:flinkdemo`, user `sa`, empty password). Press Enter in the terminal to exit. |

On Windows use `gradlew.bat` (or `.\gradlew` in PowerShell) instead of `./gradlew`.

Each test asserts the exact rows written to H2, so the "Expected output" sections below are the same values the tests check. To experiment, edit a file in `data/`, re-run the use case and compare the printed tables (the tests will then fail, as expected).

---

## Index

| # | Feature area | Use case | Output tables |
|---|---|---|---|
| 1 | DataStream API: map, flatMap, filter, keyBy, reduce | UC-01 | `customer_totals` |
| 2 | Event time, watermarks, late events, idleness | UC-02 | `click_events` |
| 3 | Windows: tumbling, sliding, session, global, triggers, evictors, late side output | UC-03 | `window_tumbling`, `window_sliding`, `window_session`, `window_global`, `window_late_events` |
| 4 | Keyed state: ValueState, ListState, MapState, state TTL | UC-04 | `user_state` |
| 5 | Broadcast state | UC-05 | `flagged_txns` |
| 6 | ProcessFunction and timers | UC-06 | `inactivity_alerts` |
| 7 | Side outputs | UC-07 | `orders_valid`, `orders_dead_letter` |
| 8 | Stream joins: interval, window, connect/CoProcessFunction | UC-08 | `join_results` |
| 9 | Flink SQL: DDL, filesystem connector, formats, window TVF, Top-N, deduplication | UC-09 | `sql_daily_sales`, `sql_top_products`, `sql_first_page` |
| 10 | Temporal join, versioned table, lookup join, custom Table connector | UC-10 | `customers`, `orders_enriched` |
| 11 | DataStream/Table conversion, Table API, changelog semantics | UC-11 | `changelog_demo`, `changelog_final` |
| 12 | UDFs: scalar, table, aggregate | UC-12 | `udf_users`, `udf_interests`, `udf_country` |
| 13 | CEP (pattern matching, timeouts) | UC-13 | `cep_matches` |
| 14 | Async I/O with retries | UC-14 | `async_enriched` |
| 15 | Checkpointing, restart strategy, exactly-once, failure recovery | UC-15 | `exactly_once_orders`, `exactly_once_totals`, `at_least_once_log`, `exactly_once_stats` |
| 16 | RocksDB state backend, savepoints, stop and resume | UC-16 | `savepoint_demo` |
| 17 | Batch vs streaming execution mode | UC-17 | `batch_vs_stream` |
| 18 | Parallelism, partitioning, chaining, slot sharing | UC-18 | `parallelism_demo`, `job_vertices` |
| 19 | Metrics, custom metric reporter, accumulators | UC-19 | `metrics_demo` |
| 20 | Custom Source (FLIP-27) and Sink V2 | UC-20 | `custom_io_demo` |
| 21 | Configuration, Web UI, REST API | UC-21 | `webui_jobs` |
| — | Connectors used across use cases: FileSource, JDBC sink, filesystem SQL connector, CSV/JSON formats, DataGen | many | — |

---

## 1. DataStream API basics — UC-01

- **What**: The core transformations. `flatMap` parses each CSV line and drops malformed ones (0 or 1 output per input). `filter` removes orders with a non-positive quantity. `map` converts each order to a per-order total. `keyBy` partitions by customer. `reduce` keeps a running total per key. The results are written with the JDBC sink connector using an idempotent `MERGE`.
- **Docs**: [DataStream overview](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/), [Operators](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/), [JDBC connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/jdbc/)
- **Code**: [`Uc01BasicTransformations`](../src/main/java/com/shakti/flinkdemo/usecases/uc01_basics/Uc01BasicTransformations.java)
- **Input**: `orders.csv` (10 valid orders, 1 with quantity -1, 1 malformed line)
- **Run / test**: `./gradlew run --args="uc01"` / `./gradlew test --tests "*Uc01*"`
- **Expected output** (`customer_totals`):

  | customer_id | order_count | total_quantity | gross_amount |
  |---|---|---|---|
  | C1 | 4 | 7 | 2475.00 |
  | C2 | 3 | 6 | 1290.00 |
  | C3 | 3 | 6 | 380.00 |

- **Try**: change O1's quantity to 2 in `orders.csv`. C1 becomes `4 | 8 | 3475.00`.

## 2. Event time and watermarks — UC-02

- **What**: Timestamps come from the event (`ts` field), not from the machine clock. A bounded out-of-orderness watermark (10 s) tells Flink that no events older than `max_seen - 10 s` are expected. Each event is stored with the watermark that was current when it arrived. An event whose timestamp is at or below that watermark is late. `withIdleness` stops an idle split from holding back watermarks.
- **Note**: Flink's built-in `forBoundedOutOfOrderness` emits watermarks periodically (every 200 ms). With small files the whole input is read before the first periodic watermark, so the demos use [`Watermarks.perEvent`](../src/main/java/com/shakti/flinkdemo/common/Watermarks.java). It has the same semantics but emits after every event, which makes the output deterministic.
- **Docs**: [Timely stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/), [Generating watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/)
- **Code**: [`Uc02EventTimeWatermarks`](../src/main/java/com/shakti/flinkdemo/usecases/uc02_eventtime/Uc02EventTimeWatermarks.java)
- **Input**: `clicks.jsonl` (12 clicks; the 11th, u2 `/cart` at 10:01:05, arrives after a click at 10:06:00)
- **Run / test**: `./gradlew run --args="uc02"` / `./gradlew test --tests "*Uc02*"`
- **Expected output**: `click_events` has 12 rows. Only `seq=11` has `is_late = TRUE`, with `watermark_before = 2026-01-01 10:05:49.999` (that is, 10:06:00 - 10 s - 1 ms). The first row's watermark is `null` because no watermark exists yet.
- **Verify**: `SELECT * FROM click_events WHERE is_late`

## 3. Windows — UC-03

- **What**: Clicks per user with each window assigner:
  - **Tumbling** 2 min
  - **Sliding** 2 min, every 1 min
  - **Session** with a 2 min gap (windows merge, so the `AggregateFunction` implements `merge`)
  - **Global** window with a custom **trigger** (`CountTrigger.of(2)`) and **evictor** (`CountEvictor.of(2)`, so only the last 2 clicks are kept)

  Counting uses incremental aggregation (`AggregateFunction`) combined with a `ProcessWindowFunction` that adds the window start and end. Late clicks are captured with `sideOutputLateData` and are not silently dropped.
- **Docs**: [Windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/)
- **Code**: [`Uc03Windows`](../src/main/java/com/shakti/flinkdemo/usecases/uc03_windows/Uc03Windows.java)
- **Run / test**: `./gradlew run --args="uc03"` / `./gradlew test --tests "*Uc03*"`
- **Expected output**:
  - `window_tumbling`: 7 rows, for example `u1 | 10:00-10:02 | 4`. The late u2 click is **not** counted.
  - `window_sliding`: 15 rows, for example `u1 | 09:59-10:01 | 3` and `u1 | 10:00-10:02 | 4`.
  - `window_session`: `u1 10:00:00-10:03:30 (4)`, `u1 10:06:00-10:08:00 (1)`, `u2 10:00:30-10:04:40 (3)`, `u3 10:01:15-10:05:00 (2)`, `u3 10:06:30-10:08:30 (1)`.
  - `window_global`: 5 firings, for example `u1 | 2 | /cart|/checkout`. Global windows never drop late data, so u2's second firing is `/home|/cart`.
  - `window_late_events`: `u2 | /cart | 10:01:05`
- **Try**: change the session gap to `Duration.ofMinutes(5)`. u1's two sessions merge into one.

## 4. Keyed state — UC-04

- **What**: Per-key state in a `KeyedProcessFunction`:
  - `ValueState` for the total clicks
  - `ListState` for the last 3 pages
  - `MapState` for visits per page, configured with **state TTL** (1 hour, `OnCreateAndWrite`, `NeverReturnExpired`)
- **Docs**: [Working with state](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/), [State TTL](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl)
- **Code**: [`Uc04KeyedState`](../src/main/java/com/shakti/flinkdemo/usecases/uc04_keyedstate/Uc04KeyedState.java)
- **Run / test**: `./gradlew run --args="uc04"` / `./gradlew test --tests "*Uc04*"`
- **Expected output** (`user_state`):

  | user_id | total_clicks | distinct_pages | last_pages | top_page |
  |---|---|---|---|---|
  | u1 | 5 | 4 | /cart,/checkout,/home | /home |
  | u2 | 4 | 3 | /products,/home,/cart | /home |
  | u3 | 3 | 3 | /home,/products,/cart | /cart (tie, alphabetical) |

## 5. Broadcast state — UC-05

- **What**: Fraud rules (`rules.csv`) are broadcast to every parallel instance and stored in broadcast state. Transactions (`txns.csv`) are keyed by account. A `KeyedBroadcastProcessFunction` checks each transaction against all rules. Rules and transactions can arrive in any order, so transactions are also kept in keyed state. When a new rule arrives, `applyToKeyedState` re-checks the stored transactions. As a result, every (transaction, rule) pair is evaluated exactly once.
- **Docs**: [Broadcast state pattern](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/broadcast_state/)
- **Code**: [`Uc05BroadcastState`](../src/main/java/com/shakti/flinkdemo/usecases/uc05_broadcast/Uc05BroadcastState.java)
- **Run / test**: `./gradlew run --args="uc05"` / `./gradlew test --tests "*Uc05*"`
- **Expected output** (`flagged_txns`): `T2 R1`, `T3 R2`, `T4 R1`, `T4 R2`, `T7 R1` (R1 = amount > 500, R2 = merchant casino)
- **Try**: add `R3,AMOUNT_OVER,100` to `rules.csv`. T1 is then flagged as well.

## 6. ProcessFunction and timers — UC-06

- **What**: A `KeyedProcessFunction` raises an inactivity alert when a user has no click for 2 minutes of event time.
  - Each click deletes the old **event-time timer** and registers a new one.
  - `onTimer` fires when the watermark passes the timer (`TIMER`).
  - If the watermark jumps past the timer together with the next click, the gap is detected when that click is processed (`ON_EVENT`).
  - Late clicks are ignored.
  - At end of input Flink sends a final watermark, so all pending timers fire.
- **Docs**: [Process function](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/)
- **Code**: [`Uc06ProcessFunctionTimers`](../src/main/java/com/shakti/flinkdemo/usecases/uc06_timers/Uc06ProcessFunctionTimers.java)
- **Run / test**: `./gradlew run --args="uc06"` / `./gradlew test --tests "*Uc06*"`
- **Expected output** (`inactivity_alerts`):

  | user_id | last_seen | alert_time | detected_by |
  |---|---|---|---|
  | u1 | 10:01:30 | 10:03:30 | ON_EVENT |
  | u1 | 10:06:00 | 10:08:00 | TIMER |
  | u2 | 10:02:40 | 10:04:40 | TIMER |
  | u3 | 10:03:00 | 10:05:00 | TIMER |
  | u3 | 10:06:30 | 10:08:30 | TIMER |

## 7. Side outputs — UC-07

- **What**: One `ProcessFunction` routes valid orders to the main output and invalid lines to a side output (`OutputTag`) together with the reason. The two outputs go to different tables. UC-03 shows the built-in side output for late window data.
- **Docs**: [Side outputs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/side_output/)
- **Code**: [`Uc07SideOutputs`](../src/main/java/com/shakti/flinkdemo/usecases/uc07_sideoutputs/Uc07SideOutputs.java)
- **Run / test**: `./gradlew run --args="uc07"` / `./gradlew test --tests "*Uc07*"`
- **Expected output**:
  - `orders_valid` has 10 rows.
  - `orders_dead_letter` has 2 rows:
    - `O11,...,-1,... | non-positive quantity`
    - `this,is,not,valid | parse error: expected 7 fields but got 4`

## 8. Stream joins — UC-08

- **What**: Orders joined with payments on `order_id` in three ways:
  - **Interval join**: the payment falls within `[order_time, order_time + 30 min]`.
  - **Window join**: the order and payment are in the same 1-hour tumbling window.
  - **connect + KeyedCoProcessFunction**: custom matching with state and timers. It also reports `UNPAID` orders and `ORPHAN_PAYMENT`s after 24 h of event time.
- **Docs**: [Joining](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/joining/), [Process function (low-level joins)](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/#low-level-joins)
- **Code**: [`Uc08Joins`](../src/main/java/com/shakti/flinkdemo/usecases/uc08_joins/Uc08Joins.java)
- **Input**: `orders.csv`, `payments.csv`
- **Run / test**: `./gradlew run --args="uc08"` / `./gradlew test --tests "*Uc08*"`
- **Expected output** (`join_results`):
  - `INTERVAL`: O1, O3, O5, O7, O10. O2 (paid after 45 min) and O9 (after 40 min) are too late.
  - `WINDOW`: O1, O2, O3, O5, O7, O9, O10.
  - `CO_PROCESS`: the same 7 matches, plus O4, O6 and O8 as `UNPAID` and O99/P8 as `ORPHAN_PAYMENT`.
- **Verify**: `SELECT join_type, COUNT(*) FROM join_results GROUP BY join_type` returns `CO_PROCESS 11`, `INTERVAL 5`, `WINDOW 7`.

## 9. Flink SQL — UC-09

- **What**:
  - SQL DDL with the **filesystem connector**, the **CSV** and **JSON formats**, a computed column, and a `WATERMARK` definition.
  - Daily revenue with the **window table-valued function** `TUMBLE`.
  - **Top-N** (`ROW_NUMBER` over an aggregate).
  - **Deduplication** to find each user's first page.

  These queries produce updating (changelog) results, so they are written with an upsert sink, [`H2ChangelogSink`](../src/main/java/com/shakti/flinkdemo/common/H2ChangelogSink.java).
- **Docs**: [Table API & SQL](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/), [Window TVF](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/window-tvf/), [Top-N](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/topn/), [Deduplication](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/deduplication/), [Filesystem SQL connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/filesystem/)
- **Code**: [`Uc09TableApiSql`](../src/main/java/com/shakti/flinkdemo/usecases/uc09_sql/Uc09TableApiSql.java), [`TableDdl`](../src/main/java/com/shakti/flinkdemo/common/TableDdl.java)
- **Run / test**: `./gradlew run --args="uc09"` / `./gradlew test --tests "*Uc09*"`
- **Expected output**:
  - `sql_daily_sales`: 2026-01-01 has 6 orders and revenue 2605.00. 2026-01-02 has 4 orders and revenue 1540.00.
  - `sql_top_products`: `1 mouse 9`, `2 keyboard 4`
  - `sql_first_page`: u1, u2 and u3 all start at `/home`
- **Note**: The malformed CSV line becomes a row of NULLs (`csv.ignore-parse-errors`). A rowtime must never be NULL, so `order_time` is a computed column with a fallback, and the `valid_orders` view filters that row out.

## 10. Temporal join, lookup join, custom connector — UC-10

- **What**:
  - **Lookup join** (`FOR SYSTEM_TIME AS OF o.proc_time`): customer name and tier are queried from an H2 table at processing time. The official JDBC connector has no H2 dialect, so this uses a small **custom Table connector**, `'connector' = 'h2-lookup'` (factory, `LookupTableSource`, `LookupFunction`, registered through `META-INF/services`).
  - **Event-time temporal join** (`FOR SYSTEM_TIME AS OF o.order_time`): each order uses the exchange rate that was valid at the order's time. The append-only rates file becomes a **versioned table** through a deduplicating view.
- **Docs**: [Joins: temporal and lookup](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/), [Versioned tables](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/versioned_tables/), [User-defined sources & sinks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sourcessinks/)
- **Code**: [`Uc10TemporalLookupJoins`](../src/main/java/com/shakti/flinkdemo/usecases/uc10_temporal/Uc10TemporalLookupJoins.java), [`h2lookup/`](../src/main/java/com/shakti/flinkdemo/usecases/uc10_temporal/h2lookup)
- **Input**: `orders.csv`, `rates.csv` (EUR is 1.10, then 1.20 from 2026-01-02), and `customers.csv` (loaded into H2 first)
- **Run / test**: `./gradlew run --args="uc10"` / `./gradlew test --tests "*Uc10*"`
- **Expected output** (`orders_enriched`):
  - O3: `Acme Corp | GOLD | 400.00 EUR | 1.1000 | 440.00`
  - O5: `1000.00 EUR | 1.1000 | 1100.00`
  - O10: `100.00 EUR | 1.2000 | 120.00`
  - USD orders use a rate of 1.0000.
- **Try**: add `EUR,1.30,2026-01-02 12:00:00` to `rates.csv`. O10 (15:30 on day 2) then uses rate 1.3000.

## 11. DataStream/Table conversion and changelog — UC-11

- **What**: `fromDataStream` turns a `DataStream<Order>` (Java records) into a Table. A **Table API** (not SQL) group-by computes totals. `toChangelogStream` turns the updating result back into a DataStream of `Row`s with a `RowKind`:
  - `+I` insert
  - `-U` retract the old value
  - `+U` the new value
- **Docs**: [DataStream API integration](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/data_stream_api/), [Dynamic tables](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/dynamic_tables/), [Table API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/tableapi/)
- **Code**: [`Uc11ChangelogConversion`](../src/main/java/com/shakti/flinkdemo/usecases/uc11_changelog/Uc11ChangelogConversion.java)
- **Run / test**: `./gradlew run --args="uc11"` / `./gradlew test --tests "*Uc11*"`
- **Expected output**:
  - `changelog_demo` has 17 rows: 3 `+I`, 7 `-U` and 7 `+U`. The sequence starts `+I C1`, `+I C2`, `-U C1 (1)`, `+U C1 (2)`, `+I C3`.
  - `changelog_final` holds the same totals as UC-01.

## 12. User-defined functions — UC-12

- **What**:
  - **Scalar function** `MASK_EMAIL`
  - **Table function** `SPLIT_INTERESTS`, used with `LATERAL TABLE`
  - **Aggregate function** `TOP_INTEREST`, which has a POJO accumulator
- **Docs**: [User-defined functions](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/udfs/)
- **Code**: [`Uc12UserDefinedFunctions`](../src/main/java/com/shakti/flinkdemo/usecases/uc12_udf/Uc12UserDefinedFunctions.java)
- **Input**: `users.csv`
- **Run / test**: `./gradlew run --args="uc12"` / `./gradlew test --tests "*Uc12*"`
- **Expected output**:
  - `udf_users`: `U1 | ALICE SMITH | a****@example.com | US`, and so on.
  - `udf_interests`: 10 rows.
  - `udf_country`: `DE 1 tech`, `IN 1 food` (tie, alphabetical), `US 2 music`.

## 13. Complex Event Processing — UC-13

- **What**: FlinkCEP detects 3 consecutive `FAIL` logins followed directly by `SUCCESS`, all within 5 minutes, per user and in event time. The pattern uses:
  - `times(3).consecutive()` for the failures
  - `next` (strict contiguity) for the success
  - `within(5 min)` for the time limit

  Partial matches that time out are reported with `TimedOutPartialMatchHandler`.
- **Docs**: [FlinkCEP](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)
- **Code**: [`Uc13ComplexEventProcessing`](../src/main/java/com/shakti/flinkdemo/usecases/uc13_cep/Uc13ComplexEventProcessing.java)
- **Input**: `auth_events.csv`
- **Run / test**: `./gradlew run --args="uc13"` / `./gradlew test --tests "*Uc13*"`
- **Expected output** (`cep_matches`):
  - `alice | 10:00:00 | 10:00:30 | 3 | MATCH`
  - `carol | 10:02:10 | 10:02:40 | 3 | MATCH`. Carol has 4 failures, and only the last 3 match.
  - `dave | 10:03:00 | null | 3 | TIMEOUT`. Dave's success came 17 minutes later.
  - bob has no row (only 1 failure).

## 14. Async I/O — UC-14

- **What**: `AsyncDataStream.orderedWaitWithRetry` enriches transactions through a simulated remote service, with up to 10 requests in flight. Results leave in input order even though later requests finish first. The service fails the first call for account A2, and a fixed-delay **retry strategy** recovers it. There is also a 5 s timeout.
- **Note**: Flink disables async retries once the operator's input has ended. The demo throttles the tiny input file so that the retries happen while the stream is still open.
- **Docs**: [Async I/O](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/asyncio/)
- **Code**: [`Uc14AsyncIo`](../src/main/java/com/shakti/flinkdemo/usecases/uc14_async/Uc14AsyncIo.java)
- **Run / test**: `./gradlew run --args="uc14"` / `./gradlew test --tests "*Uc14*"`
- **Expected output** (`async_enriched`): rows in order T1 to T7. T2 and T5 (A2) have `attempts = 2`. Tiers are A1 LOW, A2 MEDIUM and A3 HIGH.
- **Try**: switch to `unorderedWait`. `seq` then follows completion order instead of input order.

## 15. Checkpointing, restart strategy, exactly-once — UC-15

- **What**:
  - Checkpoints are taken every 100 ms.
  - A **failure is injected** while processing the 6th order.
  - The **fixed-delay restart strategy** restarts the job, and Flink restores state and the file source's read position from the last checkpoint, then replays the records after it.
  - Totals kept in keyed state are **exactly-once**.
  - The JDBC sink is at-least-once. Written with an idempotent `MERGE` the DB result is still exactly-once, but a plain `INSERT` log can contain replayed duplicates.

  For non-idempotent writes the JDBC connector also offers an XA sink, `buildExactlyOnce`. It is not used here.
- **Docs**: [Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/), [Task failure recovery](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/task_failure_recovery/), [Fault tolerance guarantees](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/guarantees/)
- **Code**: [`Uc15CheckpointingExactlyOnce`](../src/main/java/com/shakti/flinkdemo/usecases/uc15_exactlyonce/Uc15CheckpointingExactlyOnce.java)
- **Run / test**: `./gradlew run --args="uc15"` / `./gradlew test --tests "*Uc15*"`
- **Expected output**:
  - `exactly_once_stats`: `injected_failures = 1`, `records_processed_including_replays > 10`, `distinct_orders = 10`
  - `exactly_once_totals`: the same values as UC-01, with no double counting
  - `at_least_once_log`: at least 10 rows and exactly 10 distinct order ids
- **Verify**: the log line `Injected failure while processing O6` appears in the output.

## 16. RocksDB state backend and savepoints — UC-16

- **What**:
  1. An unbounded job (a `FileSource` that keeps monitoring a folder) counts clicks per user in **RocksDB** state.
  2. The job is stopped with **`stopWithSavepoint`**.
  3. A new file is added to the folder.
  4. A second job is **started from the savepoint**.

  The counts continue from the saved state, and the source remembers which files it already read. Operators have stable `uid`s so state maps back after a restart.
- **Docs**: [State backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/), [Savepoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/), [FileSource](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/filesystem/)
- **Code**: [`Uc16SavepointsStateBackend`](../src/main/java/com/shakti/flinkdemo/usecases/uc16_savepoints/Uc16SavepointsStateBackend.java)
- **Input**: `data/savepoint/clicks-part1.jsonl`, then `clicks-part2.jsonl`
- **Run / test**: `./gradlew run --args="uc16"` / `./gradlew test --tests "*Uc16*"`
- **Expected output** (`savepoint_demo`): `u1 5`, `u2 4`, `u3 3`, all written by phase 2. Without the restore, u1 would be 2.
- **Verify**: the log shows `Phase 1 stopped with savepoint file:/.../savepoint-...`.

## 17. Execution modes — UC-17

- **What**: The same keyBy and reduce program runs in `STREAMING` mode and in `BATCH` mode. Streaming emits an updated total for every input record. Batch knows the input is bounded and emits only the final result per key.
- **Docs**: [Execution mode (batch/streaming)](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/execution_mode/)
- **Code**: [`Uc17BatchExecutionMode`](../src/main/java/com/shakti/flinkdemo/usecases/uc17_batch/Uc17BatchExecutionMode.java)
- **Run / test**: `./gradlew run --args="uc17"` / `./gradlew test --tests "*Uc17*"`
- **Expected output** (`batch_vs_stream`):
  - `STREAMING` has 10 rows and `BATCH` has 4.
  - The final totals in both modes are `keyboard 4`, `laptop 3`, `monitor 3` and `mouse 9`.

## 18. Parallelism, partitioning, chaining, slot sharing — UC-18

- **What**: The job wires operators with different parallelism and data exchange:
  - Source (p=1), then `rebalance` to parse (p=4)
  - `keyBy` (hash partitioning) to max-per-sensor and format (p=2, **chained** into one task)
  - audit (p=2, `disableChaining`)
  - Sink (p=1) in its own **slot sharing group**

  The job graph vertices are stored, and the execution plan JSON is printed. You can paste it into the Flink plan visualizer.
- **Docs**: [Parallel execution](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/execution/parallel/), [Task chaining and resource groups](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/#task-chaining-and-resource-groups)
- **Code**: [`Uc18ParallelismChaining`](../src/main/java/com/shakti/flinkdemo/usecases/uc18_parallelism/Uc18ParallelismChaining.java)
- **Run / test**: `./gradlew run --args="uc18"` / `./gradlew test --tests "*Uc18*"`
- **Expected output**:
  - `parallelism_demo`: S1 23.2, S2 22.8, S3 19.5, S4 26.3, with 3 readings each. Each sensor is always handled by the same keyed subtask (0 or 1).
  - `job_vertices`:
    - `parse` (4)
    - `max-per-sensor -> format` (2, chained)
    - `audit` (2)
    - `h2:parallelism_demo...` (1)

## 19. Metrics and accumulators — UC-19

- **What**:
  - Custom operator **metrics** in group `flinkdemo`: Counter, Meter, Histogram (`DescriptiveStatisticsHistogram`) and Gauge.
  - A **custom metric reporter**, configured with `metrics.reporter.collect.factory.class` and registered through `META-INF/services`. It captures the final values.
  - An **accumulator**, read from `JobExecutionResult`.
- **Docs**: [Metrics](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/), [Metric reporters](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/metric_reporters/), [Accumulators & counters](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/user_defined_functions/#accumulators--counters)
- **Code**: [`Uc19MetricsAccumulators`](../src/main/java/com/shakti/flinkdemo/usecases/uc19_metrics/Uc19MetricsAccumulators.java), [`CollectingMetricReporter`](../src/main/java/com/shakti/flinkdemo/usecases/uc19_metrics/CollectingMetricReporter.java)
- **Run / test**: `./gradlew run --args="uc19"` / `./gradlew test --tests "*Uc19*"`
- **Expected output** (`metrics_demo`):
  - `accumulator.hot-readings` = 5
  - `metric.readings` = 12
  - `metric.readingsPerSecond` = events=12
  - `metric.lastTemperature` = 26.3
  - `metric.temperatureX10` = count=12, min=187, max=263

## 20. Custom Source and Sink — UC-20

- **What**: [`LinesSource`](../src/main/java/com/shakti/flinkdemo/usecases/uc20_customio/LinesSource.java) is written against the unified **Source API (FLIP-27)**:
  - Splits are files, and each split records the lines already read, so it can be checkpointed.
  - A `SplitEnumerator` hands out splits on request.
  - A `SourceReader` in each subtask reads its splits.
  - Serializers are provided for the splits and the enumerator state.

  [`BufferedH2Sink`](../src/main/java/com/shakti/flinkdemo/usecases/uc20_customio/BufferedH2Sink.java) is written against **Sink V2**. It buffers rows and writes them as a batch on `flush`, which Flink calls at checkpoints and at end of input.
- **Docs**: [Data sources (FLIP-27)](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sources/), [DataStream connectors overview](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/overview/)
- **Code**: [`Uc20CustomSourceSink`](../src/main/java/com/shakti/flinkdemo/usecases/uc20_customio/Uc20CustomSourceSink.java)
- **Run / test**: `./gradlew run --args="uc20"` / `./gradlew test --tests "*Uc20*"`
- **Expected output** (`custom_io_demo`): 13 lines from `sensors.csv` and 5 from `users.csv`, read in parallel by 2 readers. `writer_subtask` shows which subtask wrote each line.

## 21. Configuration, Web UI, REST API — UC-21

- **What**: `createLocalEnvironmentWithWebUI` starts the embedded cluster with the **web dashboard**. It needs `flink-runtime-web`, and it is configured through a `Configuration` (`rest.port`). The use case runs an unbounded `DataGeneratorSource` job (5 records/s, with checkpoints every 2 s). It then reads `/jobs/overview` from the **REST API** and cancels the job.
- **Docs**: [Configuration](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/), [REST API](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/rest_api/), [DataGen connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/datagen/)
- **Code**: [`Uc21WebUi`](../src/main/java/com/shakti/flinkdemo/usecases/uc21_webui/Uc21WebUi.java)
- **Run**: `./gradlew run --args="uc21"`. While it runs (30 s by default), open http://localhost:8081 to see the job graph, metrics, checkpoints and back pressure.
  - Run longer: `./gradlew run --args="uc21" -Dflinkdemo.webui.seconds=300`
  - Use another port: `-Dflinkdemo.webui.port=8090`
- **Test**: `./gradlew test --tests "*Uc21*"`. The test uses a free port and a 3 s run.
- **Expected output** (`webui_jobs`): `uc21-web-ui-demo | RUNNING | <events written so far, > 0>`

---

## Connectors and formats used

| Connector / format | Where |
|---|---|
| `FileSource` + `TextLineInputFormat` (bounded and continuous monitoring) | `Sources`, UC-16 |
| JDBC sink (`flink-connector-jdbc-core`, `JdbcSink.builder()`) | most use cases |
| Filesystem SQL connector with CSV and JSON formats | UC-09, UC-10, UC-12 |
| DataGen source (`DataGeneratorSource`, rate limiting) | UC-21 |
| Custom Table connector (`h2-lookup`) | UC-10 |
| Custom Source (FLIP-27) and Sink V2 | UC-20, `H2ChangelogSink` |

## Not covered (needs a real cluster, external systems, or removed in Flink 2.x)

| Feature | Why not here |
|---|---|
| High availability (ZooKeeper / Kubernetes HA) | Needs a cluster and a coordination service |
| Deployment: standalone, YARN, native Kubernetes, Flink Kubernetes Operator, application mode | Needs a cluster; this project runs Flink embedded |
| Reactive mode, adaptive scheduler rescaling, Flink autoscaler | Needs a cluster with changing resources |
| Kafka, Pulsar, Kinesis, Elasticsearch and other external connectors | Need external systems (the project uses files and H2 only) |
| XA exactly-once JDBC sink | Needs a database with full XA support; the idempotent-upsert approach is shown instead (UC-15) |
| SQL Client, SQL Gateway, catalogs (Hive, JDBC catalog), materialized tables | Separate processes or external metastores |
| PyFlink (Python API) | Java-only project |
| ForSt / disaggregated state, changelog state backend | Built for remote storage in a cluster; RocksDB (UC-16) shows the pluggable backend concept |
| DataSet API, Scala API, Queryable State, legacy SourceFunction/SinkFunction | Removed or deprecated in Flink 2.0 |
