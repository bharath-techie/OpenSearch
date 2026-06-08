# Routing the vanilla (pure-parquet) path through the indexed pipeline — handoff

> Goal: collapse the **two scan implementations into one**. Today pure-parquet
> queries (no Lucene-delegated index folders) run the **vanilla** `ListingTable`
> path; queries with delegatable filters run the **indexed** `QueryShardExec`
> path. We want pure-parquet queries to ALSO run through the indexed pipeline —
> flag-gated at first, so `ListingTable` stays the default and both can be
> diff'd for equivalence.
>
> Motivation: single code path (one place to land features/fixes), and the
> indexed path's per-operator metrics for pure-parquet queries too.
>
> Status: NOT STARTED. This doc is the plan + findings. The key finding is that
> the indexed executor **already** handles the pure-parquet case — the change is
> almost entirely a routing gate, not new scan logic.

---

## 1. Vanilla vs indexed — what actually selects each (corrected mental model)

- **Vanilla** = `query_executor::execute_with_context` → `ListingTable` (or
  `ShardTableProvider` when row-ids are needed). Chosen for **pure parquet
  filters with NO Lucene-delegated index folders**.
- **Indexed** = `indexed_executor::execute_indexed_with_context` →
  `IndexedTableProvider` / `QueryShardExec`. Chosen when the planner found
  Lucene-delegatable filters and built an `indexed_config`.

The selection is a single gate — `ffm.rs:1006`:

```rust
let use_indexed = session_handle.indexed_config.is_some()
    || (has_row_id && query_strategy != QueryStrategy::ListingTable);
```

`indexed_config` is `Some` only when Lucene delegation happened
(`session_context.rs:371`). Pure-parquet → `indexed_config = None` → `else`
branch → vanilla `ListingTable`.

NOTE: this is NOT the `QueryStrategy` enum. `QueryStrategy` only distinguishes
the row-id rewrite flavour (`ListingTable` = ShardTableProvider+row_base vs
`None`/`IndexedPredicateOnly`). The vanilla-vs-indexed scan choice is the
`use_indexed` gate above, driven by `indexed_config`.

---

## 2. The key finding — the indexed executor ALREADY runs pure-parquet

`execute_indexed_with_context_inner` (indexed_executor.rs) handles
`indexed_config = None` gracefully, no new code needed:

- `classification_override` becomes `None` (indexed_executor.rs:495).
- classification falls through to `FilterClass::None` (indexed_executor.rs:567).
- `FilterClass::None` builds a **`PredicateOnlyEvaluator`**
  (indexed_executor.rs:605-635): "page-pruned universe, residual predicate
  applied in `on_batch_mask`" — i.e. pure parquet-native filtering through the
  indexed pipeline, no Lucene collector. The comment at :610 explicitly says
  this path was already extended to work when `emit_row_ids` is false.

So `QueryShardExec` + `PredicateOnlyEvaluator` is a working pure-parquet scan
today. The only reason pure-parquet queries don't use it is the `ffm.rs` gate
never routes them there.

`PredicateOnlyEvaluator` lives at
`src/indexed_table/eval/predicate_evaluator.rs`. Residual pushdown into decode
goes through `IndexedTableConfig.pushdown_predicate` →
`ParquetSource::with_predicate` (parquet_bridge.rs); for `FilterClass::None` the
whole tree is pushed (indexed_executor.rs:592-598).

---

## 3. The change (flag-gated)

### 3a. Add the flag
`src/datafusion_query_config.rs` — new field near `target_partitions` /
`parquet_pushdown_filters`:
```rust
pub route_pure_parquet_through_indexed: bool,   // default false
```
Wire it through the FFM `WireConfigSnapshot` decode (the `From<WireConfig>` impl
~line 198-201) like the other bools, and through the Java
`WireConfigSnapshot.java` (+ a cluster setting in `DatafusionSettings.java` if
runtime-togglable is wanted).

### 3b. Flip the gate
`src/ffm.rs:1006`:
```rust
let use_indexed = session_handle.indexed_config.is_some()
    || (has_row_id && query_strategy != QueryStrategy::ListingTable)
    || session_handle.query_config.route_pure_parquet_through_indexed;  // NEW
```

That is the functional core. Flag off → byte-identical to today (vanilla
default). Flag on → pure-parquet queries go to `QueryShardExec`.

---

## 4. Risks to verify (these, not the gate, are where time will go)

1. **Session-context setup mismatch (most likely to need a 2nd edit).** There are
   two session-creation paths — `create_session_context` (vanilla) and
   `…_indexed` (indexed; deregisters default ListingTable, registers a
   placeholder then `IndexedTableProvider`). Flipping the gate alone may route a
   query whose `SessionContextHandle` was built the *vanilla* way into the
   indexed executor. Check `session_context.rs` and confirm the handle a
   pure-parquet query carries is set up so `execute_indexed_with_context` works
   (or make the indexed entry self-sufficient). START HERE in the new session.
2. **Correctness pushdown is `Exact`.** `IndexedTableProvider::supports_filters_pushdown`
   returns `Exact`, so DataFusion DROPS the outer `FilterExec`. For
   `FilterClass::None` the residual MUST be fully applied by
   `PredicateOnlyEvaluator` + parquet pushdown, or unfiltered rows leak silently.
   Test a filter that is NOT statistics-prunable (forces row-level residual).
3. **Result-set parity.** Run the same pure-parquet query both ways (flag off vs
   on) and DIFF the returned rows — must be identical. Include: no-filter full
   scan, a selective filter, a filter on a column with nulls, and a query with an
   implicit LIMIT.
4. **Row-id flavour.** Vanilla applies `ProjectRowIdOptimizer` for the
   `ListingTable` strategy. Confirm a pure-parquet query that does NOT request
   row-ids behaves the same through both paths.

---

## 5. Build / run / verify loop

```bash
# Build the dylib the node loads — WORKSPACE manifest (carries the tokio_unstable
# cargo cfg the plugin needs; a plain build in the plugin dir won't relink it):
cargo build --manifest-path /Users/gbh/Documents/dev/OpenSearch/sandbox/libs/dataformat-native/rust/Cargo.toml

# Restart the archive node (loads target/debug/libopensearch_native.dylib):
kill $(pgrep -f Dopensearch | head -1)
cd /Users/gbh/Documents/data-fol/3.7.0-ARCHIVE && ./bin/opensearch > /tmp/os-archive.log 2>&1 &
# wait for yellow:
curl -s -m 3 "localhost:9200/_cat/indices/clickbench?h=health"

# PURE-PARQUET query = filter on a non-indexed column, NO match():
#   source = clickbench | where ResolutionWidth > 1900 | fields ResolutionWidth, RegionID
# Flag OFF: plan/scan is vanilla DataSourceExec (ListingTable).
# Flag ON : scan is QueryShardExec (indexed path ran).
# Diff the returned rows between the two — must be identical.

# Unit tests run from the PLUGIN rust dir (needs its .cargo/config.toml):
cd sandbox/plugins/analytics-backend-datafusion/rust && cargo test
```

### Gotchas (carried from the scan-timing work)
- Rust→Java logs only via `native_bridge_common::log_info!` (plain `log::*` is
  dropped). Logs land in `…/3.7.0-ARCHIVE/logs/opensearch.log`.
- Verify the loaded dylib has your change: `strings <dylib> | grep <log-string>`
  and check dylib mtime < node start (`ps -o lstart= -p <pid>`).
- Keep the node single-partition for clean comparison:
  `search.concurrent_segment_search.mode = none` forces `target_partitions = 1`.

---

## 6. File map (where everything is)

| What | File:line |
|---|---|
| vanilla↔indexed routing gate | `src/ffm.rs:1006` (`use_indexed`) |
| vanilla scan (ListingTable / ShardTableProvider) | `src/query_executor.rs:107,135,240` |
| indexed entry (context variant) | `src/indexed_executor.rs:434` `execute_indexed_with_context` |
| classification → FilterClass::None fallback | `src/indexed_executor.rs:564-570` |
| pure-parquet evaluator build | `src/indexed_executor.rs:605-635` |
| `PredicateOnlyEvaluator` | `src/indexed_table/eval/predicate_evaluator.rs` |
| residual→parquet pushdown | `src/indexed_executor.rs:585-600`, `parquet_bridge.rs` |
| query config + FFM decode | `src/datafusion_query_config.rs` (`target_partitions`, decode ~:198) |
| Java wire config | `src/main/java/org/opensearch/be/datafusion/WireConfigSnapshot.java` |
| `IndexedTableProvider` / `QueryShardExec` | `src/indexed_table/table_provider.rs` |
| session context setup (vanilla vs indexed) | `src/session_context.rs` |

Related: `scan-timing-investigation.md` and `scan-latency-waterfall.md` (the
indexed path's measured latency profile — useful when comparing perf of the two
paths on the same pure-parquet query).
