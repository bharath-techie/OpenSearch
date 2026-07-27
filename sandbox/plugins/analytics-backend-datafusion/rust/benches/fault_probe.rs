//! Page-fault attribution probe: our execution path vs a plain DataFusion baseline.
//!
//! # Why
//!
//! On ClickBench q18 (`count(*) GROUP BY UserID, SearchPhrase`, 100M rows, 616MB scan) the node
//! takes ~433k-510k minor page faults per query, all of them on `datafusion-cpu` threads, while
//! stock `datafusion-cli` takes ~67k for the identical query on the identical data under the
//! identical jemalloc config. That 7.6x is not the allocator, not the Java layer, and not the
//! shared RuntimeEnv — so it has to be something this crate does around the pool / budget /
//! session. This probe isolates that in ONE process with no JVM, no rebuild-and-restart cycle:
//! run the same substrait plan through
//!
//!   A. `SessionContext::new_with_state` + plain `GreedyMemoryPool`  (baseline, ~= datafusion-cli)
//!   B. our `execute_query` path with the tracking pool / budget / guard
//!
//! and count minor faults + wall time per iteration for each. A large A-vs-B fault gap localises
//! the churn to our wrapper; a small one means the fault difference lives elsewhere.
//!
//! # Usage
//!
//! ```text
//! CB_DIR=/path/to/parquet/dir CB_SQL='SELECT ... FROM t GROUP BY ...' \
//!   cargo bench --bench fault_probe
//! ```
//! Defaults to a synthetic 2M-row two-column table when `CB_DIR` is unset, which still exercises
//! the grouped-aggregate path that dominates q18.

use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt};
use opensearch_datafusion::api::DataFusionRuntime;
use opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig;
use opensearch_datafusion::query_executor;
use opensearch_datafusion::runtime_manager::RuntimeManager;
use prost::Message;

/// Process-wide minor fault count, summed across all threads. Per-thread `/proc/self/stat` is
/// useless here: the faults land on the tokio CPU workers, not the caller.
fn minor_faults() -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir("/proc/self/task") {
        for e in entries.flatten() {
            if let Ok(s) = std::fs::read_to_string(e.path().join("stat")) {
                if let Some(rest) = s.rfind(')').map(|i| &s[i + 2..]) {
                    if let Some(v) = rest.split(' ').nth(7).and_then(|v| v.parse::<u64>().ok()) {
                        total += v;
                    }
                }
            }
        }
    }
    total
}

fn synth_parquet(dir: &std::path::Path, rows: usize) {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let schema = Arc::new(Schema::new(vec![
        Field::new("UserID", DataType::Int64, false),
        Field::new("SearchPhrase", DataType::Utf8, false),
    ]));
    // High-cardinality group keys so the hash table is large — that is what q18 stresses.
    let ids: Vec<i64> = (0..rows as i64).map(|i| i % (rows as i64 / 3 + 1)).collect();
    let phrases: Vec<String> = (0..rows).map(|i| format!("phrase-{}", i % 9973)).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(phrases)),
        ],
    )
    .unwrap();
    let file = File::create(dir.join("bench.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// The ClickBench df54-ordered `create.sql`, verbatim, so arm A is the same table definition
/// `datafusion-cli` benchmarks against: sort order declared via WITH ORDER (+
/// prefer_existing_sort), `binary_as_string`, and the EventDate-casting view. Registering a
/// hand-rolled ListingTable instead measured ~1.53s vs the CLI's ~1.02s on q18, i.e. the harness
/// rather than the engine.
async fn register_like_cli(ctx: &SessionContext, dir: &str) {
    let ddl = format!(
        "CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '{}/' \
         WITH ORDER (\"CounterID\" DESC, \"EventDate\" DESC, \"UserID\" DESC, \
         \"EventTime\" DESC, \"WatchID\" DESC) OPTIONS ('binary_as_string' 'true')",
        dir.trim_end_matches('/')
    );
    ctx.sql(&ddl).await.unwrap().collect().await.unwrap();
    ctx.sql("SET datafusion.optimizer.prefer_existing_sort = true")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql(
        "CREATE VIEW hits AS SELECT * EXCEPT (\"EventDate\"), \
         CAST(\"EventDate\" AS DATE) AS \"EventDate\" FROM hits_raw",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
}

async fn register(dir: &str) -> (SessionContext, Arc<arrow::datatypes::Schema>) {
    let ctx = SessionContext::new();
    let url = ListingTableUrl::parse(dir).unwrap();
    let opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_file_extension(".parquet")
        .with_collect_stat(true);
    let inferred = opts.infer_schema(&ctx.state(), &url).await.unwrap();
    // Coerce exactly as the production scan boundary does, so the substrait plan we produce here
    // declares the same types the execution path will bind against. Without this the plan carries
    // `_id: BinaryView` (parquet's view type) while our table provider reports `Binary`, and the
    // substrait consumer rejects the scan. See schema_coerce.rs module docs.
    let schema = opensearch_datafusion::schema_coerce::coerce_inferred_schema(inferred);
    let cfg = ListingTableConfig::new(url)
        .with_listing_options(opts)
        .with_schema(schema.clone());
    ctx.register_table("t", Arc::new(ListingTable::try_new(cfg).unwrap()))
        .unwrap();
    (ctx, schema)
}

fn main() {
    let dir_owned;
    let (dir, tmp_guard) = match std::env::var("CB_DIR") {
        Ok(d) => (d, None),
        Err(_) => {
            let tmp = tempfile::tempdir().unwrap();
            synth_parquet(tmp.path(), 2_000_000);
            dir_owned = tmp.path().to_str().unwrap().to_string();
            (dir_owned.clone(), Some(tmp))
        }
    };
    let sql = std::env::var("CB_SQL").unwrap_or_else(|_| {
        "SELECT \"UserID\", \"SearchPhrase\", count(*) FROM t GROUP BY \"UserID\", \"SearchPhrase\" LIMIT 10".into()
    });
    let iters: usize = std::env::var("CB_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    let pool_bytes: usize = std::env::var("CB_POOL_GB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(40)
        * 1024
        * 1024
        * 1024;

    // Arm A runs the CLI's own SQL against the CLI's `hits` view; arm B needs the same query
    // against `t`, the table name our substrait plan is produced for.
    let dir_for_ddl = dir.clone();
    let sql_cli = sql.replace(" FROM t ", " FROM hits ").replace("FROM t\n", "FROM hits\n");
    println!(
        "dir  = {dir}\nsql(B)= {sql}\nsql(A)= {sql_cli}\niters= {iters}  pool={} GB",
        pool_bytes >> 30
    );

    let mgr = RuntimeManager::new(8, 1.5, 1.5);

    // Build the substrait plan once, exactly as the node does.
    let (plan_bytes, metas) = mgr.io_runtime.block_on(async {
        let (ctx, _schema) = register(&dir).await;
        let lp = ctx.sql(&sql).await.unwrap().logical_plan().clone();
        let sub = to_substrait_plan(&lp, &ctx.state()).unwrap();
        let mut buf = Vec::new();
        sub.encode(&mut buf).unwrap();

        let store = Arc::new(LocalFileSystem::new());
        let url = ListingTableUrl::parse(&dir).unwrap();
        let mut list = store.list(Some(url.prefix()));
        let mut v = Vec::new();
        while let Some(m) = list.try_next().await.unwrap() {
            if m.location.as_ref().ends_with(".parquet") {
                v.push(m);
            }
        }
        (buf, Arc::new(v))
    });
    println!("files= {}\n", metas.len());

    // ── Arm A: plain DataFusion, plain GreedyMemoryPool (the datafusion-cli shape) ──
    println!("{:<38} {:>9} {:>12}", "arm", "wall_ms", "minor_faults");
    // Build the session ONCE, like datafusion-cli does for a whole -f script, then only re-run
    // the query per iteration. Rebuilding the session per iteration re-infers the schema and
    // re-lists files, which is setup cost datafusion-cli does not pay per query.
    let plain_url = ListingTableUrl::parse(&dir).unwrap();
    let plain_ctx = mgr.io_runtime.block_on(async {
        let rt = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_bytes)))
            .with_disk_manager_builder(DiskManagerBuilder::default())
            .build()
            .unwrap();
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_runtime_env(Arc::new(rt))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_like_cli(&ctx, &dir_for_ddl).await;
        ctx
    });
    for i in 0..iters {
        let t = std::time::Instant::now();
        let f0 = minor_faults();
        mgr.io_runtime.block_on(async {
            let df = plain_ctx.sql(&sql_cli).await.unwrap();
            let _ = df.collect().await.unwrap();
        });
        let faults = minor_faults().saturating_sub(f0);
        if i >= 2 {
            println!(
                "{:<38} {:>9.1} {:>12}",
                "A plain DF, reused session",
                t.elapsed().as_secs_f64() * 1000.0,
                faults
            );
        }
    }

    // ── Arm B: our execute_query path (tracking pool + budget + guard + optimizers) ──
    // Two sub-arms differing ONLY in `query_memory_pool`: our per-query tracking pool overlay
    // vs None (falls through to the shared global pool, i.e. what plain DataFusion does). Same
    // code path otherwise, so a fault delta here indicts the pool overlay specifically.
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_bytes)))
        .with_disk_manager_builder(DiskManagerBuilder::default())
        .build()
        .unwrap();
    let df_runtime = DataFusionRuntime::new_for_bench(runtime_env);
    let our_url = ListingTableUrl::parse(&dir).unwrap();
    for (label, use_tracking_pool) in [
        ("B1 our path, shared pool (None)", false),
        ("B2 our path, per-query tracking pool", true),
    ] {
    for i in 0..iters {
        let t = std::time::Instant::now();
        let f0 = minor_faults();
        mgr.io_runtime.block_on(async {
            let qpool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>> =
                if use_tracking_pool {
                    Some(Arc::new(GreedyMemoryPool::new(pool_bytes))
                        as Arc<dyn datafusion::execution::memory_pool::MemoryPool>)
                } else {
                    None
                };
            let ptr = query_executor::execute_query(
                our_url.clone(),
                metas.clone(),
                "t".into(),
                plan_bytes.clone(),
                &df_runtime,
                mgr.cpu_executor(),
                qpool,
                &DatafusionQueryConfig::test_default(),
                0,
                Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>,
                None,
                &[],
                &[],
                opensearch_datafusion::datafusion_query_config::InternalSearch::Off,
            )
            .await
            .unwrap();
            let mut stream = unsafe {
                Box::from_raw(
                    ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                        std::pin::Pin<
                            Box<
                                dyn futures::Stream<
                                        Item = datafusion::error::Result<
                                            arrow_array::RecordBatch,
                                        >,
                                    > + Send,
                            >,
                        >,
                    >,
                )
            };
            use futures::StreamExt;
            while let Some(b) = stream.next().await {
                let _ = b.unwrap();
            }
        });
        let faults = minor_faults().saturating_sub(f0);
        if i >= 2 {
            println!(
                "{:<38} {:>9.1} {:>12}",
                label,
                t.elapsed().as_secs_f64() * 1000.0,
                faults
            );
        }
    }
    }

    drop(tmp_guard);
}
