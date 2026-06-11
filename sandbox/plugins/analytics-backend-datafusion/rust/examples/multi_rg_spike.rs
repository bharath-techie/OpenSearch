// A1 measurement spike — isolate the per-RG `DataSourceExec` setup tax.
//
// Reads a real parquet file two ways and times first-batch latency + total:
//   APPROACH 1 (today): N separate DataSourceExec, one RG each, looped serially
//                       — mirrors stream.rs calling create_full_scan_stream per RG.
//   APPROACH 2 (consolidated): ONE DataSourceExec over all RGs via a single
//                       multi-RG ParquetAccessPlan.
//
// Both full-scan (read every row, no predicate, no refinement) so the ONLY
// variable is per-RG setup + IO pipelining. If approach 2's time-to-first-batch
// and total collapse vs approach 1, the A1 lever is confirmed and the real
// (correctness-preserving) rewrite is worth doing.
//
// Run:
//   cd sandbox/libs/dataformat-native/rust
//   cargo run --release -p opensearch-datafusion --example multi_rg_spike -- /path/to/hits_0.parquet

use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::cache::DefaultFilesMetadataCache;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use opensearch_datafusion::indexed_table::parquet_bridge::{
    self, load_parquet_metadata, RowGroupStreamConfig,
};

/// Re-encode `src` into a sibling file with `rg_size` rows per row group, so
/// the spike can reproduce the production "many small RGs" shape (q08 had ~110).
/// Returns the new path. Streams batches through to bound memory.
fn reencode_with_rg_size(src: &str, rg_size: usize) -> String {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;

    let dst = format!("{src}.rg{rg_size}.parquet");
    if std::path::Path::new(&dst).exists() {
        return dst;
    }
    let in_file = File::open(src).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(in_file)
        .unwrap()
        .with_batch_size(rg_size);
    let schema = builder.schema().clone();
    let reader = builder.build().unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_size(rg_size)
        .build();
    let out_file = File::create(&dst).unwrap();
    let mut writer = ArrowWriter::try_new(out_file, schema, Some(props)).unwrap();
    for batch in reader {
        writer.write(&batch.unwrap()).unwrap();
    }
    writer.close().unwrap();
    dst
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let src = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/Users/gbh/Downloads/hits_0.parquet".to_string());
    let runs: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    // RG size: production q08 had ~110 RGs over the segment. Default ~9091
    // rows/RG over 1M rows ≈ 110 RGs. Override via arg 3.
    let rg_size: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(9091);
    // Arg 4: "perrg" or "multi" runs ONLY that approach (for cold-cache A/B
    // across separate processes on distinct files). Default: both, interleaved.
    let only = std::env::args().nth(4).unwrap_or_default();
    let do_perrg = only.is_empty() || only == "perrg";
    let do_multi = only.is_empty() || only == "multi";

    // If the source already has multiple row groups (real production segment),
    // use it as-is. Only re-encode single-RG files (e.g. the raw hits_0.parquet).
    let path = {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let f = std::fs::File::open(&src).unwrap();
        let n = SerializedFileReader::new(f)
            .unwrap()
            .metadata()
            .num_row_groups();
        if n > 1 {
            println!("source has {n} row groups — using as-is (no re-encode)");
            src.clone()
        } else {
            println!("re-encoding {src} at rg_size={rg_size} ...");
            reencode_with_rg_size(&src, rg_size)
        }
    };

    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let store_url = ObjectStoreUrl::parse("file://").unwrap();
    let location = object_store::path::Path::from_filesystem_path(&path).unwrap();
    let metadata_cache: Arc<dyn FileMetadataCache> =
        Arc::new(DefaultFilesMetadataCache::new(256 * 1024 * 1024));

    let (full_schema, file_size, metadata) =
        load_parquet_metadata(Arc::clone(&store), &location, Arc::clone(&metadata_cache))
            .await
            .expect("load metadata");

    let num_rgs = metadata.num_row_groups();
    let total_rows: i64 = (0..num_rgs)
        .map(|i| metadata.row_group(i).num_rows())
        .sum();
    println!(
        "file={}  size={:.1}MB  row_groups={}  total_rows={}",
        path,
        file_size as f64 / 1e6,
        num_rgs,
        total_rows
    );

    // Project a single column so decode cost is small and constant across both
    // approaches — we want setup/IO to dominate, matching the q08 narrow shape.
    let projection = Some(vec![0usize]);
    let cfg = || RowGroupStreamConfig {
        file_path: location.to_string(),
        file_size,
        store: Arc::clone(&store),
        store_url: store_url.clone(),
        full_schema: full_schema.clone(),
        metadata: Arc::clone(&metadata),
        projection: projection.clone(),
        predicate: None,
    };
    let all_rgs: Vec<usize> = (0..num_rgs).collect();

    println!("\n{:>10} {:>14} {:>14} {:>12}", "approach", "first_batch_ms", "total_ms", "rows");
    for r in 0..runs {
        // ---- APPROACH 1: per-RG DataSourceExec, looped ----
        if do_perrg {
            let config = cfg();
            let t0 = Instant::now();
            let mut first_batch_ms = f64::NAN;
            let mut rows = 0usize;
            let mut first_seen = false;
            for &rg in &all_rgs {
                let (mut stream, _plan) = parquet_bridge::create_full_scan_stream(&config, rg)
                    .expect("per-rg stream");
                while let Some(b) = stream.next().await {
                    let b = b.expect("batch");
                    if !first_seen {
                        first_batch_ms = t0.elapsed().as_secs_f64() * 1e3;
                        first_seen = true;
                    }
                    rows += b.num_rows();
                }
            }
            let total_ms = t0.elapsed().as_secs_f64() * 1e3;
            println!(
                "{:>10} {:>14.1} {:>14.1} {:>12}",
                format!("per-rg#{r}"),
                first_batch_ms,
                total_ms,
                rows
            );
        }

        // ---- APPROACH 2: ONE DataSourceExec over all RGs ----
        if do_multi {
            let config = cfg();
            let t0 = Instant::now();
            let mut first_batch_ms = f64::NAN;
            let mut rows = 0usize;
            let mut first_seen = false;
            let (mut stream, _plan) =
                parquet_bridge::create_multi_rg_full_scan_stream(&config, &all_rgs)
                    .expect("multi-rg stream");
            while let Some(b) = stream.next().await {
                let b = b.expect("batch");
                if !first_seen {
                    first_batch_ms = t0.elapsed().as_secs_f64() * 1e3;
                    first_seen = true;
                }
                rows += b.num_rows();
            }
            let total_ms = t0.elapsed().as_secs_f64() * 1e3;
            println!(
                "{:>10} {:>14.1} {:>14.1} {:>12}",
                format!("multi#{r}"),
                first_batch_ms,
                total_ms,
                rows
            );
        }
    }
}
