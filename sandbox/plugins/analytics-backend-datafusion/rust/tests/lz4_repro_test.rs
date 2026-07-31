// Standalone repro for the LZ4_RAW "provided output is too small" failure seen on
// the indexed path. Drives a REAL clickbench parquet file through the same two
// DataFusion interfaces the indexed scan uses (RowGroupAccessProvider +
// ArrowPredicateFactory) so the decoder rebuild / frontier reconciliation runs
// against real multi-row-group LZ4 data.
//
// Ignored by default because it needs the local data dir.
// Run with:
//   cargo test -p opensearch-datafusion --test lz4_repro_test -- --ignored --nocapture

use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::{
    ArrowPredicateFactory, RowGroupAccessProvider, RowGroupAccessProviderFactory,
};
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowPredicate, RowSelection, RowSelector,
};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt};

const DATA_DIR: &str =
    "/Users/gbh/Documents/data/nodes/0/indices/euv-GKLFSeSkrvwjOQZ9jQ/0/parquet";

/// Provider that mimics the indexed evaluator: narrows every row group to a
/// sparse selection (as a delegation bitset would) so `rebuild_decoder` runs
/// with real selections at every boundary.
#[derive(Debug)]
struct NarrowingProvider {
    /// Keep every Nth row; 0 means "Scan whole row group".
    stride: usize,
    /// Skip row groups whose index is in here.
    skip: Vec<usize>,
    calls: std::sync::Mutex<Vec<Vec<usize>>>,
}

#[async_trait::async_trait]
impl RowGroupAccessProvider for NarrowingProvider {
    async fn access_for(
        &mut self,
        pending: &[usize],
        metadata: &ParquetMetaData,
    ) -> datafusion::common::Result<Vec<RowGroupAccess>> {
        self.calls.lock().unwrap().push(pending.to_vec());
        // Answer only for the head, like the indexed provider does when it
        // evaluates one row group at a time.
        let Some(&head) = pending.first() else {
            return Ok(vec![]);
        };
        if self.skip.contains(&head) {
            return Ok(vec![RowGroupAccess::Skip]);
        }
        if self.stride == 0 {
            return Ok(vec![RowGroupAccess::Scan]);
        }
        let rows = metadata.row_group(head).num_rows() as usize;
        let mut selectors: Vec<RowSelector> = Vec::new();
        let mut pos = 0;
        while pos < rows {
            let take = 1.min(rows - pos);
            selectors.push(RowSelector::select(take));
            pos += take;
            let skip = self.stride.min(rows.saturating_sub(pos));
            if skip > 0 {
                selectors.push(RowSelector::skip(skip));
                pos += skip;
            }
        }
        Ok(vec![RowGroupAccess::Selection(RowSelection::from(
            selectors,
        ))])
    }
}

#[derive(Debug)]
struct NarrowingProviderFactory {
    stride: usize,
    skip: Vec<usize>,
}

impl RowGroupAccessProviderFactory for NarrowingProviderFactory {
    fn create(
        &self,
        _file: &PartitionedFile,
        _metadata: &Arc<ParquetMetaData>,
    ) -> datafusion::common::Result<Option<Box<dyn RowGroupAccessProvider>>> {
        Ok(Some(Box::new(NarrowingProvider {
            stride: self.stride,
            skip: self.skip.clone(),
            calls: std::sync::Mutex::new(Vec::new()),
        })))
    }
}

/// Predicate that keeps every row, so it exercises the decode-time RowFilter
/// path without changing results.
#[derive(Debug)]
struct KeepAll {
    mask: ProjectionMask,
}

impl ArrowPredicate for KeepAll {
    fn projection(&self) -> &ProjectionMask {
        &self.mask
    }
    fn evaluate(
        &mut self,
        batch: RecordBatch,
    ) -> Result<BooleanArray, datafusion::arrow::error::ArrowError> {
        Ok(BooleanArray::from(vec![true; batch.num_rows()]))
    }
}

#[derive(Debug)]
struct KeepAllFactory;

impl ArrowPredicateFactory for KeepAllFactory {
    fn create(
        &self,
        _file: &PartitionedFile,
        metadata: &Arc<ParquetMetaData>,
    ) -> datafusion::common::Result<Vec<Box<dyn ArrowPredicate>>> {
        let mask = ProjectionMask::roots(metadata.file_metadata().schema_descr(), [0]);
        Ok(vec![Box::new(KeepAll { mask }) as Box<dyn ArrowPredicate>])
    }
}

/// Load footer metadata. `page_index` mirrors the production metadata cache,
/// which loads with `PageIndexPolicy::Optional` -- so the cached
/// `ParquetMetaData` carries an `offset_index`, which makes arrow-rs use its
/// sparse per-page fetch path rather than reading whole column chunks.
async fn load_metadata_opt(
    store: &Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
    page_index: bool,
) -> Arc<ParquetMetaData> {
    use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
    use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    let meta = store.head(path).await.unwrap();
    let mut reader = ParquetObjectReader::new(Arc::clone(store), meta.location.clone())
        .with_file_size(meta.size);
    let policy = if page_index {
        PageIndexPolicy::Required
    } else {
        PageIndexPolicy::Skip
    };
    Arc::new(
        ParquetMetaDataReader::new()
            .with_page_index_policy(policy)
            .load_and_finish(&mut reader, meta.size)
            .await
            .unwrap(),
    )
}

/// Footer-only metadata with a SCOPED page index grafted on, exactly as
/// `indexed_executor` does when `analytics.scoped_page_index` is enabled:
/// real OffsetIndex for `predicate ∪ projection ∪ {col 0}`, and a
/// whole-chunk single-page PLACEHOLDER for every other column.
async fn load_metadata_scoped(
    store: &Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
    predicate_cols: &[usize],
    projection_cols: &[usize],
) -> Arc<ParquetMetaData> {
    let footer = load_metadata_opt(store, path, false).await;
    opensearch_datafusion::cache::page_index::load_scoped_page_index_cols(
        store,
        path,
        &footer,
        predicate_cols,
        projection_cols,
    )
    .await
    .expect("scoped page index")
}

async fn load_metadata(
    store: &Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
) -> Arc<ParquetMetaData> {
    load_metadata_opt(store, path, false).await
}

/// Run a scan over `rgs` row groups with the given provider/predicate config and
/// return either the total row count or the error string.
async fn run_scan(
    file: &str,
    rgs: usize,
    stride: usize,
    skip: Vec<usize>,
    with_predicate: bool,
    columns: Vec<usize>,
) -> Result<usize, String> {
    run_scan_pi(file, rgs, stride, skip, with_predicate, columns, false).await
}

#[allow(clippy::too_many_arguments)]
async fn run_scan_pi(
    file: &str,
    rgs: usize,
    stride: usize,
    skip: Vec<usize>,
    with_predicate: bool,
    columns: Vec<usize>,
    page_index: bool,
) -> Result<usize, String> {
    run_scan_meta(file, rgs, stride, skip, with_predicate, columns, if page_index { MetaMode::FullPageIndex } else { MetaMode::NoPageIndex }).await
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum MetaMode {
    NoPageIndex,
    FullPageIndex,
    /// Scoped page index grafted the way the production indexed path does:
    /// real OffsetIndex only for the projected/predicate columns, whole-chunk
    /// placeholder pages elsewhere.
    ScopedPageIndex,
    /// Scoped page index whose column set omits a projected column, so that
    /// column is read through a whole-chunk placeholder page. This is the
    /// pre-fix production bug: `collect_plan_column_names` omitted the injected
    /// `__row_id__`, so it was read through a placeholder. The fix
    /// (`collect_scoped_projection_names` always adds `__row_id__`) moves the
    /// path into the `ScopedPageIndex` case above, which passes.
    ScopedPageIndexMissingProjection,
}

#[allow(clippy::too_many_arguments)]
async fn run_scan_meta(
    file: &str,
    rgs: usize,
    stride: usize,
    skip: Vec<usize>,
    with_predicate: bool,
    columns: Vec<usize>,
    meta_mode: MetaMode,
) -> Result<usize, String> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let path = object_store::path::Path::from_filesystem_path(format!("{DATA_DIR}/{file}"))
        .unwrap();
    let object_meta = store.head(&path).await.unwrap();
    let metadata = match meta_mode {
        MetaMode::NoPageIndex => load_metadata_opt(&store, &path, false).await,
        MetaMode::FullPageIndex => load_metadata_opt(&store, &path, true).await,
        MetaMode::ScopedPageIndex => {
            // Map arrow field indices -> parquet leaf indices. This schema is flat
            // (all primitives), so leaf order matches field order.
            load_metadata_scoped(&store, &path, &[], &columns).await
        }
        // Scope the OffsetIndex to a column set that does NOT include everything
        // the scan projects, reproducing what happens in production when the
        // projected column list handed to the scoped page-index loader misses a
        // column the scan actually reads (e.g. the injected `__row_id__`).
        MetaMode::ScopedPageIndexMissingProjection => {
            load_metadata_scoped(&store, &path, &[], &[0usize]).await
        }
    };

    let num_rgs = metadata.num_row_groups().min(rgs);
    let mut access_plan = ParquetAccessPlan::new_none(metadata.num_row_groups());
    for rg in 0..num_rgs {
        access_plan.set(rg, RowGroupAccess::Scan);
    }

    let arrow_schema = {
        use datafusion::parquet::arrow::parquet_to_arrow_schema;
        let fm = metadata.file_metadata();
        Arc::new(parquet_to_arrow_schema(fm.schema_descr(), fm.key_value_metadata()).unwrap())
    };

    let partitioned_file =
        PartitionedFile::new(path.as_ref().to_string(), object_meta.size)
            .with_extensions(Arc::new(access_plan));

    let mut source = ParquetSource::new(Arc::clone(&arrow_schema))
        .with_enable_page_index(false)
        .with_parquet_file_reader_factory(Arc::new(
            opensearch_datafusion::indexed_table::parquet_bridge::CachedMetadataReaderFactory::new(
                Arc::clone(&store),
                Arc::clone(&metadata),
                Arc::new(
                    opensearch_datafusion::indexed_table::parquet_bridge::ReadIoStats::default(),
                ),
            ),
        ));
    source = source.with_row_group_access_provider_factory(Arc::new(NarrowingProviderFactory {
        stride,
        skip,
    }));
    if with_predicate {
        source = source.with_arrow_predicate_factory(Arc::new(KeepAllFactory));
    }

    let mut builder = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(source),
    )
    .with_file(partitioned_file)
    .with_batch_size(Some(8192));
    builder = builder.with_projection_indices(Some(columns)).unwrap();

    let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(builder.build());
    let ctx = Arc::new(datafusion::execution::TaskContext::default());
    let mut stream = exec.execute(0, ctx).map_err(|e| e.to_string())?;
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(b) => rows += b.num_rows(),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn repro_lz4_output_too_small() {
    // Column indices in the arrow schema. __row_id__, WatchID and _seq_no are the
    // int64 columns whose data pages decompress to exactly 2 MiB -- the size in
    // the reported failure.
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());

    for file in [
        "_parquet_file_generation_merged_433.parquet",
        "_parquet_file_generation_merged_117.parquet",
    ] {
        let path =
            object_store::path::Path::from_filesystem_path(format!("{DATA_DIR}/{file}")).unwrap();
        let metadata = load_metadata(&store, &path).await;
        let fm = metadata.file_metadata();
        let schema =
            datafusion::parquet::arrow::parquet_to_arrow_schema(fm.schema_descr(), fm.key_value_metadata())
                .unwrap();
        let idx = |name: &str| schema.index_of(name).unwrap();
        let row_id = idx("__row_id__");
        let watch_id = idx("WatchID");
        let url = idx("URL");
        println!(
            "\n### {file}: {} RGs, __row_id__={row_id} WatchID={watch_id} URL={url}",
            metadata.num_row_groups()
        );

        let col_sets: Vec<(&str, Vec<usize>)> = vec![
            ("rowid+watchid", vec![row_id, watch_id]),
            ("rowid+url", vec![row_id, url]),
            ("rowid only", vec![row_id]),
        ];

        for (cols_name, cols) in &col_sets {
            for &(case, stride, ref skip, pred) in &[
                ("scan", 0usize, vec![], false),
                ("scan+pred", 0, vec![], true),
                ("stride7", 7, vec![], false),
                ("stride7+pred", 7, vec![], true),
                ("stride1000", 1000, vec![], false),
                ("skip1+stride7", 7, vec![1usize], false),
                ("skip1+stride7+pred", 7, vec![1usize], true),
                ("skip0+scan", 0, vec![0usize], false),
            ] {
                for mode in [
                    MetaMode::NoPageIndex,
                    MetaMode::FullPageIndex,
                    MetaMode::ScopedPageIndex,
                    MetaMode::ScopedPageIndexMissingProjection,
                ] {
                    let res = run_scan_meta(
                        file,
                        metadata.num_row_groups(),
                        stride,
                        skip.clone(),
                        pred,
                        cols.clone(),
                        mode,
                    )
                    .await;
                    let pi = match mode {
                        MetaMode::NoPageIndex => "no-pi",
                        MetaMode::FullPageIndex => "full-pi",
                        MetaMode::ScopedPageIndex => "scoped-pi",
                        MetaMode::ScopedPageIndexMissingProjection => "scoped-pi-MISSING",
                    };
                    match res {
                        Ok(rows) => println!("  PASS  {cols_name} / {case} / {pi}: {rows} rows"),
                        Err(e) => println!("  FAIL  {cols_name} / {case} / {pi}: {e}"),
                    }
                }
            }
        }
    }
}
