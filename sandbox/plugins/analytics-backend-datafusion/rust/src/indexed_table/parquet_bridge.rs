/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! DataFusion parquet bridge — isolates ALL DataFusion parquet-specific API calls.
//!
//! Everything that touches `ParquetSource`, `FileScanConfigBuilder`,
//! `DataSourceExec`, `ParquetAccessPlan`, `RowGroupAccess::Selection/Scan`,
//! `ParquetFileReaderFactory`, `ArrowReaderMetadata`, `ArrowReaderOptions`
//! lives here. `stream.rs` only uses this module's public API.
//!
//! All I/O goes through the caller-supplied `object_store::ObjectStore`. No
//! direct `LocalFileSystem` / `std::fs` usage — that was the PR #21164 version's
//! design and it was reworked here so the indexed path respects the same store
//! the vanilla path uses (file://, s3://, etc.).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::metadata::{
    CachedParquetMetaData, DFParquetMetadata,
};
use datafusion::datasource::physical_plan::parquet::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory, RowGroupAccess,
};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::cache::cache_manager::{CachedFileMetadataEntry, FileMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::{ObjectStore, ObjectStoreExt};
use prost::bytes::Bytes;

// ── Parquet Metadata Loading ─────────────────────────────────────────

/// Load **footer-only** parquet metadata (row-group + file stats, no page index),
/// managing the `FileMetadataCache` ourselves so the page index is **never
/// decoded**.
///
/// Why we don't just hand the cache to `DFParquetMetadata::fetch_metadata`: in
/// DataFusion 54 that method hardcodes the page-index policy — when a cache is
/// present it uses `PageIndexPolicy::Optional` and force-decodes the FULL page
/// index (ColumnIndex + OffsetIndex, every column, every row group) before
/// caching, and only uses `Skip` when NO cache is passed
/// (`datafusion-datasource-parquet/src/metadata.rs`). Stripping on `put` would
/// discard that index but the expensive decode already happened. On wide schemas
/// that decode is the dominant cost we're trying to avoid.
///
/// So this fn: consults the cache directly; on a valid hit returns the cached
/// (footer-only) metadata with zero I/O; on a miss it fetches **without** a cache
/// (→ `Skip` policy → the page index is never decoded) and puts the footer-only
/// entry itself. The scoped page index is built separately, per query, only for
/// the predicate columns (see [`super::page_index_loader`]).
///
/// `MutexFileMetadataCache::put` still strips defensively as a backstop for the
/// scan paths DataFusion drives directly (the opener / `infer_schema`), but this
/// loader makes the warm path and both scoped-reader paths skip the decode
/// entirely.
pub async fn load_parquet_metadata(
    store: Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    metadata_cache: Arc<dyn FileMetadataCache>,
) -> std::result::Result<(SchemaRef, u64, Arc<ParquetMetaData>), String> {
    let meta = store
        .head(location)
        .await
        .map_err(|e| format!("object-store head {}: {}", location, e))?;
    let size = meta.size;

    // Cache hit (validated against current size/last_modified) → no I/O, no decode.
    if let Some(cached) = metadata_cache.get(location) {
        if cached.is_valid_for(&meta) {
            if let Some(cp) = cached
                .file_metadata
                .as_any()
                .downcast_ref::<CachedParquetMetaData>()
            {
                let pq_meta = Arc::clone(cp.parquet_metadata());
                let file_meta = pq_meta.file_metadata();
                let schema =
                    parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
                        .map_err(|e| format!("parquet_to_arrow_schema {}: {}", location, e))?;
                return Ok((Arc::new(schema), size, pq_meta));
            }
        }
    }

    // Miss → fetch WITHOUT a cache so DataFusion uses PageIndexPolicy::Skip and
    // never decodes the page index.
    let pq_meta = DFParquetMetadata::new(&*store, &meta)
        .fetch_metadata()
        .await
        .map_err(|e| format!("load parquet metadata {}: {}", location, e))?;

    // Publish the footer-only entry to the shared cache ourselves.
    metadata_cache.put(
        location,
        CachedFileMetadataEntry::new(
            meta.clone(),
            Arc::new(CachedParquetMetaData::new(Arc::clone(&pq_meta))),
        ),
    );

    let file_meta = pq_meta.file_metadata();
    let schema = parquet_to_arrow_schema(file_meta.schema_descr(), file_meta.key_value_metadata())
        .map_err(|e| format!("parquet_to_arrow_schema {}: {}", location, e))?;

    Ok((Arc::new(schema), size, pq_meta))
}

/// Shared accumulator for object-store read wall-time.
#[derive(Debug, Default)]
pub struct ReadIoStats {
    pub total_ns: AtomicU64,
    pub count: AtomicU64,
}

fn record_io(stats: &ReadIoStats, dur: Duration) {
    let ns = dur.as_nanos() as u64;
    stats.total_ns.fetch_add(ns, Ordering::Relaxed);
    stats.count.fetch_add(1, Ordering::Relaxed);
}

/// Configuration for creating a per-row-group parquet stream.
pub struct RowGroupStreamConfig {
    /// Object-store-relative path to the parquet file.
    pub file_path: String,
    pub file_size: u64,
    /// Object store the file lives in (resolved from the session's RuntimeEnv).
    pub store: Arc<dyn ObjectStore>,
    /// URL of the store for DataFusion's `FileScanConfig`.
    pub store_url: ObjectStoreUrl,
    pub full_schema: SchemaRef,
    pub metadata: Arc<ParquetMetaData>,
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    pub io_stats: Arc<ReadIoStats>,
}

/// Create a stream that reads a single row group using `RowSelection`.
///
/// Predicate pushdown IS safe here — `RowSelection` is applied during decode,
/// so the predicate sees only selected rows and indices stay aligned.
pub fn create_row_selection_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
    selection: RowSelection,
    push_predicate: bool,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    access_plan.set(rg_index, RowGroupAccess::Selection(selection));
    create_stream_with_access_plan(config, access_plan, push_predicate)
}

/// Create a stream that reads a single row group with full scan.
///
/// Predicate pushdown is NOT safe here — caller applies a `BooleanMask` AFTER
/// decode, so pushdown during decode would cause mask offset misalignment.
pub fn create_full_scan_stream(
    config: &RowGroupStreamConfig,
    rg_index: usize,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let num_rgs = config.metadata.num_row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(num_rgs);
    // TODO(page-boundary-selection): replace `Scan` with a `Selection` built
    // from the caller's candidate bitmap at page boundaries. The idea:
    //   - Read the RG's `offset_index` to get per-page row counts.
    //   - For each page, select if any candidate bit falls within its row
    //     range, else skip.
    //   - Pass the resulting `RowSelection` via
    //     `RowGroupAccess::Selection(selection)`.
    // This keeps the selector Vec small (O(pages), not O(rows)) regardless of
    // candidate density, while letting parquet skip whole pages whose row
    // ranges are entirely outside the candidate set. Bigger I/O savings than
    // today's full-scan for dense-but-clustered matches, and cheap to build
    // for any selectivity — unifying today's split between `RowSelection`
    // strategy (<3%) and `BooleanMask` strategy (≥3%).
    //
    // Before implementing, verify parquet-rs's `Selection` delivery
    // semantics (does it deliver contiguous packed rows or original-position
    // rows with gaps?) so the caller's post-decode mask alignment stays
    // correct. Documented in `pr-reviews/EVALUATOR_HANDOFF.md`.
    access_plan.set(rg_index, RowGroupAccess::Scan);
    create_stream_with_access_plan(config, access_plan, false)
}

fn create_stream_with_access_plan(
    config: &RowGroupStreamConfig,
    access_plan: ParquetAccessPlan,
    push_predicate: bool,
) -> Result<(SendableRecordBatchStream, Arc<dyn ExecutionPlan>)> {
    let partitioned_file = PartitionedFile::new(config.file_path.clone(), config.file_size)
        .with_extensions(Arc::new(access_plan));

    let reader_factory = Arc::new(CachedMetadataReaderFactory::new(
        Arc::clone(&config.store),
        Arc::clone(&config.metadata),
        Arc::clone(&config.io_stats),
    )) as Arc<dyn ParquetFileReaderFactory>;

    let mut parquet_source = ParquetSource::new(config.full_schema.clone())
        .with_parquet_file_reader_factory(reader_factory)
        // cannot use page index because we have collector bitset matches that are not visible
        // with just parquet predicates
        .with_enable_page_index(false);

    if push_predicate {
        if let Some(ref pred) = config.predicate {
            parquet_source = parquet_source
                .with_predicate(Arc::clone(pred))
                .with_pushdown_filters(true)
                .with_reorder_filters(true);
        }
    }

    let mut config_builder =
        FileScanConfigBuilder::new(config.store_url.clone(), Arc::new(parquet_source))
            .with_file(partitioned_file);

    if let Some(ref proj) = config.projection {
        // Empty projection (e.g. COUNT(*)) is honoured as "read no
        // columns". Parquet delivers correct row counts via the
        // access plan but skips all column I/O.
        config_builder = config_builder.with_projection_indices(Some(proj.clone()))?;
    }

    let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config_builder.build());
    let ctx = Arc::new(datafusion::execution::TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    Ok((stream, exec))
}

/// Factory that creates parquet readers with pre-cached metadata.
///
/// Avoids re-reading metadata for each row group.
#[derive(Debug)]
pub struct CachedMetadataReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata: Arc<ParquetMetaData>,
    io_stats: Arc<ReadIoStats>,
}

impl CachedMetadataReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata: Arc<ParquetMetaData>,
        io_stats: Arc<ReadIoStats>,
    ) -> Self {
        Self { store, metadata, io_stats }
    }
}

impl ParquetFileReaderFactory for CachedMetadataReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file.object_meta.location.as_ref(), metrics);
        Ok(Box::new(CachedMetadataReader {
            store: Arc::clone(&self.store),
            location: file.object_meta.location.clone(),
            metadata: Arc::clone(&self.metadata),
            metrics: file_metrics,
            io_stats: Arc::clone(&self.io_stats),
        }))
    }
}

struct CachedMetadataReader {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
    metrics: ParquetFileMetrics,
    io_stats: Arc<ReadIoStats>,
}

impl AsyncFileReader for CachedMetadataReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let io_stats = Arc::clone(&self.io_stats);
        async move {
            let t0 = Instant::now();
            let r = store
                .get_range(&location, range)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)));
            record_io(&io_stats, t0.elapsed());
            r
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.metrics.bytes_scanned.add(total as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        let io_stats = Arc::clone(&self.io_stats);
        async move {
            let t0 = Instant::now();
            let r = store
                .get_ranges(&location, &ranges)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)));
            record_io(&io_stats, t0.elapsed());
            r
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata = Arc::clone(&self.metadata);
        async move { Ok(metadata) }.boxed()
    }
}

#[cfg(test)]
mod schema_evolution_tests {
    //! Reproduces the indexed-path schema-evolution residual bug: when the
    //! physical parquet file's column layout differs from the union/table
    //! schema (a column at a different leaf position than its union index),
    //! a residual predicate referencing the column by its UNION index must
    //! still read the correct leaf. The vanilla listing path does this via
    //! per-file schema adaptation; this test checks the indexed bridge does
    //! the same (and FAILS today, dropping rows / reading the wrong leaf).

    use super::*;
    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::logical_expr::Operator;
    use datafusion::scalar::ScalarValue;
    use futures::StreamExt;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// Write a parquet file whose PHYSICAL schema is `physical_schema`.
    fn write_parquet(physical_schema: SchemaRef, columns: Vec<Arc<dyn datafusion::arrow::array::Array>>) -> NamedTempFile {
        let batch = RecordBatch::try_new(physical_schema.clone(), columns).unwrap();
        let tmp = NamedTempFile::new().unwrap();
        let props = datafusion::parquet::file::properties::WriterProperties::builder()
            .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::Page)
            .build();
        let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), physical_schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        tmp
    }

    /// The bug: residual on a numeric column that sits at a DIFFERENT leaf in
    /// the physical file than its position in the union/table schema. With the
    /// union schema fed to `ParquetSource` and the predicate referencing the
    /// union index, reading must still hit the right physical leaf.
    ///
    /// Physical file columns: `[brand:Utf8, sev:Int32]`  (sev = leaf 1)
    /// Union/table schema:     `[brand:Utf8, inserted:Int32, sev:Int32]` (sev = union index 2)
    /// Residual: `sev >= 0` → references union index 2. Every row has sev>=0,
    /// so NO row may be dropped. If the path reads union-leaf 2 against a file
    /// with only 2 leaves (0,1), it mis-reads / nulls → drops rows.
    #[tokio::test]
    async fn residual_on_shifted_leaf_reads_correct_column() {
        // Physical schema (what's actually in the file): brand, sev.
        let physical_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("brand", DataType::Utf8, true),
            Field::new("sev", DataType::Int32, true),
        ]));
        let brands = StringArray::from(vec!["a", "b", "c", "d"]);
        let sevs = Int32Array::from(vec![0, 17, 9, 21]); // all >= 0
        let tmp = write_parquet(
            physical_schema.clone(),
            vec![Arc::new(brands), Arc::new(sevs)],
        );
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();

        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
        let parquet_meta = meta.metadata().clone();

        // Union/table schema: an extra column `inserted` BEFORE sev, so sev is
        // at union index 2 (but physical leaf 1). This simulates schema
        // evolution / merged files where the column position shifted.
        let union_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("brand", DataType::Utf8, true),
            Field::new("inserted", DataType::Int32, true),
            Field::new("sev", DataType::Int32, true),
        ]));

        // Residual `sev >= 0`, with the Column referencing the UNION index (2),
        // exactly as the indexed path builds it (bound to the union DFSchema).
        let residual: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("sev", 2)),
            Operator::GtEq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));

        // Projection: read brand (0) and sev (2) by union index.
        let projection = vec![0usize, 2usize];

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let store_url = ObjectStoreUrl::local_filesystem();
        let config = RowGroupStreamConfig {
            file_path: path.to_string_lossy().to_string(),
            file_size: size,
            store,
            store_url,
            full_schema: union_schema,
            metadata: Arc::clone(&parquet_meta),
            projection: Some(projection),
            predicate: Some(residual),
            io_stats: Arc::new(ReadIoStats::default()),
        };

        // Row-granular pushdown ON (push_predicate=true) — the cluster's
        // low-selectivity regime that pushes the residual into parquet decode.
        let (mut stream, _exec) =
            create_row_selection_stream(&config, 0, full_rg_selection(&parquet_meta, 0), true)
                .expect("stream builds");

        let mut total_rows = 0usize;
        while let Some(batch) = stream.next().await {
            let b = batch.expect("batch decodes without error");
            total_rows += b.num_rows();
        }

        assert_eq!(
            total_rows, 4,
            "all 4 rows have sev>=0 and must survive the residual; got {total_rows} \
             (indexed path read the wrong leaf for the shifted column)"
        );
    }

    /// Variant: the residual column is ABSENT from the physical file entirely
    /// (pure schema evolution — column added after this file was written). The
    /// listing path null-fills it; `sev >= 0` over null → null → row excluded,
    /// which is CORRECT. But `sev IS NULL` must keep all rows. This pins whether
    /// the indexed path null-fills absent columns the same way the listing path
    /// does, rather than erroring or mis-reading.
    #[tokio::test]
    async fn residual_on_absent_column_null_fills() {
        // Physical file has ONLY brand — no sev column at all.
        let physical_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("brand", DataType::Utf8, true),
        ]));
        let brands = StringArray::from(vec!["a", "b", "c", "d"]);
        let tmp = write_parquet(physical_schema.clone(), vec![Arc::new(brands)]);
        let path = tmp.path().to_path_buf();
        let size = std::fs::metadata(&path).unwrap().len();

        let file = std::fs::File::open(&path).unwrap();
        let meta =
            ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
        let parquet_meta = meta.metadata().clone();

        // Union schema adds `sev` at index 1 — absent from the physical file.
        let union_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("brand", DataType::Utf8, true),
            Field::new("sev", DataType::Int32, true),
        ]));

        // Residual `sev IS NOT NULL` — over a null-filled absent column this is
        // FALSE for all rows → 0 rows. That's correct. The point of the test is
        // it must NOT error and must agree with listing-path null semantics.
        // We assert the dual: `sev IS NULL` keeps all 4 rows.
        use datafusion::physical_expr::expressions::IsNullExpr;
        let residual: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(IsNullExpr::new(Arc::new(Column::new("sev", 1))));

        let projection = vec![0usize, 1usize];
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let store_url = ObjectStoreUrl::local_filesystem();
        let config = RowGroupStreamConfig {
            file_path: path.to_string_lossy().to_string(),
            file_size: size,
            store,
            store_url,
            full_schema: union_schema,
            metadata: Arc::clone(&parquet_meta),
            projection: Some(projection),
            predicate: Some(residual),
            io_stats: Arc::new(ReadIoStats::default()),
        };

        let (mut stream, _exec) =
            create_row_selection_stream(&config, 0, full_rg_selection(&parquet_meta, 0), true)
                .expect("stream builds");
        let mut total_rows = 0usize;
        while let Some(batch) = stream.next().await {
            let b = batch.expect("batch decodes without error");
            total_rows += b.num_rows();
        }
        assert_eq!(
            total_rows, 4,
            "sev IS NULL over an absent (null-filled) column must keep all 4 rows; got {total_rows}"
        );
    }

    /// Build a RowSelection that selects every row of `rg_index`.
    fn full_rg_selection(meta: &ParquetMetaData, rg_index: usize) -> RowSelection {
        use datafusion::parquet::arrow::arrow_reader::RowSelector;
        let n = meta.row_group(rg_index).num_rows() as usize;
        RowSelection::from(vec![RowSelector::select(n)])
    }
}
