/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The `ExecutionPlan` node for an indexed parquet read.
//!
//! [`IndexedExec`] covers one segment chunk — one or more row groups of a single
//! parquet file — and holds nothing but the inputs the scan needs. All streaming
//! is DataFusion's: `execute` hands those inputs to
//! [`build_chunk_stream`](super::chunk_stream::build_chunk_stream), which plugs
//! the indexed evaluator into DataFusion's own parquet decoder through a
//! `RowGroupAccessProvider` (which rows of each row group are candidates) and,
//! for evaluators that refine during decode, an `ArrowPredicateFactory`.
//!
//! There is no OpenSearch-side driver: row-group sequencing, byte scheduling,
//! prefetch overlap, projection and limits all live on the DataFusion side of
//! those two interfaces. See [`super::chunk_stream`] for what remains after
//! the decoder — `__row_id__` rebasing and post-decode refinement.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;

use super::eval::RowGroupBitsetSource;
use super::metrics::StreamMetrics;
use crate::datafusion_query_config::DatafusionQueryConfig;

/// Row group metadata.
#[derive(Debug, Clone)]
pub struct RowGroupInfo {
    pub index: usize,
    pub first_row: i64,
    pub num_rows: i64,
}

// ── IndexedExec ──────────────────────────────────────────────────────

/// Execution plan for a single segment chunk (1+ row groups from one segment).
/// One DataFusion parquet scan covers the whole chunk, with each row group's
/// index evaluation overlapping the decode of the previous one.
pub struct IndexedExec {
    pub(crate) schema: SchemaRef,
    pub(crate) full_schema: SchemaRef,
    pub(crate) object_path: object_store::path::Path,
    pub(crate) file_size: u64,
    pub(crate) store: Arc<dyn object_store::ObjectStore>,
    pub(crate) store_url: datafusion::execution::object_store::ObjectStoreUrl,
    pub(crate) row_groups: Vec<RowGroupInfo>,
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) properties: Arc<PlanProperties>,
    pub(crate) metadata: Arc<ParquetMetaData>,
    pub(crate) predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Pluggable bitset source (SingleCollector or RustTree).
    pub(crate) evaluator: std::sync::Mutex<Option<Arc<dyn RowGroupBitsetSource>>>,
    pub(crate) doc_range: Option<(i32, i32)>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) stream_metrics: StreamMetrics,
    /// Query-scoped tunables. Shared by Arc across IndexedExec instances from the
    /// same query; the relevant values are read out once in `execute` so the
    /// per-batch path never touches the Arc.
    pub(crate) query_config: Arc<DatafusionQueryConfig>,
    /// Cumulative row offset for this segment within the shard.
    pub(crate) global_base: u64,
    /// Index in the OUTPUT schema where the decoded `___row_id` values should be
    /// rebased to shard-global. `None` when row ids were not requested or
    /// `___row_id` is not in the projection.
    pub(crate) row_id_output_index: Option<usize>,
    /// Optional runtime dynamic filter (TopK / join) accepted via physical
    /// pushdown, referencing the sort columns. Used to prune row groups whose
    /// parquet statistics cannot satisfy the (tightening) predicate. `None`
    /// when no dynamic filter was pushed to this query.
    pub(crate) dynamic_filter: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Per-query cancellation token. When cancelled, the access provider stops
    /// dispatching further row-group evaluations and the stream stops draining.
    /// `None` disables cancellation checks.
    pub(crate) cancellation_token: Option<tokio_util::sync::CancellationToken>,
}

impl fmt::Debug for IndexedExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedExec")
            .field("row_groups", &self.row_groups.len())
            .field("has_predicate", &self.predicate.is_some())
            .finish()
    }
}

impl DisplayAs for IndexedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let total_rows: i64 = self.row_groups.iter().map(|rg| rg.num_rows).sum();
        let doc_range_str = match self.doc_range {
            Some((min, max)) => format!(", doc_range=[{}, {})", min, max),
            None => String::new(),
        };
        write!(
            f,
            "IndexedExec: rg={}, total_rows={}, predicate={}{}",
            self.row_groups.len(),
            total_rows,
            self.predicate.is_some(),
            doc_range_str,
        )
    }
}

impl ExecutionPlan for IndexedExec {
    fn name(&self) -> &str {
        "IndexedExec"
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let evaluator = {
            let mut guard = self.evaluator.lock().unwrap();
            guard
                .take()
                .ok_or_else(|| DataFusionError::Internal("evaluator already consumed".into()))?
        };

        super::chunk_stream::build_chunk_stream(super::chunk_stream::ChunkStreamArgs {
            schema: self.schema.clone(),
            full_schema: self.full_schema.clone(),
            projection: self.projection.clone(),
            object_path: self.object_path.clone(),
            file_size: self.file_size,
            store: Arc::clone(&self.store),
            store_url: self.store_url.clone(),
            metadata: Arc::clone(&self.metadata),
            predicate: self.predicate.clone(),
            evaluator,
            row_groups: self.row_groups.clone(),
            doc_range: self.doc_range,
            metrics: self.stream_metrics.clone(),
            indexed_pushdown_filters: self.query_config.indexed_pushdown_filters,
            batch_size: self.query_config.batch_size,
            granularity: super::access_provider::SelectionGranularity::from_config(
                &self.query_config,
            ),
            decode_time_refinement: self.query_config.indexed_decode_time_refinement,
            global_base: self.global_base,
            row_id_output_index: self.row_id_output_index,
            dynamic_filter: self.dynamic_filter.clone(),
            cancellation_token: self.cancellation_token.clone(),
        })
    }
}
