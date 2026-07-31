/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! One DataFusion scan per segment chunk.
//!
//! DataFusion drives the decoder across every row group in the chunk. The
//! indexed evaluator plugs in through two interfaces and nothing else:
//!
//! ```text
//!   ┌──────────── OpenSearch ────────────┐   ┌──── DataFusion ────┐
//!   │ IndexedAccessProvider              │   │ decoder frontier   │
//!   │   candidate rows per row group ────┼──▶│ byte scheduling    │
//!   │ RefinementPredicate                │   │ projection, limits │
//!   │   exact rows, during decode ───────┼──▶│ RowFilter          │
//!   └────────────────────────────────────┘   └────────────────────┘
//!                                                     │
//!            rebase __row_id__ / post-decode refine ◀─┘
//! ```
//!
//! What remains here is only what DataFusion cannot own: rebasing `__row_id__`
//! from file-relative to shard-global, and post-decode refinement for evaluators
//! that declined [`RowGroupBitsetSource::refines_during_decode`].
//!
//! This replaces the hand-written multi-row-group driver: the decoder rebuild,
//! byte-range pumping, row-group frontier reconciliation, and per-row-group
//! `DataSourceExec` construction all live in DataFusion now.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use futures::StreamExt;

use super::access_provider::{
    IndexedAccessProviderFactory, RefinementPredicateFactory, RgContextStore, SelectionGranularity,
};
use super::eval::RowGroupBitsetSource;
use super::metrics::StreamMetrics;
use super::parquet_bridge::{create_chunk_scan, ChunkScanConfig, ReadIoStats};
use super::stream::RowGroupInfo;

/// Everything needed to build one chunk scan.
pub(super) struct ChunkStreamArgs {
    pub schema: SchemaRef,
    pub full_schema: SchemaRef,
    pub projection: Option<Vec<usize>>,
    pub object_path: object_store::path::Path,
    pub file_size: u64,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub store_url: datafusion::execution::object_store::ObjectStoreUrl,
    pub metadata: Arc<datafusion::parquet::file::metadata::ParquetMetaData>,
    pub predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    pub evaluator: Arc<dyn RowGroupBitsetSource>,
    pub row_groups: Vec<RowGroupInfo>,
    pub doc_range: Option<(i32, i32)>,
    pub metrics: StreamMetrics,
    pub indexed_pushdown_filters: bool,
    pub batch_size: usize,
    pub global_base: u64,
    pub row_id_output_index: Option<usize>,
    /// Runtime dynamic filter (TopK / join) accepted via physical pushdown, when
    /// one was pushed. Row groups its tightening threshold provably excludes are
    /// skipped by the access provider before the index or the decoder sees them.
    pub dynamic_filter: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    pub cancellation_token: Option<tokio_util::sync::CancellationToken>,
    /// Per-row-group candidate-selection granularity.
    pub granularity: SelectionGranularity,
    /// Whether an evaluator that *can* refine during decode is allowed to.
    /// `false` forces the post-decode path — see [`ChunkStreamArgs`] docs on
    /// `indexed_decode_time_refinement`.
    pub decode_time_refinement: bool,
}

pub(super) fn build_chunk_stream(args: ChunkStreamArgs) -> Result<SendableRecordBatchStream> {
    // Decode-time refinement costs a second decode pass: the refinement's own
    // columns are decoded for the whole candidate set, then the projection is
    // decoded for the survivors. That pays off only when the refinement rejects
    // most candidates. When the candidate stage is already near-exact — an
    // indexed term match, typically — nothing is rejected and the extra pass is
    // pure overhead, so the flag lets the post-decode path be selected instead.
    let refines_during_decode =
        args.decode_time_refinement && args.evaluator.refines_during_decode();

    let push_predicate = push_predicate(args.indexed_pushdown_filters, args.evaluator.as_ref());

    // Coalescing short skip runs hands the decoder rows the candidate stage
    // rejected, so it is sound only if refinement drops them again — and
    // refinement identifies those rows by `__row_id__`. Both conditions are
    // required: an evaluator willing to mask cannot do so on a scan that does not
    // deliver positions.
    let granularity = if row_ids_are_delivered(&args) {
        args.granularity
    } else {
        SelectionGranularity::row_granular()
    };

    let contexts = Arc::new(RgContextStore::new());

    let access_factory = Arc::new(IndexedAccessProviderFactory::new(
        Arc::clone(&args.evaluator),
        args.row_groups.clone(),
        args.doc_range,
        Arc::clone(&contexts),
        args.metrics.clone(),
        args.dynamic_filter.clone(),
        Arc::clone(&args.full_schema),
        args.cancellation_token.clone(),
        granularity,
    ))
        as Arc<dyn datafusion::datasource::physical_plan::parquet::RowGroupAccessProviderFactory>;

    let predicate_factory = refines_during_decode.then(|| {
        Arc::new(RefinementPredicateFactory::new(
            Arc::clone(&args.evaluator),
            args.row_groups.clone(),
            Arc::clone(&contexts),
        )) as Arc<dyn datafusion::datasource::physical_plan::parquet::ArrowPredicateFactory>
    });

    let io_stats = args
        .metrics
        .io_stats
        .clone()
        .unwrap_or_else(|| Arc::new(ReadIoStats::default()));

    let scan_config = ChunkScanConfig {
        file_path: args.object_path.as_ref().to_string(),
        file_size: args.file_size,
        store: Arc::clone(&args.store),
        store_url: args.store_url.clone(),
        full_schema: Arc::clone(&args.full_schema),
        metadata: Arc::clone(&args.metadata),
        projection: args.projection.clone(),
        predicate: args.predicate.clone(),
        push_predicate,
        io_stats,
        batch_size: args.batch_size,
        row_group_indexes: args.row_groups.iter().map(|rg| rg.index).collect(),
    };

    let (stream, _exec) = create_chunk_scan(&scan_config, Some(access_factory), predicate_factory)?;

    let schema = Arc::clone(&args.schema);
    let state = FinalizeState {
        args,
        contexts,
        refines_during_decode,
        inner: stream,
    };

    let out = futures::stream::unfold(state, |mut state| async move {
        state.next_batch().await.map(|batch| (batch, state))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
}

/// Post-decode work DataFusion cannot do: refinement for evaluators that
/// declined decode-time filtering, and `__row_id__` rebasing.
struct FinalizeState {
    args: ChunkStreamArgs,
    contexts: Arc<RgContextStore>,
    refines_during_decode: bool,
    inner: SendableRecordBatchStream,
}

impl FinalizeState {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            if self
                .args
                .cancellation_token
                .as_ref()
                .is_some_and(|t| t.is_cancelled())
            {
                return Some(Err(DataFusionError::Execution("query cancelled".into())));
            }

            let batch = match self.inner.next().await? {
                Ok(batch) => batch,
                Err(e) => return Some(Err(e)),
            };
            if batch.num_rows() == 0 {
                continue;
            }
            if let Some(ref count) = self.args.metrics.parquet_batches_received {
                count.add(1);
            }

            let batch = match self.finalize(batch) {
                Ok(batch) => batch,
                Err(e) => return Some(Err(e)),
            };
            if batch.num_rows() == 0 {
                continue;
            }
            if let Some(ref count) = self.args.metrics.output_rows {
                count.add(batch.num_rows());
            }
            if let Some(ref count) = self.args.metrics.batches_produced {
                count.add(1);
            }
            return Some(Ok(batch));
        }
    }

    fn finalize(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch = if self.refines_during_decode {
            // Parquet already applied the refinement mask during decode.
            batch
        } else {
            self.refine(batch)?
        };

        let t = Instant::now();
        let out = match self.args.row_id_output_index {
            Some(row_id_idx) => super::row_id_injection::rebase_row_ids(
                &batch,
                self.args.global_base,
                row_id_idx,
                &self.args.schema,
            )?,
            None => project_to_output_schema(batch, &self.args.schema)?,
        };
        if let Some(ref timer) = self.args.metrics.projection_fixup_time {
            timer.add_duration(t.elapsed());
        }
        Ok(out)
    }

    /// Apply `on_batch_mask` to a fully decoded batch.
    ///
    /// Only reached for evaluators that declined decode-time refinement. The row
    /// group is derived from `__row_id__` the same way the decode-time predicate
    /// does it, so the two paths agree on which candidate set applies.
    fn refine(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let row_ids = row_id_column(&batch)?;
        let (rg_first_row, state) = match row_ids.as_ref() {
            Some(ids) if !ids.is_empty() => {
                let first = ids.value(0);
                let locator = super::eval::decode_predicate::RowGroupLocator::new(
                    self.args.row_groups.clone(),
                );
                match locator.locate(first) {
                    Some(rg) => (rg.first_row, self.contexts.peek(rg.index)),
                    None => (0, None),
                }
            }
            _ => (0, None),
        };

        let row_positions = match row_ids.as_ref() {
            Some(ids) => super::eval::RowPositions::new(ids, rg_first_row),
            None => super::eval::RowPositions::unavailable(batch.num_rows()),
        };
        if !row_positions.are_available() && self.args.evaluator.needs_row_positions() {
            return Err(DataFusionError::Internal(format!(
                "evaluator requires row positions but {} is not in the scanned schema",
                crate::ROW_ID_COLUMN_NAME
            )));
        }

        static UNIT: () = ();
        let rg_state: &(dyn std::any::Any + Send + Sync) = match state.as_ref() {
            Some(ctx) => ctx.state(),
            None => &UNIT,
        };

        let t = Instant::now();
        let mask = self
            .args
            .evaluator
            .on_batch_mask(rg_state, rg_first_row, &row_positions, &batch)
            .map_err(|e| DataFusionError::External(e.into()))?;
        if let Some(ref timer) = self.args.metrics.on_batch_mask_time {
            timer.add_duration(t.elapsed());
        }

        let Some(mask) = mask else {
            return Ok(batch);
        };
        let t = Instant::now();
        let out = filter_record_batch(&batch, &mask)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        if let Some(ref timer) = self.args.metrics.filter_record_batch_time {
            timer.add_duration(t.elapsed());
        }
        Ok(out)
    }
}

/// Drop hidden read columns and restore the column order advertised by
/// [`IndexedExec`](super::stream::IndexedExec).
///
/// The parquet scan's projection is a sorted union of output, predicate, and
/// row-id columns. It is therefore not generally the schema that downstream
/// expressions planned against. Match by name and rebuild with the advertised
/// schema so column indices and types remain aligned.
fn project_to_output_schema(batch: RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema().as_ref() == output_schema.as_ref() {
        return Ok(batch);
    }

    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
            let index = batch.schema().index_of(field.name()).map_err(|_| {
                DataFusionError::Internal(format!(
                    "output column {} missing from the delivered batch",
                    field.name()
                ))
            })?;
            Ok(Arc::clone(batch.column(index)))
        })
        .collect::<Result<Vec<ArrayRef>>>()?;

    Ok(RecordBatch::try_new_with_options(
        Arc::clone(output_schema),
        columns,
        &datafusion::arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(batch.num_rows())),
    )?)
}

/// Whether DataFusion should apply its own pushed-down predicate during decode.
///
/// Independent of the evaluator's refinement: `forbid_parquet_pushdown` means the
/// evaluator owns exactness, so a pushed predicate would duplicate that work.
fn push_predicate(indexed_pushdown_filters: bool, evaluator: &dyn RowGroupBitsetSource) -> bool {
    indexed_pushdown_filters && !evaluator.forbid_parquet_pushdown()
}

/// Whether the scan will deliver `__row_id__`, which refinement needs to map a
/// delivered row back to its row-group position.
///
/// Two ways it can be absent: the file's schema has no such column (fixtures and
/// externally-written parquet), or the read projection omitted it because no
/// evaluator asked. Either way refinement cannot reject an over-read row, so the
/// caller must not coalesce.
fn row_ids_are_delivered(args: &ChunkStreamArgs) -> bool {
    let Ok(index) = args.full_schema.index_of(crate::ROW_ID_COLUMN_NAME) else {
        return false;
    };
    match args.projection {
        // No projection means "read every column", `__row_id__` included.
        None => true,
        Some(ref columns) => columns.contains(&index),
    }
}

/// The delivered `__row_id__` values, when the scan projected them.
fn row_id_column(batch: &RecordBatch) -> Result<Option<Int64Array>> {
    let Some(column) = batch.column_by_name(crate::ROW_ID_COLUMN_NAME) else {
        return Ok(None);
    };
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .map(Some)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{} must be Int64, got {:?}",
                crate::ROW_ID_COLUMN_NAME,
                column.data_type()
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use datafusion::arrow::array::{BooleanArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::ArrowReaderMetadata;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use super::*;
    use crate::indexed_table::eval::{PrefetchedRg, RowPositions};

    /// Records what `refine` passed to the evaluator, and masks nothing.
    #[derive(Debug, Default)]
    struct RecordingEvaluator {
        /// `rg_first_row` values seen by `on_batch_mask`, in order.
        seen_first_rows: Mutex<Vec<i64>>,
        needs_row_positions: bool,
        forbid_parquet_pushdown: bool,
    }

    impl RowGroupBitsetSource for RecordingEvaluator {
        fn prefetch_rg(
            &self,
            _rg: &RowGroupInfo,
            _min_doc: i32,
            _max_doc: i32,
        ) -> std::result::Result<Option<PrefetchedRg>, String> {
            unreachable!("post-decode refinement does not call the candidate stage")
        }

        fn on_batch_mask(
            &self,
            _rg_state: &dyn std::any::Any,
            rg_first_row: i64,
            _row_positions: &RowPositions,
            _batch: &RecordBatch,
        ) -> std::result::Result<Option<BooleanArray>, String> {
            self.seen_first_rows.lock().unwrap().push(rg_first_row);
            Ok(None)
        }

        fn needs_row_positions(&self) -> bool {
            self.needs_row_positions
        }

        fn forbid_parquet_pushdown(&self) -> bool {
            self.forbid_parquet_pushdown
        }
    }

    /// An evaluator that owns exactness vetoes DataFusion's own pushdown, so the
    /// same predicate is not evaluated twice. The node-wide setting does not
    /// override the veto.
    #[test]
    fn evaluator_veto_disables_parquet_pushdown() {
        let permissive = RecordingEvaluator::default();
        assert!(push_predicate(true, &permissive));
        assert!(!push_predicate(false, &permissive));

        let vetoing = RecordingEvaluator {
            forbid_parquet_pushdown: true,
            ..Default::default()
        };
        assert!(
            !push_predicate(true, &vetoing),
            "an evaluator that owns exactness must not have the predicate pushed as well"
        );
    }

    fn scanned_schema(with_row_id: bool) -> SchemaRef {
        let mut fields = vec![Field::new("v", DataType::Int32, false)];
        if with_row_id {
            fields.push(Field::new(
                crate::ROW_ID_COLUMN_NAME,
                DataType::Int64,
                false,
            ));
        }
        Arc::new(Schema::new(fields))
    }

    fn batch_with_row_ids(values: &[i32], row_ids: Option<&[i64]>) -> RecordBatch {
        let schema = scanned_schema(row_ids.is_some());
        let mut columns: Vec<datafusion::arrow::array::ArrayRef> =
            vec![Arc::new(Int32Array::from(values.to_vec()))];
        if let Some(ids) = row_ids {
            columns.push(Arc::new(Int64Array::from(ids.to_vec())));
        }
        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// `ChunkStreamArgs` carries the scan's inputs; `FinalizeState` reads only a
    /// few of them, so the rest are stubs.
    fn finalize_state(
        evaluator: Arc<RecordingEvaluator>,
        row_groups: Vec<RowGroupInfo>,
        scanned: SchemaRef,
        row_id_output_index: Option<usize>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
        inner_batches: Vec<RecordBatch>,
    ) -> FinalizeState {
        let metadata = {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let mut buffer = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), None).unwrap();
            writer.write(&RecordBatch::new_empty(schema)).unwrap();
            writer.close().unwrap();
            let loaded =
                ArrowReaderMetadata::load(&bytes::Bytes::from(buffer), Default::default()).unwrap();
            Arc::clone(loaded.metadata())
        };

        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&scanned),
            futures::stream::iter(inner_batches.into_iter().map(Ok)),
        ));

        FinalizeState {
            args: ChunkStreamArgs {
                schema: Arc::clone(&scanned),
                full_schema: Arc::clone(&scanned),
                projection: None,
                object_path: object_store::path::Path::from("stub.parquet"),
                file_size: 0,
                store: Arc::new(object_store::local::LocalFileSystem::new()),
                store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
                metadata,
                predicate: None,
                evaluator,
                row_groups,
                doc_range: None,
                metrics: StreamMetrics::empty(),
                indexed_pushdown_filters: false,
                batch_size: 1024,
                global_base: 0,
                row_id_output_index,
                dynamic_filter: None,
                cancellation_token,
                granularity: SelectionGranularity::row_granular(),
                decode_time_refinement: false,
            },
            contexts: Arc::new(RgContextStore::new()),
            refines_during_decode: false,
            inner,
        }
    }

    fn row_groups(count: usize, rows_each: i64) -> Vec<RowGroupInfo> {
        (0..count)
            .map(|index| RowGroupInfo {
                index,
                first_row: index as i64 * rows_each,
                num_rows: rows_each,
            })
            .collect()
    }

    /// A cancelled query stops the scan instead of finalizing more batches. The
    /// check runs before the inner stream is polled, so a token cancelled after
    /// planning still short-circuits.
    #[tokio::test]
    async fn cancellation_ends_the_stream_with_an_error() {
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        let evaluator = Arc::new(RecordingEvaluator::default());
        let mut state = finalize_state(
            Arc::clone(&evaluator),
            row_groups(1, 4),
            scanned_schema(true),
            None,
            Some(token),
            vec![batch_with_row_ids(&[1, 2], Some(&[0, 1]))],
        );

        let err = state.next_batch().await.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("query cancelled"),
            "unexpected error: {err}"
        );
        assert!(
            evaluator.seen_first_rows.lock().unwrap().is_empty(),
            "no batch should have been refined"
        );
    }

    /// Refinement locates the row group from the batch's first `__row_id__`, so
    /// the evaluator sees the row group's own base rather than the file's.
    #[tokio::test]
    async fn refinement_reports_the_row_group_base_of_the_batch() {
        let evaluator = Arc::new(RecordingEvaluator::default());
        // Second row group: rows 4..8.
        let mut state = finalize_state(
            Arc::clone(&evaluator),
            row_groups(3, 4),
            scanned_schema(true),
            None,
            None,
            vec![batch_with_row_ids(&[5, 6], Some(&[5, 6]))],
        );

        state.next_batch().await.unwrap().unwrap();

        assert_eq!(*evaluator.seen_first_rows.lock().unwrap(), vec![4]);
    }

    /// A `__row_id__` outside every row group in the chunk cannot be attributed,
    /// so refinement falls back to a file-relative base instead of panicking or
    /// applying another row group's state.
    #[tokio::test]
    async fn unlocatable_row_id_falls_back_to_a_zero_base() {
        let evaluator = Arc::new(RecordingEvaluator::default());
        let mut state = finalize_state(
            Arc::clone(&evaluator),
            row_groups(2, 4),
            scanned_schema(true),
            None,
            None,
            // Row 99 is past the chunk's last row group.
            vec![batch_with_row_ids(&[1], Some(&[99]))],
        );

        state.next_batch().await.unwrap().unwrap();

        assert_eq!(*evaluator.seen_first_rows.lock().unwrap(), vec![0]);
    }

    /// An evaluator that refines against row positions cannot work without
    /// `__row_id__`; that is a planning bug and must surface as an error rather
    /// than silently emitting the unrefined candidate superset.
    #[tokio::test]
    async fn missing_row_id_column_errors_when_positions_are_required() {
        let evaluator = Arc::new(RecordingEvaluator {
            needs_row_positions: true,
            ..Default::default()
        });
        let mut state = finalize_state(
            Arc::clone(&evaluator),
            row_groups(1, 4),
            scanned_schema(false),
            None,
            None,
            vec![batch_with_row_ids(&[1, 2], None)],
        );

        let err = state.next_batch().await.unwrap().unwrap_err();
        assert!(
            err.to_string().contains(crate::ROW_ID_COLUMN_NAME),
            "unexpected error: {err}"
        );
    }

    /// An evaluator that does not read row positions refines fine without the
    /// column, so no scan pays for projecting it.
    #[tokio::test]
    async fn missing_row_id_column_is_fine_when_positions_are_not_required() {
        let evaluator = Arc::new(RecordingEvaluator::default());
        let mut state = finalize_state(
            Arc::clone(&evaluator),
            row_groups(1, 4),
            scanned_schema(false),
            None,
            None,
            vec![batch_with_row_ids(&[1, 2], None)],
        );

        let batch = state.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(*evaluator.seen_first_rows.lock().unwrap(), vec![0]);
    }

    /// `__row_id__` is a physical INT64 column. Anything else means the file
    /// disagrees with what the indexed path assumes, which must be reported and
    /// not reinterpreted.
    #[test]
    fn non_int64_row_id_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            crate::ROW_ID_COLUMN_NAME,
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0, 1]))]).unwrap();

        let err = row_id_column(&batch).unwrap_err();
        assert!(
            err.to_string().contains("must be Int64"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn projection_drops_hidden_columns_and_restores_output_order() {
        let scanned_schema = Arc::new(Schema::new(vec![
            Field::new("hidden", DataType::Utf8, false),
            Field::new("number", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            scanned_schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, false),
            Field::new("number", DataType::Int32, false),
        ]));

        let output = project_to_output_schema(batch, &output_schema).unwrap();

        assert_eq!(output.schema(), output_schema);
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            20
        );
    }

    /// Empty batches are a normal consequence of a row group whose candidates
    /// the refinement rejects entirely; they must be swallowed rather than
    /// ending the stream or reaching the consumer.
    #[tokio::test]
    async fn empty_batches_are_skipped_not_emitted() {
        let evaluator = Arc::new(RecordingEvaluator::default());
        let scanned = scanned_schema(true);
        let mut state = finalize_state(
            Arc::clone(&evaluator),
            row_groups(2, 4),
            Arc::clone(&scanned),
            None,
            None,
            vec![
                RecordBatch::new_empty(Arc::clone(&scanned)),
                batch_with_row_ids(&[1], Some(&[0])),
            ],
        );

        let batch = state.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(state.next_batch().await.is_none());
    }

    /// Coalescing is licensed by refinement dropping the over-read rows, and
    /// refinement identifies them by `__row_id__`. These are the three ways the
    /// column can be missing or present.
    #[test]
    fn row_id_delivery_follows_the_read_projection() {
        let evaluator = Arc::new(RecordingEvaluator::default());
        let state = |scanned: SchemaRef, projection: Option<Vec<usize>>| {
            let mut state = finalize_state(
                Arc::clone(&evaluator),
                row_groups(1, 4),
                scanned,
                None,
                None,
                vec![],
            );
            state.args.projection = projection;
            state
        };

        // Read-everything projection covers `__row_id__` implicitly.
        assert!(row_ids_are_delivered(
            &state(scanned_schema(true), None).args
        ));
        // Explicit projection including the column (index 1).
        assert!(row_ids_are_delivered(
            &state(scanned_schema(true), Some(vec![0, 1])).args
        ));
        // Explicit projection that dropped it.
        assert!(!row_ids_are_delivered(
            &state(scanned_schema(true), Some(vec![0])).args
        ));
        // File has no such column at all — externally-written parquet.
        assert!(!row_ids_are_delivered(
            &state(scanned_schema(false), None).args
        ));
    }
}
