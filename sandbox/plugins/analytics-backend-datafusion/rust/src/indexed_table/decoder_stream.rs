/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! One adaptive parquet decoder per indexed segment chunk.
//!
//! Arrow owns the decoder frontier and byte buffers. OpenSearch only supplies
//! the next row group's externally-derived selection at a boundary, then uses
//! `peek_next_row_group` to verify that evaluator and decoder state agree.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{new_null_array, BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::{
    apply_file_schema_type_coercions, build_row_filter, ParquetFileMetrics,
};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader, RowFilter, RowSelection,
    RowSelector,
};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use datafusion::parquet::DecodeResult;
use datafusion::physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion::physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Time};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
};
use futures::future::poll_fn;

use super::eval::RowGroupBitsetSource;
use super::metrics::StreamMetrics;
use super::parquet_bridge::{create_cached_metadata_reader, ReadIoStats};
use super::row_selection::PositionMap;
use super::stream::{
    build_rg_plan, FilterStrategy, IndexReader, PrefetchedRowGroup, RowGroupDecodePlan,
    RowGroupInfo,
};

pub(super) struct DecoderStreamArgs {
    pub schema: SchemaRef,
    pub full_schema: SchemaRef,
    pub projection: Option<Vec<usize>>,
    pub object_path: object_store::path::Path,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub metadata: Arc<ParquetMetaData>,
    pub predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    pub evaluator: Arc<dyn RowGroupBitsetSource>,
    pub row_groups: Vec<RowGroupInfo>,
    pub doc_range: Option<(i32, i32)>,
    pub metrics: StreamMetrics,
    pub force_strategy: Option<FilterStrategy>,
    pub min_skip_run_default: usize,
    pub min_skip_run_selectivity_threshold: f64,
    pub indexed_pushdown_filters: bool,
    pub batch_size: usize,
    pub global_base: u64,
    pub row_id_output_index: Option<usize>,
    pub cancellation_token: Option<tokio_util::sync::CancellationToken>,
}

struct RgState {
    rg: RowGroupInfo,
    position_map: Option<PositionMap>,
    mask: Option<BooleanArray>,
    context: Box<dyn std::any::Any + Send + Sync>,
    batch_offset: usize,
    mask_offset: usize,
    expected_tail_row_groups: usize,
    row_filter_can_skip: bool,
}

pub(super) fn build_decoder_stream(args: DecoderStreamArgs) -> Result<SendableRecordBatchStream> {
    let arrow_meta = prepare_arrow_metadata(&args.metadata, &args.full_schema)?;
    let physical_schema = Arc::clone(arrow_meta.schema());
    let adapted_predicate = adapt_predicate(
        args.predicate.clone(),
        Arc::clone(&args.full_schema),
        Arc::clone(&physical_schema),
    )?;
    let projection_mask = build_projection_mask(
        &args.full_schema,
        args.projection.as_deref(),
        adapted_predicate.as_ref(),
        &physical_schema,
        &args.metadata,
    );

    let decoder_metrics = ExecutionPlanMetricsSet::new();
    let file_metrics = ParquetFileMetrics::new(0, args.object_path.as_ref(), &decoder_metrics);
    let io_stats = args
        .metrics
        .io_stats
        .clone()
        .unwrap_or_else(|| Arc::new(ReadIoStats::default()));
    let reader = create_cached_metadata_reader(
        Arc::clone(&args.store),
        args.object_path.clone(),
        Arc::clone(&args.metadata),
        file_metrics.clone(),
        io_stats,
    );
    let arrow_reader_metrics = ArrowReaderMetrics::enabled();
    let index_reader = IndexReader::new(
        Arc::clone(&args.evaluator),
        args.row_groups.clone(),
        args.doc_range,
        args.metrics.rg_skipped.clone(),
        args.metrics.prefetch_wait_time.clone(),
        args.metrics.prefetch_wait_count.clone(),
        Some(Arc::clone(&args.metadata)),
        args.metrics.dynamic_filter_rg_pruned_at_prefetch.clone(),
        args.cancellation_token.clone(),
    );

    let schema = Arc::clone(&args.schema);
    let state = DriverState {
        coalescer: LimitedBatchCoalescer::new(Arc::clone(&schema), args.batch_size, None),
        args,
        index_reader,
        arrow_meta,
        physical_schema,
        adapted_predicate,
        projection_mask,
        reader,
        file_metrics,
        arrow_reader_metrics,
        decoder_metrics,
        decoder: None,
        batch_reader: None,
        current_rg: None,
        initialized: false,
        upstream_done: false,
        coalescer_finished: false,
        metrics_published: false,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        state.next_batch().await.map(|batch| (batch, state))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

fn prepare_arrow_metadata(
    metadata: &Arc<ParquetMetaData>,
    full_schema: &SchemaRef,
) -> Result<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
    let inferred = ArrowReaderMetadata::try_new(Arc::clone(metadata), options.clone())?;
    let Some(coerced) =
        apply_file_schema_type_coercions(full_schema.as_ref(), inferred.schema().as_ref())
    else {
        return Ok(inferred);
    };
    Ok(ArrowReaderMetadata::try_new(
        Arc::clone(metadata),
        options.with_schema(Arc::new(coerced)),
    )?)
}

fn adapt_predicate(
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    logical_schema: SchemaRef,
    physical_schema: SchemaRef,
) -> Result<Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    let adapter =
        DefaultPhysicalExprAdapterFactory.create(logical_schema, Arc::clone(&physical_schema))?;
    let rewritten = adapter.rewrite(predicate)?;
    Ok(Some(
        PhysicalExprSimplifier::new(&physical_schema).simplify(rewritten)?,
    ))
}

fn build_projection_mask(
    logical_schema: &SchemaRef,
    projection: Option<&[usize]>,
    predicate: Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    physical_schema: &SchemaRef,
    metadata: &ParquetMetaData,
) -> ProjectionMask {
    let mut names: HashSet<&str> = match projection {
        Some(indices) => indices
            .iter()
            .filter_map(|&index| logical_schema.fields().get(index))
            .map(|field| field.name().as_str())
            .collect(),
        None => logical_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect(),
    };
    let predicate_columns = predicate
        .map(datafusion::physical_expr::utils::collect_columns)
        .unwrap_or_default();
    names.extend(predicate_columns.iter().map(|column| column.name()));

    let roots = physical_schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| names.contains(field.name().as_str()).then_some(idx));
    ProjectionMask::roots(metadata.file_metadata().schema_descr(), roots)
}

struct DriverState {
    args: DecoderStreamArgs,
    index_reader: IndexReader,
    arrow_meta: ArrowReaderMetadata,
    physical_schema: SchemaRef,
    adapted_predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    projection_mask: ProjectionMask,
    reader: Box<dyn AsyncFileReader + Send>,
    file_metrics: ParquetFileMetrics,
    arrow_reader_metrics: ArrowReaderMetrics,
    decoder_metrics: ExecutionPlanMetricsSet,
    decoder: Option<ParquetPushDecoder>,
    batch_reader: Option<ParquetRecordBatchReader>,
    current_rg: Option<RgState>,
    coalescer: LimitedBatchCoalescer,
    initialized: bool,
    upstream_done: bool,
    coalescer_finished: bool,
    metrics_published: bool,
}

struct ActiveComputeTimer {
    metric: Option<Time>,
    started: Option<Instant>,
}

impl ActiveComputeTimer {
    fn new(metric: Option<Time>) -> Self {
        Self {
            metric,
            started: Some(Instant::now()),
        }
    }

    fn pause(&mut self) {
        let Some(started) = self.started.take() else {
            return;
        };
        if let Some(ref metric) = self.metric {
            metric.add_duration(started.elapsed());
        }
    }

    fn resume(&mut self) {
        self.started = Some(Instant::now());
    }
}

impl Drop for ActiveComputeTimer {
    fn drop(&mut self) {
        self.pause();
    }
}

impl DriverState {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        let mut compute_timer = ActiveComputeTimer::new(self.args.metrics.elapsed_compute.clone());
        if !self.initialized {
            self.initialized = true;
            let started = Instant::now();
            self.index_reader.init_prefetch();
            if let Some(ref timer) = self.args.metrics.init_prefetch_time {
                timer.add_duration(started.elapsed());
            }
        }

        loop {
            if self.index_reader.is_cancelled() {
                return self.fail(DataFusionError::Execution("query cancelled".to_string()));
            }

            if let Some(batch) = self.coalescer.next_completed_batch() {
                if let Some(ref count) = self.args.metrics.output_rows {
                    count.add(batch.num_rows());
                }
                if let Some(ref count) = self.args.metrics.batches_produced {
                    count.add(1);
                }
                return Some(Ok(batch));
            }

            if self.upstream_done {
                if !self.coalescer_finished {
                    if let Err(error) = self.coalescer.finish() {
                        return self.fail(error);
                    }
                    self.coalescer_finished = true;
                    continue;
                }
                if self.coalescer.is_empty() {
                    self.publish_metrics();
                    return None;
                }
            }

            if let Some(reader) = self.batch_reader.as_mut() {
                let started = Instant::now();
                let next = reader.next();
                if let Some(ref timer) = self.args.metrics.parquet_poll_time {
                    timer.add_duration(started.elapsed());
                }
                match next {
                    Some(Ok(batch)) if batch.num_rows() > 0 => {
                        if let Some(ref count) = self.args.metrics.parquet_batches_received {
                            count.add(1);
                        }
                        let refined = match self.refine_batch(batch) {
                            Ok(batch) => batch,
                            Err(error) => return self.fail(error),
                        };
                        if refined.num_rows() == 0 {
                            continue;
                        }
                        let started = Instant::now();
                        let status = self.coalescer.push_batch(refined);
                        if let Some(ref timer) = self.args.metrics.coalesce_time {
                            timer.add_duration(started.elapsed());
                        }
                        if let Some(ref count) = self.args.metrics.batches_pre_coalesce {
                            count.add(1);
                        }
                        match status {
                            Ok(PushBatchStatus::Continue) => continue,
                            Ok(PushBatchStatus::LimitReached) => {
                                self.upstream_done = true;
                                continue;
                            }
                            Err(error) => return self.fail(error),
                        }
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(error)) => {
                        return self.fail(DataFusionError::ArrowError(Box::new(error), None));
                    }
                    None => {
                        self.batch_reader = None;
                        self.current_rg = None;
                        continue;
                    }
                }
            }

            if self.current_rg.is_none() {
                compute_timer.pause();
                let next_rg = self.advance_to_next_rg().await;
                compute_timer.resume();
                match next_rg {
                    Ok(true) => {}
                    Ok(false) => {
                        self.upstream_done = true;
                        continue;
                    }
                    Err(error) => return self.fail(error),
                }
            }

            let decoded = self
                .decoder
                .as_mut()
                .expect("decoder exists while a row group is active")
                .try_next_reader();
            let advanced_past_current = self.current_rg.as_ref().is_some_and(|state| {
                self.decoder
                    .as_ref()
                    .expect("decoder exists while a row group is active")
                    .row_groups_remaining()
                    < state.expected_tail_row_groups
            });
            if advanced_past_current {
                let row_filter_can_skip = self
                    .current_rg
                    .as_ref()
                    .is_some_and(|state| state.row_filter_can_skip);
                if !row_filter_can_skip {
                    return self.fail(DataFusionError::Execution(
                        "adaptive decoder advanced beyond its planned row group".to_string(),
                    ));
                }
                // A RowFilter can eliminate the current RG while try_next_reader
                // speculatively starts a suffix RG. Drop that unplanned work and
                // let IndexReader evaluate the suffix before rebuilding.
                self.decoder = None;
                self.current_rg = None;
                continue;
            }
            match decoded {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    compute_timer.pause();
                    let read_result = self.reader.get_byte_ranges(ranges.clone()).await;
                    compute_timer.resume();
                    let data = match read_result {
                        Ok(data) => data,
                        Err(error) => {
                            return self.fail(DataFusionError::ParquetError(Box::new(error)))
                        }
                    };
                    if let Err(error) = self
                        .decoder
                        .as_mut()
                        .expect("decoder exists after IO")
                        .push_ranges(ranges, data)
                    {
                        return self.fail(DataFusionError::ParquetError(Box::new(error)));
                    }
                }
                Ok(DecodeResult::Data(reader)) => {
                    self.batch_reader = Some(reader);
                }
                Ok(DecodeResult::Finished) => {
                    let row_filter_can_skip = self
                        .current_rg
                        .as_ref()
                        .is_some_and(|state| state.row_filter_can_skip);
                    if row_filter_can_skip {
                        self.decoder = None;
                        self.current_rg = None;
                        continue;
                    }
                    return self.fail(DataFusionError::Execution(
                        "adaptive decoder finished before its planned row group".to_string(),
                    ));
                }
                Err(error) => {
                    return self.fail(DataFusionError::ParquetError(Box::new(error)));
                }
            }
        }
    }

    async fn advance_to_next_rg(&mut self) -> Result<bool> {
        let prefetched = poll_fn(|cx| self.index_reader.poll_next_row_group(cx)).await?;
        let Some(prefetched) = prefetched else {
            return Ok(false);
        };
        self.install_row_group(prefetched)?;
        Ok(true)
    }

    fn install_row_group(&mut self, prefetched: PrefetchedRowGroup) -> Result<()> {
        let PrefetchedRowGroup { rg, prefetched } = prefetched;
        if let Some(ref timer) = self.args.metrics.index_time {
            timer.add_duration(Duration::from_nanos(prefetched.eval_nanos));
        }
        if let Some(ref count) = self.args.metrics.rows_matched {
            count.add(prefetched.rows.matched_rows());
        }
        if let Some(ref count) = self.args.metrics.rows_pruned {
            count.add((rg.num_rows as usize).saturating_sub(prefetched.rows.matched_rows()));
        }
        if let Some(ref count) = self.args.metrics.rg_processed {
            count.add(1);
        }

        let RowGroupDecodePlan {
            selection,
            position_map,
            mask,
            push_predicate,
        } = build_rg_plan(
            self.args.force_strategy,
            self.args.min_skip_run_default,
            self.args.min_skip_run_selectivity_threshold,
            self.args.indexed_pushdown_filters,
            &self.args.evaluator,
            &self.args.metrics,
            &rg,
            prefetched.rows,
            self.args.row_id_output_index.is_some(),
        );
        let remaining = self.remaining_row_groups(rg.index)?;
        let expected_tail_row_groups = remaining.len().saturating_sub(1);
        let selection = self.selection_with_select_all_suffix(selection, &remaining)?;

        let started = Instant::now();
        let builder = match self.decoder.take() {
            Some(decoder) => {
                if !decoder.is_at_row_group_boundary() {
                    return Err(DataFusionError::Execution(
                        "adaptive decoder rebuild attempted outside a row-group boundary"
                            .to_string(),
                    ));
                }
                decoder.into_builder()?
            }
            None => ParquetPushDecoderBuilder::new_with_metadata(self.arrow_meta.clone()),
        };
        let (row_filter, row_filter_installed) = self.build_row_filter(push_predicate)?;
        let decoder = builder
            .with_projection(self.projection_mask.clone())
            .with_batch_size(self.args.batch_size)
            .with_metrics(self.arrow_reader_metrics.clone())
            .with_row_groups(remaining)
            .with_row_selection(selection)
            .with_row_filter(row_filter)
            .build()?;
        let actual = decoder.peek_next_row_group()?;
        if actual != Some(rg.index) {
            return Err(DataFusionError::Execution(format!(
                "adaptive decoder/evaluator row-group mismatch: expected {}, decoder reports {:?}",
                rg.index, actual
            )));
        }
        if let Some(ref timer) = self.args.metrics.parquet_time {
            timer.add_duration(started.elapsed());
        }

        self.current_rg = Some(RgState {
            rg,
            position_map,
            mask,
            context: prefetched.context,
            batch_offset: 0,
            mask_offset: 0,
            expected_tail_row_groups,
            row_filter_can_skip: row_filter_installed,
        });
        self.decoder = Some(decoder);
        Ok(())
    }

    fn remaining_row_groups(&self, current: usize) -> Result<Vec<usize>> {
        let position = self
            .args
            .row_groups
            .iter()
            .position(|rg| rg.index == current)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "prefetched row group {current} is not in the chunk plan"
                ))
            })?;
        Ok(self.args.row_groups[position..]
            .iter()
            .map(|rg| rg.index)
            .collect())
    }

    fn selection_with_select_all_suffix(
        &self,
        current: RowSelection,
        remaining: &[usize],
    ) -> Result<RowSelection> {
        let mut selectors: Vec<RowSelector> = current.into();
        for &rg_index in remaining.iter().skip(1) {
            let rows = self
                .args
                .metadata
                .row_group(rg_index)
                .num_rows()
                .try_into()
                .map_err(|_| {
                    DataFusionError::Execution(format!(
                        "negative row count for row group {rg_index}"
                    ))
                })?;
            if rows > 0 {
                selectors.push(RowSelector::select(rows));
            }
        }
        Ok(RowSelection::from(selectors))
    }

    fn build_row_filter(&self, push_predicate: bool) -> Result<(RowFilter, bool)> {
        if push_predicate {
            if let Some(predicate) = self.adapted_predicate.as_ref() {
                let filter = build_row_filter(
                    predicate,
                    &self.physical_schema,
                    &self.args.metadata,
                    true,
                    &self.file_metrics,
                );
                match filter {
                    Ok(Some(filter)) => return Ok((filter, true)),
                    Ok(None) => {}
                    Err(error) => {
                        log::debug!(
                            "RowFilter construction failed; decoding without pushdown: {error}"
                        );
                    }
                }
            }
        }
        // `into_builder` preserves the previous filter. An empty RowFilter
        // explicitly clears it when the next RG requires post-decode alignment.
        Ok((RowFilter::new(vec![]), false))
    }

    fn refine_batch(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let state = self
            .current_rg
            .as_mut()
            .expect("row-group state exists while decoding");
        let batch_len = batch.num_rows();
        let empty_position_map = PositionMap::Identity { delivered_count: 0 };
        let started = Instant::now();
        let eval_mask = self
            .args
            .evaluator
            .on_batch_mask(
                state.context.as_ref(),
                state.rg.first_row,
                state.position_map.as_ref().unwrap_or(&empty_position_map),
                state.batch_offset,
                batch_len,
                &batch,
            )
            .map_err(|error| DataFusionError::External(error.into()))?;
        if let Some(ref timer) = self.args.metrics.on_batch_mask_time {
            timer.add_duration(started.elapsed());
        }

        let row_id_context =
            self.args
                .row_id_output_index
                .map(|_| super::row_id_injection::RowIdContext {
                    batch_offset: state.batch_offset,
                    position_map: state.position_map.clone(),
                    base: self.args.global_base + state.rg.first_row as u64,
                    eval_mask: eval_mask.clone(),
                });

        let output = match eval_mask {
            Some(mask) => {
                state.mask_offset += batch_len;
                state.batch_offset += batch_len;
                let started = Instant::now();
                let output = filter_record_batch(&batch, &mask)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
                if let Some(ref timer) = self.args.metrics.filter_record_batch_time {
                    timer.add_duration(started.elapsed());
                }
                output
            }
            None => {
                let output = if let Some(mask) = state.mask.as_ref() {
                    let started = Instant::now();
                    let slice = mask.slice(state.mask_offset, batch_len);
                    if let Some(ref timer) = self.args.metrics.mask_slice_time {
                        timer.add_duration(started.elapsed());
                    }
                    state.mask_offset += batch_len;
                    let started = Instant::now();
                    let output = filter_record_batch(&batch, &slice)
                        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
                    if let Some(ref timer) = self.args.metrics.filter_record_batch_time {
                        timer.add_duration(started.elapsed());
                    }
                    output
                } else {
                    batch
                };
                state.batch_offset += batch_len;
                output
            }
        };

        let started = Instant::now();
        let output = if let Some(row_id_index) = self.args.row_id_output_index {
            let mask_offset_before = state.mask_offset.saturating_sub(batch_len);
            super::row_id_injection::inject_row_ids(
                &output,
                row_id_context
                    .as_ref()
                    .expect("row-id context exists when row IDs are requested"),
                batch_len,
                state.mask.as_ref(),
                mask_offset_before,
                row_id_index,
                &self.args.schema,
            )?
        } else {
            project_output(&output, &self.args.schema)?
        };
        if let Some(ref timer) = self.args.metrics.projection_fixup_time {
            timer.add_duration(started.elapsed());
        }
        self.copy_arrow_reader_metrics();
        Ok(output)
    }

    fn copy_arrow_reader_metrics(&self) {
        if let Some(value) = self.arrow_reader_metrics.records_read_from_inner() {
            self.file_metrics.predicate_cache_inner_records.set(value);
        }
        if let Some(value) = self.arrow_reader_metrics.records_read_from_cache() {
            self.file_metrics.predicate_cache_records.set(value);
        }
    }

    fn fail(&mut self, error: DataFusionError) -> Option<Result<RecordBatch>> {
        self.upstream_done = true;
        self.coalescer_finished = true;
        self.publish_metrics();
        Some(Err(error))
    }

    fn publish_metrics(&mut self) {
        if self.metrics_published {
            return;
        }
        self.metrics_published = true;
        self.copy_arrow_reader_metrics();
        if let Some(ref sink) = self.args.metrics.inner_parquet_metrics {
            if let Ok(mut sets) = sink.lock() {
                sets.push(self.decoder_metrics.clone_inner());
            }
        }
    }
}

fn project_output(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.schema().as_ref() == output_schema.as_ref() {
        return Ok(batch.clone());
    }
    let rows = batch.num_rows();
    let batch_schema = batch.schema();
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
            batch_schema
                .index_of(field.name())
                .map(|index| Arc::clone(batch.column(index)))
                .unwrap_or_else(|_| new_null_array(field.data_type(), rows))
        })
        .collect();
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(output_schema),
        columns,
        &datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}
