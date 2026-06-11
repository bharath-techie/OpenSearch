/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Consolidated one-decoder, row-group-by-row-group indexed scan.
//!
//! # Why this exists
//!
//! The per-RG [`IndexedStream`](super::stream) builds a fresh `DataSourceExec`
//! (re-deriving `ArrowReaderMetadata` + constructing a `TaskContext` + building
//! a parquet decoder) for EACH row group. On a 110-RG segment that per-RG setup
//! dominated q08 (`parquet_first_poll_time` ~107ms, measured).
//!
//! This module derives [`ArrowReaderMetadata`] ONCE (so the parquet→arrow schema
//! parse, the expensive part, happens a single time and is cheaply `Arc`-cloned
//! per RG) and drives a [`ParquetPushDecoder`] directly — no `DataSourceExec`,
//! no per-RG metadata derivation.
//!
//! # Parquet is all-RGs; Lucene is per-RG (the key separation)
//!
//! Parquet decoding is amortized by reusing the parsed schema; Lucene candidate
//! evaluation stays strictly per row group and is OVERLAPPED with decode: we
//! kick off RG n+1's `prefetch_rg` (Lucene/FFM candidate eval) on a blocking
//! task the moment RG n starts decoding, and only block on it at RG n's
//! boundary. So there is NO up-front staging of all RGs' Lucene — the abandoned
//! design that serialized every collector call before the first row.
//!
//! # Gating
//!
//! Selected in `IndexedExec::execute` only when `indexed_multi_rg_decode` is on
//! AND there is no runtime dynamic filter (TopK/join keep the per-RG path; a
//! single long-lived scan can't drop row groups mid-flight as a filter tightens).
//!
//! # Correctness
//!
//! Each per-RG decoder yields batches for exactly that RG. Per-RG refinement
//! (`on_batch_mask` / `PositionMap` / row-id base) is built by
//! [`build_rg_plan`](super::stream::build_rg_plan) — the SAME helper the per-RG
//! path uses — so output is byte-identical (asserted by the `diff_*` e2e tests).
//!
//! # Metrics parity with the per-RG path
//!
//! This path emits the same metrics the per-RG [`IndexedStream`] does for every
//! cost it actually incurs: `index_time`, `parquet_time`, `rows_matched/pruned`,
//! `rg_processed/skipped`, `position_map_*` and `min_skip_run_*` (via the shared
//! `build_rg_plan`), `on_batch_mask_time` / `mask_slice_time` /
//! `filter_record_batch_time` / `projection_fixup_time`, `coalesce_time`,
//! `batches_pre_coalesce` / `parquet_batches_received` / `batches_produced`,
//! `output_rows`, and `prefetch_wait_time` / `prefetch_wait_count` (timed at the
//! prefetch await below — a non-zero wait means the previous RG's decode failed
//! to fully cover this RG's Lucene eval, i.e. the overlap broke down).
//!
//! Intentionally NOT emitted (structurally N/A — this path drives a raw
//! `ParquetPushDecoder` and never builds a `DataSourceExec`, so there is no
//! source for them): `parquet_first_poll_time` / `parquet_first_poll_count` (the
//! per-RG `DataSourceExec` lazy-open cost this path eliminates by construction),
//! `parquet_poll_time` (no inner stream to poll), `inner_parquet_metrics` and
//! `parquet_read_time`/`bytes_scanned` (those come from DataFusion's
//! `ParquetFileMetrics`, which only exists inside `DataSourceExec`), and the
//! `dynamic_filter_rg_pruned_*` counters (this path is gated to no-dynamic-filter
//! queries). An EXPLAIN ANALYZE on this path will show those as absent/zero — by
//! design, not a regression.

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{BooleanArray, RecordBatch};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::push_decoder::ParquetPushDecoderBuilder;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::DecodeResult;
use datafusion::physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::DataFusionError;
use object_store::ObjectStore;

use super::eval::{PrefetchedRg, RowGroupBitsetSource};
use super::metrics::StreamMetrics;
use super::row_selection::PositionMap;
use super::stream::{build_rg_plan, FilterStrategy, RowGroupInfo};

/// Inputs needed to build the consolidated decoder stream. Assembled by
/// `IndexedExec::execute`.
pub struct DecoderStreamArgs {
    pub schema: SchemaRef,
    pub full_schema: SchemaRef,
    pub object_path: object_store::path::Path,
    pub store: Arc<dyn ObjectStore>,
    pub metadata: Arc<ParquetMetaData>,
    pub projection: Option<Vec<usize>>,
    pub evaluator: Arc<dyn RowGroupBitsetSource>,
    pub row_groups: Vec<RowGroupInfo>,
    pub doc_range: Option<(i32, i32)>,
    pub stream_metrics: StreamMetrics,
    pub force_strategy: Option<FilterStrategy>,
    pub min_skip_run_default: usize,
    pub min_skip_run_selectivity_threshold: f64,
    pub target_batch_size: usize,
    pub global_base: u64,
    /// Output-schema index where the computed `___row_id` column is inserted,
    /// or `None` when row-ids aren't emitted. This is the real gate (mirrors
    /// `IndexedStream`); the provider-level `emit_row_ids` bool is subsumed by
    /// it being `Some`.
    pub row_id_output_index: Option<usize>,
}

/// Build the consolidated stream as an async `RecordBatchStream`.
pub fn build_decoder_stream(args: DecoderStreamArgs) -> SendableRecordBatchStream {
    let schema = args.schema.clone();
    let inner = run(args);
    Box::pin(RecordBatchStreamAdapter::new(schema, inner))
}

/// Per-RG refinement state — the fields `IndexedStream::finalize_batch` mutates,
/// scoped to one row group. Rebuilt at each RG boundary by `plan_rg`.
struct RgState {
    rg: RowGroupInfo,
    position_map: PositionMap,
    mask: Option<BooleanArray>,
    context: Box<dyn std::any::Any + Send + Sync>,
    batch_offset: usize,
    mask_offset: usize,
}

/// Result of one RG's Lucene prefetch: `None` = RG had no candidates (skip it).
type PrefetchResult = std::result::Result<Option<PrefetchedRg>, String>;

/// Compute `[min_doc, max_doc)` for an RG, clamped to the chunk's doc_range.
/// Returns `None` if the clamped range is empty (skip the RG).
fn rg_doc_range(rg: &RowGroupInfo, doc_range: Option<(i32, i32)>) -> Option<(i32, i32)> {
    let mut min_doc = rg.first_row as i32;
    let mut max_doc = (rg.first_row + rg.num_rows) as i32;
    if let Some((lo, hi)) = doc_range {
        min_doc = min_doc.max(lo);
        max_doc = max_doc.min(hi);
        if min_doc >= max_doc {
            return None;
        }
    }
    Some((min_doc, max_doc))
}

/// Build the schema to supply to `ArrowReaderMetadata` so decoded batches carry
/// the TABLE's arrow types (e.g. `Utf8`) rather than parquet's defaults (e.g.
/// `Utf8View`). Starts from the file's native inferred arrow schema (so it has
/// the exact length/order `with_supplied_schema` demands) and overrides each
/// field's data type with the same-named field in `full_schema` when present.
/// Mirrors the coercion DataFusion's parquet opener applies on the per-RG path.
fn build_supplied_schema(
    metadata: &ParquetMetaData,
    full_schema: &SchemaRef,
) -> SchemaRef {
    // The file's native arrow schema (parquet defaults). Falls back to no
    // coercion (return a clone of `full_schema`) only if this fails, which it
    // won't for a valid file we already parsed.
    let inferred = match ArrowReaderMetadata::try_new(
        Arc::new(metadata.clone()),
        ArrowReaderOptions::new().with_page_index(false),
    ) {
        Ok(m) => m.schema().clone(),
        Err(_) => return full_schema.clone(),
    };
    use datafusion::arrow::datatypes::{Field, Schema};
    let fields: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|f| {
            match full_schema.fields().iter().find(|tf| tf.name() == f.name()) {
                // Adopt the table type but keep the file field's nullability —
                // `with_supplied_schema` checks nullability against the file, so
                // forcing the table's nullability would spuriously error.
                Some(tf) => Field::new(f.name(), tf.data_type().clone(), f.is_nullable())
                    .with_metadata(f.metadata().clone()),
                None => f.as_ref().clone(),
            }
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, inferred.metadata().clone()))
}

/// Spawn the Lucene/FFM candidate eval for one RG on a blocking task so it
/// overlaps the current RG's decode. Returns a handle the caller blocks on at
/// the RG boundary.
fn spawn_prefetch(
    evaluator: Arc<dyn RowGroupBitsetSource>,
    rg: RowGroupInfo,
    doc_range: Option<(i32, i32)>,
) -> tokio::task::JoinHandle<PrefetchResult> {
    tokio::task::spawn_blocking(move || match rg_doc_range(&rg, doc_range) {
        None => Ok(None),
        Some((min_doc, max_doc)) => evaluator.prefetch_rg(&rg, min_doc, max_doc),
    })
}

/// Turn one RG's prefetch result into the parquet `RowSelection` + the per-RG
/// refinement `RgState`. `None` when the RG has no candidates.
fn plan_rg(
    args: &DecoderStreamArgs,
    rg: RowGroupInfo,
    prefetched: PrefetchedRg,
) -> (
    datafusion::parquet::arrow::arrow_reader::RowSelection,
    RgState,
) {
    if let Some(ref timer) = args.stream_metrics.index_time {
        timer.add_duration(Duration::from_nanos(prefetched.eval_nanos));
    }
    if let Some(ref counter) = args.stream_metrics.rows_matched {
        counter.add(prefetched.candidates.len() as usize);
    }
    if let Some(ref counter) = args.stream_metrics.rows_pruned {
        counter.add((rg.num_rows as usize).saturating_sub(prefetched.candidates.len() as usize));
    }
    if let Some(ref counter) = args.stream_metrics.rg_processed {
        counter.add(1);
    }
    let (_min_skip_run, selection, position_map, mask) = build_rg_plan(
        args.force_strategy,
        args.min_skip_run_default,
        args.min_skip_run_selectivity_threshold,
        &args.evaluator,
        &args.stream_metrics,
        &rg,
        prefetched.candidates,
        prefetched.mask_buffer,
        prefetched.selection_runs,
    );
    let state = RgState {
        rg,
        position_map,
        mask,
        context: prefetched.context,
        batch_offset: 0,
        mask_offset: 0,
    };
    (selection, state)
}

/// Refine one decoded batch against the current RG's state — mirrors
/// `IndexedStream::finalize_batch`. Mutates `st.batch_offset`/`st.mask_offset`.
fn refine_batch(
    args: &DecoderStreamArgs,
    st: &mut RgState,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    let batch_len = batch.num_rows();
    let metrics = &args.stream_metrics;

    let rg_state: &dyn std::any::Any = st.context.as_ref();

    let t_on_batch = Instant::now();
    let eval_mask = args
        .evaluator
        .on_batch_mask(
            rg_state,
            st.rg.first_row,
            &st.position_map,
            st.batch_offset,
            batch_len,
            &batch,
        )
        .map_err(|e| DataFusionError::External(e.into()))?;
    if let Some(ref t) = metrics.on_batch_mask_time {
        t.add_duration(t_on_batch.elapsed());
    }

    let row_id_ctx = if args.row_id_output_index.is_some() {
        Some(super::row_id_injection::RowIdContext {
            batch_offset: st.batch_offset,
            position_map: Some(st.position_map.clone()),
            base: args.global_base + st.rg.first_row as u64,
            eval_mask: eval_mask.clone(),
        })
    } else {
        None
    };

    let output = match eval_mask {
        Some(mask) => {
            st.mask_offset += batch_len;
            st.batch_offset += batch_len;
            let t_filter = Instant::now();
            let filtered = filter_record_batch(&batch, &mask)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            if let Some(ref t) = metrics.filter_record_batch_time {
                t.add_duration(t_filter.elapsed());
            }
            filtered
        }
        None => {
            let current = if let Some(ref mask) = st.mask {
                let t_slice = Instant::now();
                // `BooleanArray::slice` (inherent) returns a `BooleanArray`.
                let mask_slice = mask.slice(st.mask_offset, batch_len);
                if let Some(ref t) = metrics.mask_slice_time {
                    t.add_duration(t_slice.elapsed());
                }
                st.mask_offset += batch_len;
                let t_filter = Instant::now();
                let filtered = filter_record_batch(&batch, &mask_slice)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                if let Some(ref t) = metrics.filter_record_batch_time {
                    t.add_duration(t_filter.elapsed());
                }
                filtered
            } else {
                batch
            };
            st.batch_offset += batch_len;
            current
        }
    };

    let t_proj = Instant::now();
    let output = if let Some(row_id_idx) = args.row_id_output_index {
        let ctx = row_id_ctx.unwrap();
        let mask_offset_before = st.mask_offset.saturating_sub(batch_len);
        super::row_id_injection::inject_row_ids(
            &output,
            &ctx,
            batch_len,
            st.mask.as_ref(),
            mask_offset_before,
            row_id_idx,
            &args.schema,
        )?
    } else if output.schema().as_ref() == args.schema.as_ref() {
        output
    } else {
        let n = args.schema.fields().len();
        if n == 0 {
            RecordBatch::try_new_with_options(
                args.schema.clone(),
                vec![],
                &datafusion::arrow::record_batch::RecordBatchOptions::new()
                    .with_row_count(Some(output.num_rows())),
            )?
        } else {
            let indices: Vec<usize> = args
                .schema
                .fields()
                .iter()
                .map(|f| output.schema().index_of(f.name()).unwrap_or(0))
                .collect();
            output.project(&indices)?
        }
    };
    if let Some(ref t) = metrics.projection_fixup_time {
        t.add_duration(t_proj.elapsed());
    }

    Ok(output)
}

/// The async driver. `unfold` over a state struct (the dependency-free idiom
/// DataFusion's own `PushDecoderStreamState::into_stream` uses) so the
/// pull-based decoder's `await`ed byte fetches compose naturally.
fn run(args: DecoderStreamArgs) -> impl futures::Stream<Item = Result<RecordBatch>> + Send {
    // Derive ArrowReaderMetadata ONCE (parses parquet→arrow schema / `fields`
    // a single time; cheaply Arc-cloned per RG). Page index off, matching the
    // per-RG bridge (candidate bitsets aren't visible to parquet's page index).
    //
    // We MUST supply a schema so the decoder emits arrow types that match the
    // table schema (`full_schema`) — exactly what the per-RG path gets for free
    // by handing `full_schema` to `ParquetSource::new` (DataFusion's opener then
    // calls `ArrowReaderMetadata::try_new` with `.with_schema(coerced)`). Without
    // this, the decoder infers parquet's DEFAULT arrow types — notably `Utf8View`
    // for string columns — while `args.schema` (projected from `full_schema`)
    // declares `Utf8`. `refine_batch`'s projection fixup only reorders columns, it
    // does NOT cast, so the type mismatch reaches a downstream kernel (e.g. a
    // `group by <string col>`) and panics with "byte view array".
    //
    // `with_supplied_schema` (inside `try_new`) requires the supplied schema to
    // match the FULL parquet file schema 1:1 in length and order. So we build it
    // from the file's native inferred arrow schema and only override each field's
    // data type with the same-named field from `full_schema` (the table types).
    // Columns absent from `full_schema` keep their inferred type.
    let supplied_schema = build_supplied_schema(&args.metadata, &args.full_schema);
    let arrow_meta_res = ArrowReaderMetadata::try_new(
        Arc::clone(&args.metadata),
        ArrowReaderOptions::new()
            .with_page_index(false)
            .with_schema(supplied_schema),
    );
    let parquet_schema = args.metadata.file_metadata().schema_descr_ptr();
    // `args.projection` indexes into `full_schema` (the table schema). The
    // parquet file's PHYSICAL column order can differ from the table schema
    // order — `infer_schema` may sort fields alphabetically, so e.g. table
    // column 2 ("score") can live at physical parquet index 1. The per-RG
    // path resolves this by name (it hands `ParquetSource` the full_schema and
    // a projection that DataFusion maps to physical columns by field name).
    // `ProjectionMask::roots` instead takes PHYSICAL root indices, so we must
    // translate table indices → physical indices by name here, else a subset
    // projection over a reordered file would decode the wrong columns. (Full
    // `SELECT *` is unaffected — it takes `ProjectionMask::all()` below.)
    //
    // Columns present in the table schema but absent from the file (schema
    // drift) are skipped: parquet can't read what isn't there, and
    // `refine_batch`'s name-based projection fixup handles the gap exactly as
    // the per-RG path does.
    let projection_mask = match &args.projection {
        Some(p) => {
            let mut physical_roots: Vec<usize> = Vec::with_capacity(p.len());
            let root_fields = parquet_schema.root_schema().get_fields();
            for &table_idx in p {
                let name = args.full_schema.field(table_idx).name();
                if let Some(phys_idx) = root_fields.iter().position(|f| f.name() == name) {
                    physical_roots.push(phys_idx);
                }
            }
            ProjectionMask::roots(&parquet_schema, physical_roots)
        }
        None => ProjectionMask::all(),
    };

    let st = DriverState {
        args,
        arrow_meta: arrow_meta_res.map_err(|e| DataFusionError::ParquetError(Box::new(e))),
        projection_mask,
        coalescer_seeded: false,
        coalescer: None,
        rg_cursor: 0,
        pending_prefetch: None,
        cur: None,
        decoder: None,
        finished: false,
    };

    futures::stream::unfold(st, |mut st| async move {
        match st.step().await {
            Some(batch) => Some((batch, st)),
            None => None,
        }
    })
}

/// All driver state carried across `unfold` iterations.
struct DriverState {
    args: DecoderStreamArgs,
    arrow_meta: Result<ArrowReaderMetadata>,
    projection_mask: ProjectionMask,
    coalescer_seeded: bool,
    coalescer: Option<LimitedBatchCoalescer>,
    /// Index into `args.row_groups` of the NEXT RG to begin (after `cur`).
    rg_cursor: usize,
    /// In-flight Lucene eval for the upcoming RG (overlap with current decode).
    pending_prefetch: Option<(RowGroupInfo, tokio::task::JoinHandle<PrefetchResult>)>,
    /// Current RG's decode + refinement state.
    cur: Option<RgState>,
    decoder: Option<datafusion::parquet::arrow::push_decoder::ParquetPushDecoder>,
    finished: bool,
}

impl DriverState {
    /// Build a decoder reading exactly one RG with `selection` (full-RG cover).
    fn build_decoder(
        &self,
        rg_index: usize,
        selection: datafusion::parquet::arrow::arrow_reader::RowSelection,
    ) -> Result<datafusion::parquet::arrow::push_decoder::ParquetPushDecoder> {
        let meta = self
            .arrow_meta
            .as_ref()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
            .clone();
        ParquetPushDecoderBuilder::new_with_metadata(meta)
            .with_projection(self.projection_mask.clone())
            .with_batch_size(self.args.target_batch_size)
            .with_row_groups(vec![rg_index])
            .with_row_selection(selection)
            .build()
            .map_err(|e| DataFusionError::ParquetError(Box::new(e)))
    }

    /// Advance the next prefetch handle one RG ahead of `rg_cursor`, skipping
    /// RGs that fall entirely outside the doc_range. Sets `pending_prefetch`.
    fn arm_next_prefetch(&mut self) {
        while self.rg_cursor < self.args.row_groups.len() {
            let rg = self.args.row_groups[self.rg_cursor].clone();
            self.rg_cursor += 1;
            if rg_doc_range(&rg, self.args.doc_range).is_none() {
                if let Some(ref c) = self.args.stream_metrics.rg_skipped {
                    c.add(1);
                }
                continue;
            }
            let handle = spawn_prefetch(Arc::clone(&self.args.evaluator), rg.clone(), self.args.doc_range);
            self.pending_prefetch = Some((rg, handle));
            return;
        }
        self.pending_prefetch = None;
    }

    /// Await the pending prefetch and, if it has candidates, build the decoder
    /// + RgState for it. Skips empty RGs (and arms the following prefetch).
    /// Returns Ok(true) if a decoder was set up, Ok(false) if no RGs remain.
    async fn advance_to_next_rg(&mut self) -> Result<bool> {
        loop {
            let Some((rg, handle)) = self.pending_prefetch.take() else {
                return Ok(false);
            };
            // Await the CURRENT RG's prefetch FIRST, *then* arm the next one.
            //
            // The Lucene/FFM collector handle (`collectDocs` on the per-query
            // Java handle) is NOT reentrant — two `collect_packed_u64_bitset`
            // calls in flight against the same handle corrupt each other's
            // output buffer (observed as `ArrayIndexOutOfBoundsException` /
            // negative byte counts on the Java side). So we must never have two
            // prefetch tasks running concurrently. Arming the next prefetch
            // only after this one resolves keeps the desired overlap
            // (prefetch(n+1) runs during DECODE of RG n, armed just below before
            // we return) while guaranteeing at most one collector call at a
            // time. The per-RG `IndexReader` upholds the same invariant.
            // Instrument the prefetch wait (mirrors `IndexReader`'s
            // `prefetch_wait_time`/`prefetch_wait_count`): if the handle hasn't
            // finished by the time we get here, the decode of the PREVIOUS RG
            // did not fully cover this RG's Lucene eval and we stall — the
            // overlap broke down. If it's already finished, overlap worked and
            // the await is ~free (we don't count it as a wait).
            let already_done = handle.is_finished();
            let t_wait = Instant::now();
            let prefetched = handle
                .await
                .map_err(|e| DataFusionError::Execution(format!("prefetch join: {e}")))?
                .map_err(|e| DataFusionError::External(e.into()))?;
            if !already_done {
                if let Some(ref t) = self.args.stream_metrics.prefetch_wait_time {
                    t.add_duration(t_wait.elapsed());
                }
                if let Some(ref c) = self.args.stream_metrics.prefetch_wait_count {
                    c.add(1);
                }
            }
            // Arm the FOLLOWING RG's prefetch now so it overlaps this RG's
            // decode (the current prefetch has completed, so this is the only
            // collector call in flight).
            self.arm_next_prefetch();
            match prefetched {
                None => {
                    // Empty RG (no candidates) — skip without a parquet read.
                    if let Some(ref c) = self.args.stream_metrics.rg_skipped {
                        c.add(1);
                    }
                    // pending_prefetch now armed to the following RG; loop.
                    continue;
                }
                Some(p) => {
                    let t_plan = Instant::now();
                    let (selection, state) = plan_rg(&self.args, rg, p);
                    let decoder = self.build_decoder(state.rg.index, selection)?;
                    if let Some(ref timer) = self.args.stream_metrics.parquet_time {
                        timer.add_duration(t_plan.elapsed());
                    }
                    self.cur = Some(state);
                    self.decoder = Some(decoder);
                    return Ok(true);
                }
            }
        }
    }

    /// Produce the next output batch, or `None` at end-of-stream. Errors are
    /// yielded as `Some(Err(_))`.
    async fn step(&mut self) -> Option<Result<RecordBatch>> {
        // Seed: build the coalescer and arm + advance to the first RG.
        if !self.coalescer_seeded {
            self.coalescer_seeded = true;
            if let Err(e) = &self.arrow_meta {
                self.finished = true;
                return Some(Err(DataFusionError::Execution(e.to_string())));
            }
            self.coalescer = Some(LimitedBatchCoalescer::new(
                self.args.schema.clone(),
                self.args.target_batch_size,
                None,
            ));
            self.arm_next_prefetch();
            match self.advance_to_next_rg().await {
                Ok(true) => {}
                Ok(false) => {
                    self.finished = true;
                    return None;
                }
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }

        loop {
            // Drain any completed coalescer batch first.
            if let Some(c) = self.coalescer.as_mut() {
                if let Some(out) = c.next_completed_batch() {
                    if let Some(ref ctr) = self.args.stream_metrics.output_rows {
                        ctr.add(out.num_rows());
                    }
                    if let Some(ref ctr) = self.args.stream_metrics.batches_produced {
                        ctr.add(1);
                    }
                    return Some(Ok(out));
                }
            }

            if self.finished {
                return None;
            }

            // Drive the current decoder.
            let decoder = self.decoder.as_mut().expect("decoder set when not finished");
            let decoded = decoder.try_decode();
            match decoded {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    let data = match self
                        .args
                        .store
                        .get_ranges(&self.args.object_path, &ranges)
                        .await
                    {
                        Ok(d) => d,
                        Err(e) => {
                            self.finished = true;
                            return Some(Err(DataFusionError::External(Box::new(e))));
                        }
                    };
                    if let Err(e) = decoder.push_ranges(ranges, data) {
                        self.finished = true;
                        return Some(Err(DataFusionError::ParquetError(Box::new(e))));
                    }
                }
                Ok(DecodeResult::Data(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    if let Some(ref c) = self.args.stream_metrics.parquet_batches_received {
                        c.add(1);
                    }
                    let st = self.cur.as_mut().expect("cur set when decoding");
                    let refined = match refine_batch(&self.args, st, batch) {
                        Ok(b) => b,
                        Err(e) => {
                            self.finished = true;
                            return Some(Err(e));
                        }
                    };
                    if refined.num_rows() == 0 {
                        continue;
                    }
                    let t0 = Instant::now();
                    let status = self.coalescer.as_mut().unwrap().push_batch(refined);
                    if let Some(ref t) = self.args.stream_metrics.coalesce_time {
                        t.add_duration(t0.elapsed());
                    }
                    if let Some(ref c) = self.args.stream_metrics.batches_pre_coalesce {
                        c.add(1);
                    }
                    match status {
                        Ok(PushBatchStatus::Continue) | Ok(PushBatchStatus::LimitReached) => {}
                        Err(e) => {
                            self.finished = true;
                            return Some(Err(e));
                        }
                    }
                    // loop to drain completed batches
                }
                Ok(DecodeResult::Finished) => {
                    // Current RG done. Advance to the next (its prefetch has been
                    // running during this decode). When none remain, flush.
                    self.decoder = None;
                    self.cur = None;
                    match self.advance_to_next_rg().await {
                        Ok(true) => { /* loop into the new decoder */ }
                        Ok(false) => {
                            // No more RGs — flush the coalescer.
                            if let Some(c) = self.coalescer.as_mut() {
                                if let Err(e) = c.finish() {
                                    self.finished = true;
                                    return Some(Err(e));
                                }
                            }
                            self.finished = true;
                            // loop: drain remaining completed batches at top.
                        }
                        Err(e) => {
                            self.finished = true;
                            return Some(Err(e));
                        }
                    }
                }
                Err(e) => {
                    self.finished = true;
                    return Some(Err(DataFusionError::ParquetError(Box::new(e))));
                }
            }
        }
    }
}
