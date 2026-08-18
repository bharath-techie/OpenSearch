/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Generic partial-state merge for materialized-view segments.
//!
//! MV segments store DataFusion *partial accumulator states* per bucket instead of
//! finalized values. This merge folds states of equal-key buckets into states with a
//! single `AggregateExec` in [`AggregateMode::PartialReduce`] — states in, states out —
//! so segments stay re-mergeable forever and the read path finalizes with a
//! `Final`-mode aggregation across segments.
//!
//! Nothing here knows any aggregation math. Each aggregate is described by a spec
//! entry (output name, aggregate function name, raw input types); the function is
//! resolved from DataFusion's registry, its state layout comes from
//! `AggregateFunctionExpr::state_fields()`, and folding is the accumulator's own
//! `merge_batch`. Any function DataFusion can aggregate — `avg`, `stddev`,
//! `approx_distinct`, UDAFs — merges without code changes.
//!
//! State column storage convention: the states of output `o` are stored as parquet
//! columns `o__st_0..o__st_{n-1}` (mapping-safe names). This is the only layout —
//! refresh writes it, merge folds it, reads finalize from it.
//!
//! Correctness stance mirrors the aggregating merge: collapsing is an optimization.
//! Callers fall back to the concatenating merge on any failure — concatenated state
//! segments stay correct (reads fold through the same ⊕), just uncompacted.

use std::fs::File;
use std::sync::Arc;

use arrow::array::{new_null_array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::schema::types::SchemaDescriptor;

use super::context::MergeContext;
use super::error::{MergeError, MergeResult};
use super::schema::projection_indices_excluding_row_id;
use crate::log_debug;
use crate::memory::merge_pool;
use native_bridge_common::memory_pool::{MemoryReservation, PoolBehavior};

/// Suffix separator for state column names: output `o`'s i-th state field is `o__st_{i}`.
const STATE_COL_SEP: &str = "__st_";

/// Our state-layout convention version — must match the emitter's
/// (`agg_mode::STATE_LAYOUT_VERSION` in the DataFusion backend).
const STATE_LAYOUT_VERSION: u32 = 1;

/// Engine version pin carried in the spec: accumulator state layouts are an on-disk
/// format, so a view written under a different DataFusion version (or layout marker)
/// must not be folded by this build.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct EngineVersion {
    pub datafusion: String,
    pub layout: u32,
}

/// One aggregate output of the view: its name, the DataFusion aggregate function that
/// produced it, and the raw input types of the original query (needed so the function
/// resolves the same state layout it used at refresh time).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct AggStateSpec {
    /// Output column name (the alias in the view definition).
    pub output: String,
    /// DataFusion aggregate function name, resolved from the session registry.
    #[serde(rename = "fn")]
    pub function: String,
    /// Arrow data types of the function's raw inputs, in `DataType` display form
    /// (e.g. `"Float64"`, `"Int64"`).
    pub input_types: Vec<String>,
}

/// Full merge spec for an MV index: group keys plus one entry per aggregate output.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct MvStateSpec {
    pub key_columns: Vec<String>,
    pub aggs: Vec<AggStateSpec>,
    /// Absent in early specs; validated when present.
    #[serde(default)]
    pub engine: Option<EngineVersion>,
}

impl MvStateSpec {
    pub fn parse(json: &str) -> MergeResult<MvStateSpec> {
        serde_json::from_str(json)
            .map_err(|e| MergeError::Logic(format!("MV state spec parse: {}", e)))
    }

    /// Refuses to fold states written by a different engine version. Returning an
    /// error routes the merge to the concatenating fallback — segments stay correct
    /// and re-foldable by a matching build; nothing is ever mis-merged.
    pub fn validate_engine(&self) -> MergeResult<()> {
        if let Some(engine) = &self.engine {
            if engine.datafusion != datafusion::DATAFUSION_VERSION || engine.layout != STATE_LAYOUT_VERSION {
                return Err(MergeError::Logic(format!(
                    "MV state spec written by datafusion {} layout {}, this build is {} layout {} — refusing to fold; rebuild the view to migrate",
                    engine.datafusion,
                    engine.layout,
                    datafusion::DATAFUSION_VERSION,
                    STATE_LAYOUT_VERSION
                )));
            }
        }
        Ok(())
    }
}

/// Name of the i-th state column for aggregate output `output`.
pub fn state_column_name(output: &str, i: usize) -> String {
    format!("{}{}{}", output, STATE_COL_SEP, i)
}

/// The planned merge: aggregate expressions, the state ("partial") schema the
/// aggregation consumes and produces, and where each state column comes from in the
/// input segments.
struct PartialMergePlan {
    /// Reusable aggregate expressions (the same objects a Partial/Final stage would use).
    aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
    /// Group-by on the key columns (positions 0..k of the state schema).
    group_by: PhysicalGroupBy,
    /// Schema of the aggregation input/output: keys first, then every aggregate's
    /// state fields in spec order, then passthrough state fields.
    state_schema: SchemaRef,
    /// For each state-schema column: the input segment column name it is read from.
    source_columns: Vec<String>,
}

/// Builds the merge plan from the spec and the union schema of the input segments.
///
/// Aggregate expressions are constructed against a synthetic raw schema (keys +
/// phantom input columns typed from the spec) — in `PartialReduce` mode the input
/// expressions are never evaluated, but `state_fields()` must resolve exactly as at
/// refresh time. State columns are consumed positionally by the exec, so this plan
/// pins the scan order; names never need to match DataFusion's internal state names.
fn plan_partial_merge(spec: &MvStateSpec, input_schema: &ArrowSchema) -> MergeResult<PartialMergePlan> {
    let logic = |m: String| MergeError::Logic(m);

    // Synthetic raw schema: keys (types from the segments) + one phantom input column
    // per aggregate argument (types from the spec).
    let mut raw_fields: Vec<Field> = Vec::new();
    for key in &spec.key_columns {
        let f = input_schema
            .field_with_name(key)
            .map_err(|_| logic(format!("MV state merge: key column [{}] missing", key)))?;
        raw_fields.push(f.clone());
    }
    let mut arg_names: Vec<Vec<String>> = Vec::new();
    for agg in &spec.aggs {
        let mut names = Vec::new();
        for (i, ty) in agg.input_types.iter().enumerate() {
            let data_type: DataType = ty
                .parse()
                .map_err(|e| logic(format!("MV state merge: input type [{}] for [{}]: {:?}", ty, agg.output, e)))?;
            let name = format!("__in_{}_{}", agg.output, i);
            raw_fields.push(Field::new(&name, data_type, true));
            names.push(name);
        }
        arg_names.push(names);
    }

    // Passthrough columns: anything in the segments that is neither a key nor a state
    // column of a spec'd aggregate. Folded with max(), whose state is the value itself
    // — MV reads never consult these, but the segment schema requires them.
    let is_state_col = |name: &str| -> bool {
        spec.aggs
            .iter()
            .any(|agg| name.starts_with(&agg.output) && name[agg.output.len()..].starts_with(STATE_COL_SEP))
    };
    let mut passthrough: Vec<Field> = Vec::new();
    for field in input_schema.fields() {
        let name = field.name().as_str();
        if spec.key_columns.iter().any(|k| k == name) || is_state_col(name) {
            continue;
        }
        passthrough.push(field.as_ref().clone());
        raw_fields.push(field.as_ref().clone());
    }
    let raw_schema = Arc::new(ArrowSchema::new(raw_fields));

    // Aggregate expressions: spec'd aggregates from the registry, then passthroughs.
    let ctx = SessionContext::new();
    let state = ctx.state();
    let mut aggr_exprs: Vec<Arc<AggregateFunctionExpr>> = Vec::new();
    for (agg, names) in spec.aggs.iter().zip(&arg_names) {
        let udaf = state
            .udaf(&agg.function)
            .map_err(|e| logic(format!("MV state merge: unknown aggregate [{}]: {}", agg.function, e)))?;
        let args = names
            .iter()
            .map(|n| col(n, &raw_schema).map_err(|e| logic(format!("MV state merge arg [{}]: {}", n, e))))
            .collect::<MergeResult<Vec<_>>>()?;
        let expr = AggregateExprBuilder::new(udaf, args)
            .schema(Arc::clone(&raw_schema))
            .alias(&agg.output)
            .build()
            .map_err(|e| logic(format!("MV state merge: build [{}]: {}", agg.output, e)))?;
        aggr_exprs.push(Arc::new(expr));
    }
    let max_udaf = state
        .udaf("max")
        .map_err(|e| logic(format!("MV state merge: max udaf: {}", e)))?;
    for field in &passthrough {
        let arg = col(field.name(), &raw_schema).map_err(|e| logic(format!("MV state merge passthrough: {}", e)))?;
        let expr = AggregateExprBuilder::new(Arc::clone(&max_udaf), vec![arg])
            .schema(Arc::clone(&raw_schema))
            .alias(field.name())
            .build()
            .map_err(|e| logic(format!("MV state merge: passthrough [{}]: {}", field.name(), e)))?;
        aggr_exprs.push(Arc::new(expr));
    }

    // State schema: keys, then each aggregate's state fields, positionally. Spec'd
    // aggregates read from convention-named columns (`o__st_i`); passthroughs read
    // from their own column (max's state is the value itself).
    let mut state_fields: Vec<Field> = Vec::new();
    let mut source_columns: Vec<String> = Vec::new();
    for key in &spec.key_columns {
        state_fields.push(input_schema.field_with_name(key).unwrap().clone());
        source_columns.push(key.clone());
    }
    for (idx, expr) in aggr_exprs.iter().enumerate() {
        let fields = expr
            .state_fields()
            .map_err(|e| logic(format!("MV state merge: state_fields: {}", e)))?;
        let is_passthrough = idx >= spec.aggs.len();
        for (i, f) in fields.iter().enumerate() {
            let source = if is_passthrough {
                passthrough[idx - spec.aggs.len()].name().to_string()
            } else {
                let convention = state_column_name(&spec.aggs[idx].output, i);
                if input_schema.field_with_name(&convention).is_err() {
                    return Err(logic(format!(
                        "MV state merge: missing state column [{}] for [{}]",
                        convention, spec.aggs[idx].output
                    )));
                }
                convention
            };
            state_fields.push(Field::new(&source, f.data_type().clone(), true));
            source_columns.push(source);
        }
    }
    let state_schema = Arc::new(ArrowSchema::new(state_fields));

    // Group-by: key columns sit at positions 0..k in the state schema.
    let group_exprs = spec
        .key_columns
        .iter()
        .map(|k| col(k, &state_schema).map(|e| (e, k.clone())))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| logic(format!("MV state merge: group expr: {}", e)))?;
    let group_by = PhysicalGroupBy::new_single(group_exprs);

    Ok(PartialMergePlan { aggr_exprs, group_by, state_schema, source_columns })
}

/// Reorders (and pads/casts) a segment batch into the state-schema column order.
/// Missing columns become nulls — a null state is a no-op for `merge_batch`.
fn project_to_state_schema(batch: &RecordBatch, plan: &PartialMergePlan) -> MergeResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(plan.source_columns.len());
    for (i, source) in plan.source_columns.iter().enumerate() {
        let target_type = plan.state_schema.field(i).data_type();
        let array = match batch.column_by_name(source) {
            Some(a) if a.data_type() == target_type => Arc::clone(a),
            Some(a) => cast(a, target_type)
                .map_err(|e| MergeError::Logic(format!("MV state merge: cast [{}]: {}", source, e)))?,
            None => new_null_array(target_type, batch.num_rows()),
        };
        columns.push(array);
    }
    RecordBatch::try_new(Arc::clone(&plan.state_schema), columns)
        .map_err(|e| MergeError::Logic(format!("MV state merge: project: {}", e)))
}

/// Merges MV state segments: reads the input parquet files, folds equal-key bucket
/// states with a `PartialReduce`-mode `AggregateExec`, and writes the collapsed
/// states as one output segment through the normal merge output path.
pub fn merge_partial_states(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    output_writer_generation: i64,
    spec: &MvStateSpec,
) -> MergeResult<super::MergeOutput> {
    let reservation = MemoryReservation::new(merge_pool(), "merge_partial_states", PoolBehavior::Reject);
    spec.validate_engine()?;
    log_debug!(
        "[RUST] DataFusion partial-state merge: {} input files, keys={:?}, {} aggs, output='{}'",
        input_files.len(),
        spec.key_columns,
        spec.aggs.len(),
        output_path
    );

    // Read input segments eagerly (same batch-volume profile as the aggregating
    // merge's collect); keep per-file batches as separate partitions.
    let mut arrow_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());
    let mut parquet_descriptors: Vec<SchemaDescriptor> = Vec::with_capacity(input_files.len());
    let mut file_generations: Vec<i64> = Vec::with_capacity(input_files.len());
    let mut file_row_counts: Vec<usize> = Vec::with_capacity(input_files.len());
    let mut file_batches: Vec<Vec<RecordBatch>> = Vec::with_capacity(input_files.len());
    for (file_idx, path) in input_files.iter().enumerate() {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let parquet_descr = builder.parquet_schema().clone();
        file_row_counts.push(builder.metadata().file_metadata().num_rows() as usize);
        file_generations.push(crate::writer_properties_builder::read_writer_generation(
            builder.metadata().file_metadata(),
            file_idx,
        ));
        let projection_indices = projection_indices_excluding_row_id(&schema);
        let projection = parquet::arrow::ProjectionMask::roots(&parquet_descr, projection_indices);
        let reader = builder.with_projection(projection).build()?;
        arrow_schemas.push(reader.schema().as_ref().clone());
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MergeError::Logic(format!("MV state merge read '{}': {}", path, e)))?;
        file_batches.push(batches);
        parquet_descriptors.push(parquet_descr);
    }
    let union_schema = ArrowSchema::try_merge(arrow_schemas.clone())
        .map_err(|e| MergeError::Logic(format!("MV state merge union schema: {}", e)))?;

    let plan = plan_partial_merge(spec, &union_schema)?;

    // Project every batch to the state schema, preserving per-file partitions.
    let partitions: Vec<Vec<RecordBatch>> = file_batches
        .iter()
        .map(|batches| batches.iter().map(|b| project_to_state_schema(b, &plan)).collect())
        .collect::<MergeResult<_>>()?;

    // ── PartialReduce: states in, states out — DataFusion folds every accumulator ──
    let n_aggs = plan.aggr_exprs.len();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| MergeError::Logic(format!("MV state merge runtime: {}", e)))?;
    let batches = runtime
        .block_on(async {
            let input = MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&plan.state_schema), None)?;
            let coalesced = Arc::new(CoalescePartitionsExec::new(input));
            let reduce = Arc::new(AggregateExec::try_new(
                AggregateMode::PartialReduce,
                plan.group_by.clone(),
                plan.aggr_exprs.clone(),
                vec![None; n_aggs],
                coalesced,
                Arc::clone(&plan.state_schema),
            )?);
            let ctx = SessionContext::new();
            datafusion::physical_plan::collect(reduce, ctx.task_ctx()).await
        })
        .map_err(|e| MergeError::Logic(format!("MV state merge datafusion: {}", e)))?;

    // ── Stream collapsed state batches into the standard output segment ──
    // Output columns are positionally the state schema; relabel with the segment
    // column names so the writer schema (derived from input segments) lines up, and
    // cast back to the segment's physical types — the mapping layer widens state
    // types (e.g. Int16 min-state stored as Int64 `long`), the reduce emits exact
    // accumulator types, and the round-trip must land on the segment schema again.
    let config_settings = crate::writer::SETTINGS_STORE
        .get(index_name)
        .map(|r| r.clone())
        .unwrap_or_default();
    let ctx_reservation = reservation.child("merge:flush");
    let mut out_ctx = MergeContext::new(
        arrow_schemas.clone(),
        &parquet_descriptors,
        output_path,
        index_name,
        config_settings.get_row_group_max_rows(),
        None,
        None,
        output_writer_generation,
        ctx_reservation,
    )?;
    let out_schema = out_ctx.data_schema().clone();
    let mut bucket_count: i64 = 0;
    for batch in batches {
        bucket_count += batch.num_rows() as i64;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
        for (i, column) in batch.columns().iter().enumerate() {
            let name = plan.state_schema.field(i).name().clone();
            let target_type = out_schema
                .field_with_name(&name)
                .map(|f| f.data_type().clone())
                .unwrap_or_else(|_| column.data_type().clone());
            let array = if column.data_type() == &target_type {
                Arc::clone(column)
            } else {
                cast(column, &target_type)
                    .map_err(|e| MergeError::Logic(format!("MV state merge: output cast [{}]: {}", name, e)))?
            };
            fields.push(Field::new(&name, target_type, true));
            columns.push(array);
        }
        let relabeled = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
            .map_err(|e| MergeError::Logic(format!("MV state merge relabel: {}", e)))?;
        let mapping = super::schema::ColumnMapping::new(relabeled.schema().as_ref(), &out_schema);
        out_ctx.push_batch(mapping.pad_batch(&relabeled)?)?;
    }
    let stats = out_ctx.finish()?;
    log_debug!(
        "[RUST] DataFusion partial-state merge complete: {} buckets, crc32={:#010x}",
        bucket_count,
        stats.crc32
    );

    // Row-id mapping: aggregation severs row provenance; MV mode secondaries merge
    // independently, so a degenerate mapping (all rows → 0) satisfies the interface.
    let total_rows: usize = file_row_counts.iter().sum();
    let mapping: Vec<i64> = vec![0i64; total_rows];
    let mut gen_keys: Vec<i64> = Vec::with_capacity(input_files.len());
    let mut gen_offsets: Vec<i32> = Vec::with_capacity(input_files.len());
    let mut gen_sizes: Vec<i32> = Vec::with_capacity(input_files.len());
    let mut offset: i32 = 0;
    for (file_idx, rows) in file_row_counts.iter().enumerate() {
        gen_keys.push(file_generations[file_idx]);
        gen_offsets.push(offset);
        gen_sizes.push(*rows as i32);
        offset += *rows as i32;
    }

    Ok(super::MergeOutput {
        mapping,
        gen_keys,
        gen_offsets,
        gen_sizes,
        metadata: stats.metadata,
        crc32: stats.crc32,
        flush_and_sort_chunk_count: 0,
        flush_and_sort_chunk_time_millis: 0,
        row_id_mapping_max: bucket_count,
    })
}


#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::compute::concat_batches;
    use datafusion::physical_plan::collect;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    /// Raw source schema: two group keys (k Utf8, p Int64), one measure (x Float64),
    /// one passthrough metadata column (_seq_no Int64).
    fn raw_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("p", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
            Field::new("_seq_no", DataType::Int64, true),
        ]))
    }

    fn raw_batch(rows: &[(&str, i64, f64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            raw_schema(),
            vec![
                Arc::new(StringArray::from(rows.iter().map(|r| Some(r.0)).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(rows.iter().map(|r| r.3).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    /// The spec under test: every mergeable class — additive (count/sum), comparison
    /// (min/max), ratio (avg), and second-moment (stddev). None get special handling.
    fn spec() -> MvStateSpec {
        MvStateSpec::parse(
            r#"{
                "key_columns": ["k", "p"],
                "aggs": [
                    {"output": "cnt", "fn": "count", "input_types": ["Float64"]},
                    {"output": "s",   "fn": "sum",   "input_types": ["Float64"]},
                    {"output": "lo",  "fn": "min",   "input_types": ["Float64"]},
                    {"output": "hi",  "fn": "max",   "input_types": ["Float64"]},
                    {"output": "a",   "fn": "avg",   "input_types": ["Float64"]},
                    {"output": "sd",  "fn": "stddev","input_types": ["Float64"]},
                    {"output": "ad",  "fn": "approx_distinct", "input_types": ["Int64"]}
                ]
            }"#,
        )
        .unwrap()
    }

    /// Spec-driven aggregate exprs against the raw schema — what the refresh path
    /// does: same UDAFs, same aliases, real input columns.
    fn refresh_exprs(spec: &MvStateSpec, schema: &SchemaRef) -> Vec<Arc<AggregateFunctionExpr>> {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let mut exprs = Vec::new();
        for agg in &spec.aggs {
            let udaf = state.udaf(&agg.function).unwrap();
            // Arg column by declared input type: Float64 → the measure, Int64 → _seq_no.
            let arg = if agg.input_types[0] == "Int64" { "_seq_no" } else { "x" };
            let expr = AggregateExprBuilder::new(udaf, vec![col(arg, schema).unwrap()])
                .schema(Arc::clone(schema))
                .alias(&agg.output)
                .build()
                .unwrap();
            exprs.push(Arc::new(expr));
        }
        // Passthrough _seq_no, as the refresh sink will carry it.
        let max_udaf = state.udaf("max").unwrap();
        exprs.push(Arc::new(
            AggregateExprBuilder::new(max_udaf, vec![col("_seq_no", schema).unwrap()])
                .schema(Arc::clone(schema))
                .alias("_seq_no")
                .build()
                .unwrap(),
        ));
        exprs
    }

    /// Simulates one refresh: raw rows → Partial-mode aggregation → state batch with
    /// convention column names → parquet segment.
    fn write_state_segment(dir: &TempDir, name: &str, spec: &MvStateSpec, raw: RecordBatch) -> String {
        let schema = raw.schema();
        let group_by = PhysicalGroupBy::new_single(vec![
            (col("k", &schema).unwrap(), "k".to_string()),
            (col("p", &schema).unwrap(), "p".to_string()),
        ]);
        let exprs = refresh_exprs(spec, &schema);
        let n = exprs.len();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let states = runtime
            .block_on(async {
                let input = MemorySourceConfig::try_new_exec(&[vec![raw]], Arc::clone(&schema), None)?;
                let partial = Arc::new(AggregateExec::try_new(
                    AggregateMode::Partial,
                    group_by,
                    exprs.clone(),
                    vec![None; n],
                    input,
                    Arc::clone(&schema),
                )?);
                let ctx = SessionContext::new();
                collect(partial, ctx.task_ctx()).await
            })
            .unwrap();
        let state_batch = concat_batches(&states[0].schema(), &states).unwrap();

        // Rename to the storage convention: keys keep their names; output `o`'s state
        // fields become o__st_0..n-1; the passthrough keeps its own name.
        let mut fields: Vec<Field> = vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("p", DataType::Int64, true),
        ];
        let mut col_idx = 2;
        for (i, expr) in exprs.iter().enumerate() {
            let state_fields = expr.state_fields().unwrap();
            for (j, f) in state_fields.iter().enumerate() {
                let name = if i < spec.aggs.len() {
                    state_column_name(&spec.aggs[i].output, j)
                } else {
                    "_seq_no".to_string()
                };
                fields.push(Field::new(&name, f.data_type().clone(), true));
                col_idx += 1;
            }
        }
        assert_eq!(col_idx, state_batch.num_columns(), "state layout drift");
        let renamed = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(fields)),
            state_batch.columns().to_vec(),
        )
        .unwrap();

        let path = dir.path().join(name);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, renamed.schema(), None).unwrap();
        writer.write(&renamed).unwrap();
        writer.close().unwrap();
        path.to_str().unwrap().to_string()
    }

    /// The read path: Final-mode aggregation over state segments, using the very same
    /// plan the merge builds — states in, finals out.
    fn finalize(spec: &MvStateSpec, state_files: &[String]) -> Vec<RecordBatch> {
        let mut schemas = Vec::new();
        let mut partitions: Vec<Vec<RecordBatch>> = Vec::new();
        for path in state_files {
            let file = File::open(path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
            schemas.push(reader.schema().as_ref().clone());
            partitions.push(reader.collect::<Result<Vec<_>, _>>().unwrap());
        }
        let union = ArrowSchema::try_merge(schemas).unwrap();
        let plan = plan_partial_merge(spec, &union).unwrap();
        let projected: Vec<Vec<RecordBatch>> = partitions
            .iter()
            .map(|bs| bs.iter().map(|b| project_to_state_schema(b, &plan).unwrap()).collect())
            .collect();
        let n = plan.aggr_exprs.len();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime
            .block_on(async {
                let input = MemorySourceConfig::try_new_exec(&projected, Arc::clone(&plan.state_schema), None)?;
                let coalesced = Arc::new(CoalescePartitionsExec::new(input));
                let final_agg = Arc::new(AggregateExec::try_new(
                    AggregateMode::Final,
                    plan.group_by.clone(),
                    plan.aggr_exprs.clone(),
                    vec![None; n],
                    coalesced,
                    Arc::clone(&plan.state_schema),
                )?);
                let ctx = SessionContext::new();
                collect(final_agg, ctx.task_ctx()).await
            })
            .unwrap()
    }

    /// Ground truth: single-shot aggregation over the union of all raw rows, using
    /// DataFusion's SQL path end to end.
    fn ground_truth(raws: Vec<RecordBatch>) -> Vec<RecordBatch> {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime
            .block_on(async {
                let ctx = SessionContext::new();
                let table = datafusion::datasource::MemTable::try_new(raw_schema(), vec![raws])?;
                ctx.register_table("t", Arc::new(table))?;
                ctx.sql(
                    "SELECT k, p, count(x) cnt, sum(x) s, min(x) lo, max(x) hi, avg(x) a, stddev(x) sd, \
                     approx_distinct(_seq_no) ad FROM t GROUP BY k, p",
                )
                .await?
                .collect()
                .await
            })
            .unwrap()
    }

    /// Extracts (k, p) → [f64 aggregate values] from result batches by column name.
    fn by_key(batches: &[RecordBatch], value_cols: &[&str]) -> std::collections::BTreeMap<(String, i64), Vec<Option<f64>>> {
        let mut out = std::collections::BTreeMap::new();
        for batch in batches {
            let k = batch.column_by_name("k").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let p = batch.column_by_name("p").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..batch.num_rows() {
                let mut vals = Vec::new();
                for name in value_cols {
                    let col = batch.column_by_name(name).unwrap();
                    let v = if col.is_null(row) {
                        None
                    } else {
                        Some(match col.data_type() {
                            DataType::Float64 => col.as_any().downcast_ref::<Float64Array>().unwrap().value(row),
                            DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().unwrap().value(row) as f64,
                            DataType::UInt64 => col
                                .as_any()
                                .downcast_ref::<arrow::array::UInt64Array>()
                                .unwrap()
                                .value(row) as f64,
                            other => panic!("unexpected result type {other}"),
                        })
                    };
                    vals.push(v);
                }
                out.insert((k.value(row).to_string(), p.value(row)), vals);
            }
        }
        out
    }

    fn assert_close(expected: &std::collections::BTreeMap<(String, i64), Vec<Option<f64>>>, actual: &std::collections::BTreeMap<(String, i64), Vec<Option<f64>>>) {
        assert_eq!(expected.keys().collect::<Vec<_>>(), actual.keys().collect::<Vec<_>>(), "bucket keys differ");
        for (key, exp) in expected {
            let act = &actual[key];
            for (i, (e, a)) in exp.iter().zip(act).enumerate() {
                match (e, a) {
                    (None, None) => {}
                    (Some(e), Some(a)) => {
                        assert!((e - a).abs() < 1e-9 * e.abs().max(1.0), "{key:?} col {i}: expected {e}, got {a}")
                    }
                    _ => panic!("{key:?} col {i}: null mismatch ({e:?} vs {a:?})"),
                }
            }
        }
    }

    const AGG_COLS: [&str; 7] = ["cnt", "s", "lo", "hi", "a", "sd", "ad"];

    /// The money test: two refreshes produce state segments with overlapping keys;
    /// the PartialReduce merge folds them; Final over the merged segment must equal a
    /// single-shot aggregation over all raw rows — for every aggregate class at once,
    /// including avg and stddev, with zero function-specific code anywhere.
    #[test]
    fn partial_merge_then_finalize_equals_single_shot() {
        let dir = TempDir::new().unwrap();
        let spec = spec();
        let rows1: Vec<(&str, i64, f64, i64)> = vec![
            ("200", 1, 10.0, 1),
            ("200", 1, 20.0, 2),
            ("500", 1, 90.0, 3),
            ("200", 2, 7.0, 4),
        ];
        let rows2: Vec<(&str, i64, f64, i64)> = vec![
            ("200", 1, 40.0, 5),
            ("200", 1, 55.5, 6),
            ("404", 1, 1.5, 7),
        ];
        let f1 = write_state_segment(&dir, "seg1.parquet", &spec, raw_batch(&rows1));
        let f2 = write_state_segment(&dir, "seg2.parquet", &spec, raw_batch(&rows2));
        let out = dir.path().join("merged.parquet");

        let result = merge_partial_states(
            &[f1, f2],
            out.to_str().unwrap(),
            "test_index",
            7,
            &spec,
        )
        .unwrap();

        // 4 distinct (k, p) buckets; inputs had 6 bucket rows across the two segments.
        assert_eq!(result.row_id_mapping_max, 4, "buckets after collapse");

        let finals = finalize(&spec, &[out.to_str().unwrap().to_string()]);
        let expected = ground_truth(vec![raw_batch(&rows1), raw_batch(&rows2)]);
        assert_close(&by_key(&expected, &AGG_COLS), &by_key(&finals, &AGG_COLS));
    }

    /// Finalizing UNMERGED segments must give the same answer as finalizing the merged
    /// one — collapsing is an optimization, never a correctness requirement.
    #[test]
    fn finalize_is_merge_invariant() {
        let dir = TempDir::new().unwrap();
        let spec = spec();
        let rows1: Vec<(&str, i64, f64, i64)> = vec![("a", 1, 3.0, 1), ("b", 1, 4.0, 2)];
        let rows2: Vec<(&str, i64, f64, i64)> = vec![("a", 1, 5.0, 3), ("a", 2, 6.0, 4)];
        let f1 = write_state_segment(&dir, "s1.parquet", &spec, raw_batch(&rows1));
        let f2 = write_state_segment(&dir, "s2.parquet", &spec, raw_batch(&rows2));

        let unmerged = finalize(&spec, &[f1.clone(), f2.clone()]);

        let out = dir.path().join("m.parquet");
        merge_partial_states(&[f1, f2], out.to_str().unwrap(), "test_index", 7, &spec).unwrap();
        let merged = finalize(&spec, &[out.to_str().unwrap().to_string()]);

        assert_close(&by_key(&unmerged, &AGG_COLS), &by_key(&merged, &AGG_COLS));
    }

    /// The partition column is part of the key: equal group keys in different
    /// partitions never collapse.
    #[test]
    fn never_collapses_across_partitions() {
        let dir = TempDir::new().unwrap();
        let spec = spec();
        let f1 = write_state_segment(&dir, "s1.parquet", &spec, raw_batch(&[("200", 1, 2.0, 1)]));
        let f2 = write_state_segment(&dir, "s2.parquet", &spec, raw_batch(&[("200", 2, 3.0, 2)]));
        let out = dir.path().join("m.parquet");

        let result = merge_partial_states(&[f1, f2], out.to_str().unwrap(), "test_index", 7, &spec).unwrap();
        assert_eq!(result.row_id_mapping_max, 2, "different partitions stay separate");
    }

    /// A segment missing a spec'd state column is not silently mis-merged: the merge
    /// errors and the caller falls back to concatenation.
    #[test]
    fn missing_state_column_is_an_error() {
        let dir = TempDir::new().unwrap();
        let spec = spec();
        // Write a segment with only the keys — no state columns at all.
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("p", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("200")])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let path = dir.path().join("bad.parquet");
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let out = dir.path().join("m.parquet");
        let err = match merge_partial_states(
            &[path.to_str().unwrap().to_string()],
            out.to_str().unwrap(),
            "test_index",
            7,
            &spec,
        ) {
            Ok(_) => panic!("expected missing-state-column error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("missing state column"), "got: {err}");
    }

    /// Mapping layers widen state types (Int16 min-state stored as `long`/Int64). The
    /// merge must cast segment→state for the fold and state→segment on write, so the
    /// round trip lands back on the segment schema.
    #[test]
    fn widened_segment_types_round_trip() {
        let dir = TempDir::new().unwrap();
        let spec = MvStateSpec::parse(
            r#"{"key_columns": ["k"],
                "aggs": [{"output": "lo", "fn": "min", "input_types": ["Int16"]}]}"#,
        )
        .unwrap();
        // Segment stores the Int16 min-state widened to Int64 (mapping type `long`).
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("lo__st_0", DataType::Int64, true),
        ]));
        let write = |name: &str, rows: Vec<(&str, i64)>| -> String {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(rows.iter().map(|r| Some(r.0)).collect::<Vec<_>>())),
                    Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
                ],
            )
            .unwrap();
            let path = dir.path().join(name);
            let file = File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            path.to_str().unwrap().to_string()
        };
        let f1 = write("a.parquet", vec![("x", 7), ("y", 3)]);
        let f2 = write("b.parquet", vec![("x", 2)]);
        let out = dir.path().join("m.parquet");

        let result = merge_partial_states(&[f1, f2], out.to_str().unwrap(), "test_index", 7, &spec).unwrap();
        assert_eq!(result.row_id_mapping_max, 2, "x collapses, y stays");

        // Output column must be back at the segment's Int64, with min folded.
        let file = File::open(out.to_str().unwrap()).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let all = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let lo = all.column_by_name("lo__st_0").unwrap();
        assert_eq!(lo.data_type(), &DataType::Int64, "state written back at segment type");
        let k = all.column_by_name("k").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let lo = lo.as_any().downcast_ref::<Int64Array>().unwrap();
        let row_x = (0..all.num_rows()).find(|&i| k.value(i) == "x").unwrap();
        assert_eq!(lo.value(row_x), 2, "min folded across widened columns");
    }

    #[test]
    fn spec_rejects_malformed_json() {
        assert!(MvStateSpec::parse("{\"key_columns\": [}").is_err());
        assert!(MvStateSpec::parse("{}").is_err());
    }

    /// A spec stamped by a different engine version must refuse to fold — the merge
    /// then falls back to concatenation, preserving the segments for a matching build.
    #[test]
    fn engine_version_mismatch_refuses_fold() {
        let spec = MvStateSpec::parse(
            r#"{"engine": {"datafusion": "999.0.0", "layout": 1},
                "key_columns": ["k"],
                "aggs": [{"output": "s", "fn": "sum", "input_types": ["Int64"]}]}"#,
        )
        .unwrap();
        let err = spec.validate_engine().err().expect("mismatch must refuse");
        assert!(err.to_string().contains("refusing to fold"), "got: {err}");

        let layout_drift = MvStateSpec::parse(&format!(
            r#"{{"engine": {{"datafusion": "{}", "layout": 999}},
                "key_columns": ["k"],
                "aggs": [{{"output": "s", "fn": "sum", "input_types": ["Int64"]}}]}}"#,
            datafusion::DATAFUSION_VERSION
        ))
        .unwrap();
        assert!(layout_drift.validate_engine().is_err());

        let current = MvStateSpec::parse(&format!(
            r#"{{"engine": {{"datafusion": "{}", "layout": {}}},
                "key_columns": ["k"],
                "aggs": [{{"output": "s", "fn": "sum", "input_types": ["Int64"]}}]}}"#,
            datafusion::DATAFUSION_VERSION,
            STATE_LAYOUT_VERSION
        ))
        .unwrap();
        assert!(current.validate_engine().is_ok());

        // Specs without an engine stamp (pre-versioning) stay foldable.
        let unstamped = MvStateSpec::parse(
            r#"{"key_columns": ["k"], "aggs": [{"output": "s", "fn": "sum", "input_types": ["Int64"]}]}"#,
        )
        .unwrap();
        assert!(unstamped.validate_engine().is_ok());
    }
}
