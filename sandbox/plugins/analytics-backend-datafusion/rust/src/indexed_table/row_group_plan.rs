/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The single per-row-group decision point (PR2 section D).
//!
//! The Java planner decides the plan shape once and ships [`FastPathHints`].
//! Rust makes exactly one decision per row group from footer stats + the hints
//! + `has_deletes`, via [`classify_row_group`]. There is no plan-shape sniffing
//! here — only a footer/range comparison and a fail-closed match on the
//! planner's declared shape.
//!
//! This module replaces the original branch's three parallel `*_within_rgs`
//! `HashSet`s + a `pushdown_predicate_sans_sort_range` `Option` with a single
//! [`RowGroupPlan`] per row group, plus the pure residual rewrite
//! [`strip_sort_range_conjuncts`] used by the `TimestampStripped` plan.

use std::sync::Arc;

use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use datafusion::parquet::file::statistics::Statistics;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;

use crate::fast_path_hints::{FastPathHints, FastPathShape, SortRange};

/// The plan Rust follows for a single row group. `HistogramBucket` arrives in
/// PR3 Stage C.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowGroupPlan {
    /// The whole RG is inside the sort range and the shape is `count(*)` over a
    /// range+delegated filter: answer from the index (countDocs upcall / footer
    /// num_rows), no Parquet decode.
    CountFromIndex,
    /// The whole RG is inside the sort range and the residual is strippable:
    /// drop the sort-range conjunct + the sort column from this RG's decode.
    TimestampStripped,
    /// The whole RG is inside the sort range and the fragment is a path-safe
    /// Sort+Limit: keep only the `budget` candidate rows that a global top-K
    /// could retain from this RG — the LAST `budget` when `keep_last` (query
    /// sort opposes the index order), else the FIRST `budget`. Decodes on the
    /// Full path with the truncated candidate set.
    TopKTruncated { keep_last: bool, budget: u32 },
    /// The whole RG is inside the sort range and the shape is a `span()`
    /// histogram whose bucket function maps this RG's footer min and max to the
    /// SAME bucket value `b` — every row in the RG lands in bucket `b`. Emit
    /// `(b, count)` from the index, no Parquet decode. A boundary RG (min and
    /// max in different buckets) fails closed to [`RowGroupPlan::Full`].
    HistogramBucket(i64),
    /// Decode as usual.
    Full,
}

/// The histogram bucket function reconstructed from the planner's hints — the
/// SAME arithmetic DataFusion runs for `span()`/`date_trunc`, so evaluating it
/// on a WITHIN RG's footer min/max decides interior vs boundary exactly.
///
/// `op` is the wire-stable u8 mirroring Java `FastPathHintSpec.BucketOp`
/// (0 NONE, 1 DIV, 2 ADD, 3 SUB, 4 MUL, 5 FLOOR_TO_MULTIPLE). `operand` is the
/// literal `N`, already coerced from the declared unit into the column's
/// PHYSICAL unit (via [`crate::fast_path_hints::coerce_ticks_to_physical`]).
///
/// Division mirrors DataFusion's Int64 `/` EXACTLY: arrow-array 59.2
/// `NativeArithmeticOp::div_checked` calls `i64::checked_div`, i.e. native
/// integer division that truncates TOWARD ZERO (verified in the cargo
/// registry). PPL `span(ts, N)` lowers to `(col / N) * N`, so
/// FLOOR_TO_MULTIPLE is `checked_div(N)` then `checked_mul(N)` with the same
/// toward-zero truncation — NOT floor-toward-negative-infinity. For negative
/// `v` these differ (e.g. `-5 / 3 * 3 == -3`, not `-6`), so we must match the
/// engine, not intuition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketFn {
    op: u8,
    operand: i64,
}

impl BucketFn {
    /// Build the bucket function from the planner hints and the sort column's
    /// PHYSICAL unit. `None` when the op is NONE/unknown, when a DIV/FLOOR/MUL
    /// operand is not strictly positive (a non-monotone or degenerate bucket),
    /// or when the operand cannot be represented in the physical unit.
    pub fn from_hints(hints: &FastPathHints, physical_unit: TimeUnit) -> Option<BucketFn> {
        let op = hints.histogram_bucket_op;
        if op == 0 || op > 5 {
            return None;
        }
        let operand = crate::fast_path_hints::coerce_ticks_to_physical(
            hints,
            physical_unit,
            hints.histogram_bucket_operand,
        )?;
        // DIV / MUL / FLOOR_TO_MULTIPLE require a strictly positive operand:
        // zero divides-by-zero or collapses every value, negative flips
        // monotonicity — none form a valid histogram bucketing. ADD/SUB are a
        // pure shift and accept any operand.
        if matches!(op, 1 | 4 | 5) && operand <= 0 {
            return None;
        }
        Some(BucketFn { op, operand })
    }

    /// Evaluate the bucket for a single physical-unit value, matching
    /// DataFusion's arithmetic (checked, truncating toward zero). `None` on
    /// overflow.
    pub fn eval(&self, v: i64) -> Option<i64> {
        match self.op {
            1 => v.checked_div(self.operand),
            2 => v.checked_add(self.operand),
            3 => v.checked_sub(self.operand),
            4 => v.checked_mul(self.operand),
            5 => v.checked_div(self.operand)?.checked_mul(self.operand),
            _ => None,
        }
    }
}

/// Footer statistics for a single row group's SORT column, already normalized
/// to the PHYSICAL Parquet unit (so they compare directly against a
/// [`SortRange`] built by [`crate::fast_path_hints::sort_range_in_physical_unit`]).
///
/// `min`/`max`/`null_count` are `Option` because a row group may not carry the
/// statistic; a missing statistic fails closed to [`RowGroupPlan::Full`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowGroupFooter {
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub null_count: Option<i64>,
    pub num_rows: i64,
}

impl RowGroupFooter {
    /// The RG is WITHIN the range iff it has no nulls in the sort column and
    /// `[min, max] ⊆ [lower, upper]`. A missing min/max/null_count => not
    /// WITHIN (fail closed). Only then are the sort-range conjuncts a tautology
    /// over the whole RG, making the count / strip shortcuts sound.
    fn is_within(&self, range: &SortRange) -> bool {
        self.null_count == Some(0)
            && matches!(self.min, Some(min) if range.lower_inclusive <= min)
            && matches!(self.max, Some(max) if max <= range.upper_inclusive)
    }
}

/// The single per-row-group decision.
///
/// Precedence (each rule fails closed to [`RowGroupPlan::Full`]):
/// 1. `has_deletes` (shard-level, from #22910) => `Full`. Fixes 1/2/3 are
///    unsound on shards with deletions (footer counts include deleted docs).
/// 2. no `range`, or the footer is not WITHIN it => `Full`.
/// 3. WITHIN && `shape == COUNT_ONLY` => `CountFromIndex`.
/// 4. WITHIN && `shape == TOPK` && `topk_path_safe` && `topk_budget > 0`
///    => `TopKTruncated` (per-RG candidate truncation is sound only inside the
///    sort range, and only when the Sort→Scan path can't reorder/fan out rows).
/// 5. WITHIN && `shape == HISTOGRAM` && a `bucket` fn was reconstructed &&
///    `bucket(min) == bucket(max)` => `HistogramBucket(b)` (interior RG — every
///    row shares bucket `b`). A boundary RG (endpoints in different buckets, or
///    an overflow) => `Full`.
/// 6. WITHIN && `range_conjunct_strippable` => `TimestampStripped`.
/// 7. otherwise => `Full`.
pub fn classify_row_group(
    footer: &RowGroupFooter,
    range: Option<&SortRange>,
    hints: &FastPathHints,
    has_deletes: bool,
    bucket: Option<&BucketFn>,
) -> RowGroupPlan {
    if has_deletes {
        return RowGroupPlan::Full;
    }
    let Some(range) = range else {
        return RowGroupPlan::Full;
    };
    if !footer.is_within(range) {
        return RowGroupPlan::Full;
    }
    match hints.shape {
        FastPathShape::CountOnly => RowGroupPlan::CountFromIndex,
        FastPathShape::TopK if hints.topk_path_safe() && hints.topk_budget > 0 => {
            RowGroupPlan::TopKTruncated {
                keep_last: hints.topk_keep_last(),
                budget: hints.topk_budget,
            }
        }
        // A WITHIN RG guarantees min/max are Some. Interior iff both endpoints
        // bucket-equal; otherwise it straddles a bucket boundary => decode.
        // Negative domain fails closed: `date_trunc` floors toward -inf while
        // `(col/N)*N` truncates toward zero, so the two lowerings only agree for
        // min >= 0 (always true for real epoch timestamps).
        FastPathShape::Histogram => match (bucket, footer.min, footer.max) {
            (Some(b), Some(min), Some(max)) if min >= 0 => match (b.eval(min), b.eval(max)) {
                (Some(b_min), Some(b_max)) if b_min == b_max => {
                    RowGroupPlan::HistogramBucket(b_min)
                }
                _ => RowGroupPlan::Full,
            },
            _ => RowGroupPlan::Full,
        },
        _ if hints.range_conjunct_strippable() => RowGroupPlan::TimestampStripped,
        _ => RowGroupPlan::Full,
    }
}

/// Build a [`RowGroupFooter`] for one row group's sort column from footer
/// `ColumnChunkMetaData` statistics only — no page-index IO (these stats are
/// always fetched with the metadata). Fail-closed: missing statistics or an
/// unsupported physical type leave `min`/`max` as `None`, which
/// [`RowGroupFooter::is_within`] treats as "not WITHIN".
///
/// `sort_col_idx` is the parquet *leaf* column index of the sort column (for a
/// top-level primitive timestamp this equals its schema field index).
pub fn row_group_footer(rg_meta: &RowGroupMetaData, sort_col_idx: usize) -> RowGroupFooter {
    let num_rows = rg_meta.num_rows();
    let (min, max, null_count) = match rg_meta.column(sort_col_idx).statistics() {
        Some(stats) => {
            // `date` (millis) and `date_nanos` (nanos) are both physically Int64;
            // the raw tick value is what we compare against the physical-unit range.
            let (min, max) = match stats {
                Statistics::Int32(s) => (
                    s.min_opt().map(|v| *v as i64),
                    s.max_opt().map(|v| *v as i64),
                ),
                Statistics::Int64(s) => (s.min_opt().copied(), s.max_opt().copied()),
                _ => (None, None),
            };
            (min, max, stats.null_count_opt().map(|n| n as i64))
        }
        None => (None, None, None),
    };
    RowGroupFooter {
        min,
        max,
        null_count,
        num_rows,
    }
}

/// Build the per-row-group plan vector for one segment — index = RG ordinal, so
/// `plans[rg.index]` is the plan for that row group.
///
/// Cheap all-[`RowGroupPlan::Full`] exit (no footer read at all) when there is
/// nothing a fast path could gain: `has_deletes` (Fixes 1/2/3 unsound under
/// deletions), no `range`, no resolvable `sort_col_idx`, or a NONE shape whose
/// residual isn't strippable. Otherwise each RG is classified from its footer
/// via [`classify_row_group`].
pub fn plan_row_groups(
    metadata: &ParquetMetaData,
    sort_col_idx: Option<usize>,
    hints: &FastPathHints,
    range: Option<&SortRange>,
    has_deletes: bool,
    bucket: Option<&BucketFn>,
) -> Vec<RowGroupPlan> {
    let n = metadata.num_row_groups();
    let nothing_to_gain = has_deletes
        || range.is_none()
        || sort_col_idx.is_none()
        || (hints.shape == FastPathShape::None && !hints.range_conjunct_strippable())
        // Histogram shape gains nothing when the bucket fn could not be built.
        || (hints.shape == FastPathShape::Histogram && bucket.is_none());
    if nothing_to_gain {
        return vec![RowGroupPlan::Full; n];
    }
    let sort_col_idx = sort_col_idx.unwrap();
    (0..n)
        .map(|rg| {
            let footer = row_group_footer(metadata.row_group(rg), sort_col_idx);
            classify_row_group(&footer, range, hints, has_deletes, bucket)
        })
        .collect()
}

/// Read a physical `Literal` as an `i64` tick count (plain integer or any
/// timestamp unit). Sub-integer / non-numeric literals => `None`.
fn scalar_as_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::TimestampSecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        _ => None,
    }
}

/// True when `expr` consists SOLELY of conjunctive range comparisons
/// (`>`, `>=`, `<`, `<=`) between `column` and integer/timestamp literals.
/// Only such a residual is a tautology on a WITHIN row group, so only such a
/// conjunct is safe to strip. OR/NOT, casts, other columns, and non-comparison
/// operators all return false (fail closed).
fn physical_expr_is_sort_range_only(expr: &Arc<dyn PhysicalExpr>, column: &str) -> bool {
    let Some(binary) = expr.as_ref().downcast_ref::<BinaryExpr>() else {
        return false;
    };
    match binary.op() {
        Operator::And => {
            physical_expr_is_sort_range_only(binary.left(), column)
                && physical_expr_is_sort_range_only(binary.right(), column)
        }
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
            let column_vs_literal = |a: &Arc<dyn PhysicalExpr>, b: &Arc<dyn PhysicalExpr>| {
                a.downcast_ref::<Column>()
                    .is_some_and(|c| c.name() == column)
                    && b.downcast_ref::<Literal>()
                        .is_some_and(|l| scalar_as_i64(l.value()).is_some())
            };
            column_vs_literal(binary.left(), binary.right())
                || column_vs_literal(binary.right(), binary.left())
        }
        _ => false,
    }
}

/// Strip every top-level AND conjunct that is a pure sort-range comparison on
/// `sort_col`, returning the residual (the AND of the surviving conjuncts), or
/// `None` when every conjunct was a sort-range conjunct (fully stripped).
///
/// This is the pure `TimestampStripped` rewrite: on a WITHIN row group the
/// sort-range conjunct is a tautology, so removing it and dropping the sort
/// column from the projection is sound and avoids decoding it. Fail-closed:
/// only top-level `AND` nodes are split; any other node (bare comparison, `OR`,
/// `NOT`, cast, non-literal bound) is one opaque conjunct — a sort-range one is
/// dropped, anything else is kept verbatim.
pub fn strip_sort_range_conjuncts(
    expr: &Arc<dyn PhysicalExpr>,
    sort_col: &str,
) -> Option<Arc<dyn PhysicalExpr>> {
    fn collect_conjuncts(e: &Arc<dyn PhysicalExpr>, out: &mut Vec<Arc<dyn PhysicalExpr>>) {
        if let Some(b) = e.as_ref().downcast_ref::<BinaryExpr>() {
            if matches!(b.op(), Operator::And) {
                collect_conjuncts(b.left(), out);
                collect_conjuncts(b.right(), out);
                return;
            }
        }
        out.push(Arc::clone(e));
    }
    let mut conjuncts = Vec::new();
    collect_conjuncts(expr, &mut conjuncts);

    let kept: Vec<Arc<dyn PhysicalExpr>> = conjuncts
        .into_iter()
        .filter(|c| !physical_expr_is_sort_range_only(c, sort_col))
        .collect();

    if kept.is_empty() {
        return None;
    }
    let mut it = kept.into_iter();
    let first = it.next().unwrap();
    Some(it.fold(first, |acc, e| {
        Arc::new(BinaryExpr::new(acc, Operator::And, e)) as Arc<dyn PhysicalExpr>
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{col, lit};

    fn hints(shape: FastPathShape, strippable: bool) -> FastPathHints {
        // Build via the wire round-trip so the private flags map exactly like
        // production (bit0 range_on_leading_sort_field, bit1 strippable).
        let flags = 0b01u8 | if strippable { 0b10 } else { 0 };
        let mut b = [0u8; crate::fast_path_hints::FASTPATHHINTS_BYTE_SIZE];
        b[0] = 1;
        b[1] = match shape {
            FastPathShape::None => 0,
            FastPathShape::CountOnly => 1,
            FastPathShape::TopK => 2,
            FastPathShape::Histogram => 3,
        };
        b[2] = flags;
        b[3] = 2; // millis
        b[8..16].copy_from_slice(&i64::MIN.to_le_bytes());
        b[16..24].copy_from_slice(&i64::MAX.to_le_bytes());
        // SAFETY: fixed 40-byte buffer written per the documented layout.
        unsafe { FastPathHints::from_ffm_ptr(b.as_ptr() as i64) }
    }

    fn footer(min: i64, max: i64, nulls: i64, rows: i64) -> RowGroupFooter {
        RowGroupFooter {
            min: Some(min),
            max: Some(max),
            null_count: Some(nulls),
            num_rows: rows,
        }
    }

    fn range(lo: i64, hi: i64) -> SortRange {
        SortRange {
            lower_inclusive: lo,
            upper_inclusive: hi,
        }
    }

    #[test]
    fn deletes_force_full() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hints(FastPathShape::CountOnly, true),
                true,
                None
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn no_range_is_full() {
        let f = footer(10, 20, 0, 100);
        assert_eq!(
            classify_row_group(
                &f,
                None,
                &hints(FastPathShape::CountOnly, true),
                false,
                None
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn not_within_is_full() {
        // max 20 > upper 15 => not within.
        let f = footer(10, 20, 0, 100);
        let r = range(0, 15);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hints(FastPathShape::CountOnly, true),
                false,
                None
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn nulls_break_within() {
        let f = footer(10, 20, 3, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hints(FastPathShape::CountOnly, true),
                false,
                None
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn missing_min_breaks_within() {
        let f = RowGroupFooter {
            min: None,
            max: Some(20),
            null_count: Some(0),
            num_rows: 100,
        };
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hints(FastPathShape::CountOnly, true),
                false,
                None
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn within_count_only_is_count_from_index() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hints(FastPathShape::CountOnly, false),
                false,
                None
            ),
            RowGroupPlan::CountFromIndex
        );
    }

    #[test]
    fn within_strippable_is_timestamp_stripped() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(&f, Some(&r), &hints(FastPathShape::None, true), false, None),
            RowGroupPlan::TimestampStripped
        );
    }

    #[test]
    fn within_not_strippable_not_count_is_full() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hints(FastPathShape::None, false),
                false,
                None
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn count_only_takes_precedence_over_strippable() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hints(FastPathShape::CountOnly, true),
                false,
                None
            ),
            RowGroupPlan::CountFromIndex
        );
    }

    // ── TopK classification ─────────────────────────────────────────

    /// TOPK hints with explicit path_safe / budget / keep_last, built through
    /// the wire round-trip so the private flags map exactly like production
    /// (bit0 range_on_leading_sort_field, bit2 topk_keep_last, bit3 path_safe).
    fn topk_hints(path_safe: bool, budget: u32, keep_last: bool) -> FastPathHints {
        let mut flags = 0b01u8; // range_on_leading_sort_field
        if keep_last {
            flags |= 0b0100;
        }
        if path_safe {
            flags |= 0b1000;
        }
        let mut b = [0u8; crate::fast_path_hints::FASTPATHHINTS_BYTE_SIZE];
        b[0] = 1;
        b[1] = 2; // TOPK
        b[2] = flags;
        b[3] = 2; // millis
        b[4..8].copy_from_slice(&budget.to_le_bytes());
        b[8..16].copy_from_slice(&i64::MIN.to_le_bytes());
        b[16..24].copy_from_slice(&i64::MAX.to_le_bytes());
        // SAFETY: fixed 40-byte buffer written per the documented layout.
        unsafe { FastPathHints::from_ffm_ptr(b.as_ptr() as i64) }
    }

    #[test]
    fn within_topk_path_safe_is_truncated() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(&f, Some(&r), &topk_hints(true, 5, true), false, None),
            RowGroupPlan::TopKTruncated {
                keep_last: true,
                budget: 5
            }
        );
    }

    #[test]
    fn within_topk_keep_first_variant() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(&f, Some(&r), &topk_hints(true, 3, false), false, None),
            RowGroupPlan::TopKTruncated {
                keep_last: false,
                budget: 3
            }
        );
    }

    #[test]
    fn topk_not_path_safe_is_full() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(&f, Some(&r), &topk_hints(false, 5, true), false, None),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn topk_zero_budget_is_full() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(&f, Some(&r), &topk_hints(true, 0, true), false, None),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn topk_deletes_force_full() {
        let f = footer(10, 20, 0, 100);
        let r = range(0, 100);
        assert_eq!(
            classify_row_group(&f, Some(&r), &topk_hints(true, 5, true), true, None),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn topk_not_within_is_full() {
        // max 20 > upper 15 => not within, so no truncation even when path-safe.
        let f = footer(10, 20, 0, 100);
        let r = range(0, 15);
        assert_eq!(
            classify_row_group(&f, Some(&r), &topk_hints(true, 5, true), false, None),
            RowGroupPlan::Full
        );
    }

    // ── Histogram BucketFn + classification ─────────────────────────

    /// HISTOGRAM hints carrying a bucket op/operand in the DECLARED unit, built
    /// through the wire layout (offset 24 = op, offset 32 = operand). Range unit
    /// = millis so identity-coerces against an Int64/millis physical column.
    fn hist_hints(op: u8, operand: i64) -> FastPathHints {
        let mut b = [0u8; crate::fast_path_hints::FASTPATHHINTS_BYTE_SIZE];
        b[0] = 1;
        b[1] = 3; // HISTOGRAM
        b[2] = 0b01; // range_on_leading_sort_field
        b[3] = 2; // millis
        b[8..16].copy_from_slice(&i64::MIN.to_le_bytes());
        b[16..24].copy_from_slice(&i64::MAX.to_le_bytes());
        b[24] = op;
        b[32..40].copy_from_slice(&operand.to_le_bytes());
        // SAFETY: fixed 40-byte buffer written per the documented layout.
        unsafe { FastPathHints::from_ffm_ptr(b.as_ptr() as i64) }
    }

    fn bucket(op: u8, operand: i64) -> BucketFn {
        BucketFn::from_hints(&hist_hints(op, operand), TimeUnit::Millisecond)
            .expect("bucket fn builds")
    }

    #[test]
    fn bucketfn_ops_match_datafusion_toward_zero() {
        // DIV truncates toward zero (arrow div_checked == i64::checked_div).
        assert_eq!(bucket(1, 3).eval(7), Some(2));
        assert_eq!(bucket(1, 3).eval(-5), Some(-1)); // -1, NOT floor -2
                                                     // FLOOR_TO_MULTIPLE = (v/N)*N with toward-zero division (PPL span()).
        assert_eq!(bucket(5, 100).eval(150), Some(100));
        assert_eq!(bucket(5, 100).eval(199), Some(100));
        assert_eq!(bucket(5, 100).eval(-50), Some(0)); // -50/100 = 0 => 0
        assert_eq!(bucket(5, 100).eval(-150), Some(-100)); // NOT floor -200
                                                           // ADD / SUB / MUL.
        assert_eq!(bucket(2, 10).eval(5), Some(15));
        assert_eq!(bucket(3, 10).eval(5), Some(-5));
        assert_eq!(bucket(4, 4).eval(-3), Some(-12));
    }

    #[test]
    fn bucketfn_from_hints_fail_closed() {
        // op NONE / unknown => no bucket.
        assert!(BucketFn::from_hints(&hist_hints(0, 100), TimeUnit::Millisecond).is_none());
        assert!(BucketFn::from_hints(&hist_hints(9, 100), TimeUnit::Millisecond).is_none());
        // DIV/MUL/FLOOR require strictly positive operand.
        assert!(BucketFn::from_hints(&hist_hints(1, 0), TimeUnit::Millisecond).is_none());
        assert!(BucketFn::from_hints(&hist_hints(5, -100), TimeUnit::Millisecond).is_none());
        assert!(BucketFn::from_hints(&hist_hints(4, 0), TimeUnit::Millisecond).is_none());
    }

    #[test]
    fn bucketfn_operand_coerced_to_physical_unit() {
        // declared millis, physical nanos: operand 1 ms => 1_000_000 ns, so
        // eval matches DataFusion running on the nanos column.
        let b = BucketFn::from_hints(&hist_hints(5, 1), TimeUnit::Nanosecond).unwrap();
        assert_eq!(b.eval(2_500_000), Some(2_000_000));
    }

    #[test]
    fn within_histogram_interior_is_bucket() {
        // RG [150..199] all floor-to-100 => bucket 100.
        let f = footer(150, 199, 0, 100);
        let r = range(0, 1000);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hist_hints(5, 100),
                false,
                Some(&bucket(5, 100))
            ),
            RowGroupPlan::HistogramBucket(100)
        );
    }

    #[test]
    fn within_histogram_boundary_is_full() {
        // RG [190..210] straddles the 100/200 boundary => decode.
        let f = footer(190, 210, 0, 100);
        let r = range(0, 1000);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hist_hints(5, 100),
                false,
                Some(&bucket(5, 100))
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn within_histogram_negative_domain_fails_closed() {
        // RG [-80..-10]: toward-zero div buckets to 0, date_trunc-style floor to -100.
        // The two span lowerings disagree below zero, so the RG must decode.
        let f = footer(-80, -10, 0, 100);
        let r = range(-1000, 1000);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hist_hints(5, 100),
                false,
                Some(&bucket(5, 100))
            ),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn histogram_without_bucket_is_full() {
        let f = footer(150, 199, 0, 100);
        let r = range(0, 1000);
        assert_eq!(
            classify_row_group(&f, Some(&r), &hist_hints(5, 100), false, None),
            RowGroupPlan::Full
        );
    }

    #[test]
    fn histogram_deletes_force_full() {
        let f = footer(150, 199, 0, 100);
        let r = range(0, 1000);
        assert_eq!(
            classify_row_group(
                &f,
                Some(&r),
                &hist_hints(5, 100),
                true,
                Some(&bucket(5, 100))
            ),
            RowGroupPlan::Full
        );
    }

    // ── strip_sort_range_conjuncts ──────────────────────────────────

    fn ts_schema() -> Schema {
        Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("status", DataType::Int64, false),
        ])
    }

    fn cmp(schema: &Schema, col_name: &str, op: Operator, v: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(col(col_name, schema).unwrap(), op, lit(v)))
    }

    fn and(a: Arc<dyn PhysicalExpr>, b: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(a, Operator::And, b))
    }

    #[test]
    fn strip_removes_sole_sort_range_returns_none() {
        let s = ts_schema();
        let expr = and(
            cmp(&s, "ts", Operator::GtEq, 100),
            cmp(&s, "ts", Operator::Lt, 200),
        );
        assert!(strip_sort_range_conjuncts(&expr, "ts").is_none());
    }

    #[test]
    fn strip_keeps_non_sort_residual() {
        let s = ts_schema();
        // ts >= 100 AND status > 5  => keeps `status > 5`.
        let residual = cmp(&s, "status", Operator::Gt, 5);
        let expr = and(cmp(&s, "ts", Operator::GtEq, 100), residual.clone());
        let out = strip_sort_range_conjuncts(&expr, "ts").expect("residual survives");
        // The surviving residual is the single non-sort conjunct.
        let b = out.as_ref().downcast_ref::<BinaryExpr>().unwrap();
        assert!(matches!(b.op(), Operator::Gt));
        assert!(b.left().downcast_ref::<Column>().unwrap().name() == "status");
    }

    #[test]
    fn strip_leaves_untouched_when_no_sort_conjunct() {
        let s = ts_schema();
        let expr = cmp(&s, "status", Operator::Gt, 5);
        let out = strip_sort_range_conjuncts(&expr, "ts").expect("kept");
        let b = out.as_ref().downcast_ref::<BinaryExpr>().unwrap();
        assert!(b.left().downcast_ref::<Column>().unwrap().name() == "status");
    }

    #[test]
    fn strip_does_not_split_under_or() {
        let s = ts_schema();
        // (ts >= 100 OR status > 5): a bare OR is one opaque conjunct, not a
        // sort-range-only conjunct, so it is kept verbatim.
        let or = Arc::new(BinaryExpr::new(
            cmp(&s, "ts", Operator::GtEq, 100),
            Operator::Or,
            cmp(&s, "status", Operator::Gt, 5),
        )) as Arc<dyn PhysicalExpr>;
        let out = strip_sort_range_conjuncts(&or, "ts").expect("OR kept");
        assert!(matches!(
            out.as_ref().downcast_ref::<BinaryExpr>().unwrap().op(),
            Operator::Or
        ));
    }

    #[test]
    fn strip_keeps_range_on_other_column() {
        let s = ts_schema();
        // status range must NOT be stripped when sort_col is ts.
        let expr = and(
            cmp(&s, "ts", Operator::GtEq, 100),
            cmp(&s, "status", Operator::Lt, 9),
        );
        let out = strip_sort_range_conjuncts(&expr, "ts").expect("status kept");
        let b = out.as_ref().downcast_ref::<BinaryExpr>().unwrap();
        assert!(b.left().downcast_ref::<Column>().unwrap().name() == "status");
    }

    // ── plan_row_groups (footer-driven, in-memory parquet) ──────────

    /// Two-row-group parquet over a single Int64 `ts` column: RG0 = [10..40]
    /// (WITHIN [0,100]), RG1 = [200..230] (NOT within). Returned as an owned
    /// `Arc<ParquetMetaData>` for classification.
    fn two_rg_metadata() -> std::sync::Arc<ParquetMetaData> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let rg0 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10i64, 20, 30, 40]))],
        )
        .unwrap();
        let rg1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![200i64, 210, 220, 230]))],
        )
        .unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        // max_row_group_size == 4 forces one RG per 4-row batch.
        let props = WriterProperties::builder()
            .set_max_row_group_size(4)
            .build();
        let mut w = ArrowWriter::try_new(tmp.reopen().unwrap(), schema, Some(props)).unwrap();
        w.write(&rg0).unwrap();
        w.write(&rg1).unwrap();
        w.close().unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(tmp.reopen().unwrap()).unwrap();
        assert_eq!(
            builder.metadata().num_row_groups(),
            2,
            "expected 2 row groups"
        );
        builder.metadata().clone()
    }

    #[test]
    fn plan_row_groups_classifies_within_vs_outside() {
        let meta = two_rg_metadata();
        let r = range(0, 100);
        let plans = plan_row_groups(
            &meta,
            Some(0),
            &hints(FastPathShape::CountOnly, false),
            Some(&r),
            false,
            None,
        );
        // RG0 WITHIN => CountFromIndex; RG1 outside => Full.
        assert_eq!(
            plans,
            vec![RowGroupPlan::CountFromIndex, RowGroupPlan::Full]
        );
    }

    #[test]
    fn plan_row_groups_deletes_force_all_full() {
        let meta = two_rg_metadata();
        let r = range(0, 100);
        let plans = plan_row_groups(
            &meta,
            Some(0),
            &hints(FastPathShape::CountOnly, false),
            Some(&r),
            true, // has_deletes
            None,
        );
        assert_eq!(plans, vec![RowGroupPlan::Full, RowGroupPlan::Full]);
    }

    #[test]
    fn plan_row_groups_no_range_is_cheap_all_full() {
        let meta = two_rg_metadata();
        let plans = plan_row_groups(
            &meta,
            Some(0),
            &hints(FastPathShape::CountOnly, false),
            None,
            false,
            None,
        );
        assert_eq!(plans, vec![RowGroupPlan::Full, RowGroupPlan::Full]);
    }

    /// Three RGs over a single Int64 `ts` column: RG0 [10..40], RG1 [110..140]
    /// (each single-bucket under floor-to-100), RG2 [190..210] (straddles the
    /// 100/200 boundary).
    fn three_rg_metadata() -> std::sync::Arc<ParquetMetaData> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let batches = [
            vec![10i64, 20, 30, 40],
            vec![110i64, 120, 130, 140],
            vec![190i64, 200, 205, 210],
        ];
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(4)
            .build();
        let mut w =
            ArrowWriter::try_new(tmp.reopen().unwrap(), schema.clone(), Some(props)).unwrap();
        for vals in batches {
            w.write(
                &RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap(),
            )
            .unwrap();
        }
        w.close().unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(tmp.reopen().unwrap()).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 3);
        builder.metadata().clone()
    }

    #[test]
    fn plan_row_groups_histogram_interior_interior_boundary() {
        let meta = three_rg_metadata();
        let r = range(0, 1000);
        let plans = plan_row_groups(
            &meta,
            Some(0),
            &hist_hints(5, 100),
            Some(&r),
            false,
            Some(&bucket(5, 100)),
        );
        assert_eq!(
            plans,
            vec![
                RowGroupPlan::HistogramBucket(0),
                RowGroupPlan::HistogramBucket(100),
                RowGroupPlan::Full,
            ]
        );
    }

    #[test]
    fn plan_row_groups_histogram_deletes_all_full() {
        let meta = three_rg_metadata();
        let r = range(0, 1000);
        let plans = plan_row_groups(
            &meta,
            Some(0),
            &hist_hints(5, 100),
            Some(&r),
            true,
            Some(&bucket(5, 100)),
        );
        assert_eq!(plans, vec![RowGroupPlan::Full; 3]);
    }
}
