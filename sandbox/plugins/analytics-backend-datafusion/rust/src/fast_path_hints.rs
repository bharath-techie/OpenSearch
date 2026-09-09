/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Planner fast-path hints — the fixed-layout wire contract the Java planner
//! ships once per shard fragment.
//!
//! The Java planner decides the plan shape exactly once (see
//! `FastPathHintExtractor`) and serializes its decision into a fixed
//! `#[repr(C)]`, versioned, little-endian struct. Rust reads it back with
//! [`FastPathHints::from_ffm_ptr`] and makes exactly one per-row-group decision
//! from footer stats + these hints + `has_deletes` — no plan-shape sniffing
//! remains in Rust.
//!
//! Layout mirrors `org.opensearch.be.datafusion.nativelib.FastPathHints`
//! (`writeTo` / `BYTE_SIZE`); a byte-layout test pins [`FASTPATHHINTS_BYTE_SIZE`]
//! on both sides.

use datafusion::arrow::datatypes::TimeUnit;

/// Total byte size of the wire struct. Pinned by a test on BOTH sides; must
/// match `FastPathHints.BYTE_SIZE` in Java.
pub const FASTPATHHINTS_BYTE_SIZE: usize = 40;

/// The plan shape the fragment was classified into. Reserved variants are
/// populated by PR3; PR2 only ever produces `None` / `CountOnly`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastPathShape {
    /// No fast path applies — decode as usual.
    None,
    /// `count(*)`/`count(literal)` with no group keys over a range+delegated filter.
    CountOnly,
    /// Sort+Limit top-K (reserved, PR3).
    TopK,
    /// `span()` histogram (reserved, PR3).
    Histogram,
}

impl FastPathShape {
    /// Fail-closed: any unknown discriminant decodes to `None`.
    fn from_u8(v: u8) -> Self {
        match v {
            1 => FastPathShape::CountOnly,
            2 => FastPathShape::TopK,
            3 => FastPathShape::Histogram,
            _ => FastPathShape::None,
        }
    }
}

/// DECLARED mapping unit of the sort/range column (from the index mapping:
/// `date` => millis, `date_nanos` => nanos). `None` means no range unit was
/// supplied — unit coercion declines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeUnit {
    None,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl RangeUnit {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => RangeUnit::Seconds,
            2 => RangeUnit::Millis,
            3 => RangeUnit::Micros,
            4 => RangeUnit::Nanos,
            _ => RangeUnit::None,
        }
    }

    /// Ticks per second for this unit. `None` has no rate.
    fn ticks_per_second(self) -> Option<i64> {
        match self {
            RangeUnit::None => None,
            RangeUnit::Seconds => Some(1),
            RangeUnit::Millis => Some(1_000),
            RangeUnit::Micros => Some(1_000_000),
            RangeUnit::Nanos => Some(1_000_000_000),
        }
    }
}

/// Ticks per second for an Arrow physical `TimeUnit`.
fn physical_ticks_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

// Flag bits (see the Java `writeTo` doc).
const FLAG_RANGE_ON_LEADING_SORT_FIELD: u8 = 1 << 0;
const FLAG_RANGE_CONJUNCT_STRIPPABLE: u8 = 1 << 1;
// bit2 topk_keep_last, bit3 topk_path_safe — populated by PR3 (top-K).
const FLAG_TOPK_KEEP_LAST: u8 = 1 << 2;
const FLAG_TOPK_PATH_SAFE: u8 = 1 << 3;

/// Decoded planner hints. All fields are plain values; the only logic here is
/// [`sort_range_in_physical_unit`], the single place unit coercion lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FastPathHints {
    pub version: u8,
    pub shape: FastPathShape,
    flags: u8,
    pub range_unit: RangeUnit,
    /// Reserved (PR3): top-K budget. Zero in PR2.
    pub topk_budget: u32,
    /// Inclusive lower bound in the DECLARED unit. `i64::MIN` = absent.
    pub range_lower_inclusive: i64,
    /// Inclusive upper bound in the DECLARED unit. `i64::MAX` = absent.
    pub range_upper_inclusive: i64,
    /// Reserved (PR3): histogram bucket op. Zero in PR2.
    pub histogram_bucket_op: u8,
    /// Reserved (PR3): histogram bucket operand. Zero in PR2.
    pub histogram_bucket_operand: i64,
}

impl Default for FastPathHints {
    /// All-zero / absent — what every non-fast-path fragment ships and what an
    /// absent (null) pointer decodes to.
    fn default() -> Self {
        FastPathHints {
            version: 1,
            shape: FastPathShape::None,
            flags: 0,
            range_unit: RangeUnit::None,
            topk_budget: 0,
            range_lower_inclusive: i64::MIN,
            range_upper_inclusive: i64::MAX,
            histogram_bucket_op: 0,
            histogram_bucket_operand: 0,
        }
    }
}

impl FastPathHints {
    /// True when the single range conjunct sits on the leading index-sort field.
    pub fn range_on_leading_sort_field(&self) -> bool {
        self.flags & FLAG_RANGE_ON_LEADING_SORT_FIELD != 0
    }

    /// True when the residual after removing the sort-range conjunct is a pure
    /// AND of the remaining conjuncts (Fix 10 strip rule).
    pub fn range_conjunct_strippable(&self) -> bool {
        self.flags & FLAG_RANGE_CONJUNCT_STRIPPABLE != 0
    }

    /// True when the top-K query sorts in the opposite direction to the index
    /// sort order, so the surviving `topk_budget` rows are the LAST ones of each
    /// WITHIN row group (rows are stored in index-sort order). `false` keeps the
    /// FIRST `topk_budget`. Only meaningful when `shape == TopK`.
    pub fn topk_keep_last(&self) -> bool {
        self.flags & FLAG_TOPK_KEEP_LAST != 0
    }

    /// True when the Sort→Scan path is truncation-safe (identity
    /// projection/filter/limit only — no window/agg/join/distinct/unnest that
    /// could reorder or fan out rows). The planner fails this closed; Rust only
    /// truncates when it is set. Only meaningful when `shape == TopK`.
    pub fn topk_path_safe(&self) -> bool {
        self.flags & FLAG_TOPK_PATH_SAFE != 0
    }

    /// Decode from a raw FFM pointer. A null (0) pointer => [`FastPathHints::default`]
    /// (the all-zero NONE fragment).
    ///
    /// # Safety
    /// When non-zero, `ptr` must point to at least [`FASTPATHHINTS_BYTE_SIZE`]
    /// bytes of live memory written by the Java `FastPathHints#writeTo`.
    pub unsafe fn from_ffm_ptr(ptr: i64) -> Self {
        if ptr == 0 {
            return FastPathHints::default();
        }
        let base = ptr as *const u8;
        // Field-by-field reads at explicit offsets (little-endian). Multi-byte
        // reads are unaligned-safe via read_unaligned.
        let version = base.read();
        let shape = FastPathShape::from_u8(base.add(1).read());
        let flags = base.add(2).read();
        let range_unit = RangeUnit::from_u8(base.add(3).read());
        let topk_budget = (base.add(4) as *const u32).read_unaligned();
        let range_lower_inclusive = (base.add(8) as *const i64).read_unaligned();
        let range_upper_inclusive = (base.add(16) as *const i64).read_unaligned();
        let histogram_bucket_op = base.add(24).read();
        let histogram_bucket_operand = (base.add(32) as *const i64).read_unaligned();
        FastPathHints {
            version,
            shape,
            flags,
            range_unit,
            topk_budget,
            range_lower_inclusive,
            range_upper_inclusive,
            histogram_bucket_op,
            histogram_bucket_operand,
        }
    }
}

/// An inclusive `[lower, upper]` sort-column range in the PHYSICAL Parquet unit,
/// ready to compare against footer stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortRange {
    pub lower_inclusive: i64,
    pub upper_inclusive: i64,
}

/// floor(a / b) for b > 0, correct for negative `a` (pre-1970 timestamps).
fn floor_div(a: i64, b: i64) -> i64 {
    a.div_euclid(b)
}

/// ceil(a / b) for b > 0.
fn ceil_div(a: i64, b: i64) -> i64 {
    let q = a.div_euclid(b);
    if a.rem_euclid(b) != 0 {
        q + 1
    } else {
        q
    }
}

/// Build the sort range in the PHYSICAL unit — the ONLY unit logic in Rust
/// (Fix 13's const-folding was moved to the Java planner).
///
/// * physical finer than declared => widen exact: lower `* factor`, upper
///   `* factor + (factor-1)` so the whole declared tick is covered.
/// * physical coarser than declared => coarsen: lower rounds up, upper rounds
///   down (conservative WITHIN — never admits rows outside the predicate).
/// * any overflow => `None` (decline the fast path).
///
/// Absent bounds (`i64::MIN` / `i64::MAX`) pass through unchanged. Returns
/// `None` when the declared unit is absent.
pub fn sort_range_in_physical_unit(
    hints: &FastPathHints,
    physical_unit: TimeUnit,
) -> Option<SortRange> {
    let declared_tps = hints.range_unit.ticks_per_second()?;
    let physical_tps = physical_ticks_per_second(physical_unit);

    let convert_lower = |v: i64| -> Option<i64> {
        if v == i64::MIN {
            return Some(i64::MIN); // absent lower bound
        }
        if physical_tps >= declared_tps {
            let factor = physical_tps / declared_tps;
            v.checked_mul(factor)
        } else {
            let factor = declared_tps / physical_tps;
            Some(ceil_div(v, factor))
        }
    };
    let convert_upper = |v: i64| -> Option<i64> {
        if v == i64::MAX {
            return Some(i64::MAX); // absent upper bound
        }
        if physical_tps >= declared_tps {
            let factor = physical_tps / declared_tps;
            v.checked_mul(factor)?.checked_add(factor - 1)
        } else {
            let factor = declared_tps / physical_tps;
            Some(floor_div(v, factor))
        }
    };

    Some(SortRange {
        lower_inclusive: convert_lower(hints.range_lower_inclusive)?,
        upper_inclusive: convert_upper(hints.range_upper_inclusive)?,
    })
}

/// Coerce a tick count expressed in the DECLARED range unit into the PHYSICAL
/// unit, using the SAME ticks ratio as the range bounds. This is how the
/// histogram bucket operand (shipped in the declared unit) is translated so
/// that `BucketFn::eval` on physical footer min/max reproduces DataFusion's own
/// bucket arithmetic (which runs on the physical-unit column). Fail-closed:
/// `None` when the declared unit is absent, when a coarser physical unit does
/// not divide the operand evenly (would change bucket semantics), or on
/// multiply overflow. A plain-Int64 column uses identity coercion (declared ==
/// physical), so the operand passes through unchanged.
pub fn coerce_ticks_to_physical(
    hints: &FastPathHints,
    physical_unit: TimeUnit,
    ticks: i64,
) -> Option<i64> {
    let declared_tps = hints.range_unit.ticks_per_second()?;
    let physical_tps = physical_ticks_per_second(physical_unit);
    if physical_tps >= declared_tps {
        let factor = physical_tps / declared_tps;
        ticks.checked_mul(factor)
    } else {
        let factor = declared_tps / physical_tps;
        if ticks % factor != 0 {
            return None;
        }
        Some(ticks / factor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{alloc, dealloc, Layout};

    /// Write a wire struct into a heap buffer and decode it — the round-trip that
    /// pins BYTE_SIZE and the offset map against the Java `writeTo`.
    fn write_and_decode(bytes: &[u8]) -> FastPathHints {
        assert_eq!(bytes.len(), FASTPATHHINTS_BYTE_SIZE);
        unsafe {
            let layout = Layout::from_size_align(FASTPATHHINTS_BYTE_SIZE, 8).unwrap();
            let buf = alloc(layout);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, FASTPATHHINTS_BYTE_SIZE);
            let hints = FastPathHints::from_ffm_ptr(buf as i64);
            dealloc(buf, layout);
            hints
        }
    }

    fn count_only_wire(
        unit: u8,
        flags: u8,
        lower: i64,
        upper: i64,
    ) -> [u8; FASTPATHHINTS_BYTE_SIZE] {
        let mut b = [0u8; FASTPATHHINTS_BYTE_SIZE];
        b[0] = 1; // version
        b[1] = 1; // shape = COUNT_ONLY
        b[2] = flags;
        b[3] = unit;
        // offset 4: topk_budget (0)
        b[8..16].copy_from_slice(&lower.to_le_bytes());
        b[16..24].copy_from_slice(&upper.to_le_bytes());
        // offset 24: histogram_bucket_op (0); 32: operand (0)
        b
    }

    #[test]
    fn byte_size_is_forty() {
        assert_eq!(FASTPATHHINTS_BYTE_SIZE, 40);
    }

    #[test]
    fn null_ptr_decodes_to_default_none() {
        let h = unsafe { FastPathHints::from_ffm_ptr(0) };
        assert_eq!(h, FastPathHints::default());
        assert_eq!(h.shape, FastPathShape::None);
    }

    #[test]
    fn roundtrip_reads_fields_at_offsets() {
        let wire = count_only_wire(2 /* millis */, 0b11, 100, 200);
        let h = write_and_decode(&wire);
        assert_eq!(h.version, 1);
        assert_eq!(h.shape, FastPathShape::CountOnly);
        assert_eq!(h.range_unit, RangeUnit::Millis);
        assert!(h.range_on_leading_sort_field());
        assert!(h.range_conjunct_strippable());
        assert_eq!(h.range_lower_inclusive, 100);
        assert_eq!(h.range_upper_inclusive, 200);
    }

    #[test]
    fn unknown_discriminants_fail_closed() {
        let mut b = [0u8; FASTPATHHINTS_BYTE_SIZE];
        b[0] = 1;
        b[1] = 99; // unknown shape
        b[3] = 42; // unknown unit
        let h = write_and_decode(&b);
        assert_eq!(h.shape, FastPathShape::None);
        assert_eq!(h.range_unit, RangeUnit::None);
    }

    #[test]
    fn topk_flags_and_budget_decode() {
        // shape = TOPK (2); flags bit2 keep_last + bit3 path_safe; budget = 50.
        let mut b = [0u8; FASTPATHHINTS_BYTE_SIZE];
        b[0] = 1;
        b[1] = 2; // TOPK
        b[2] = 0b0000_1100; // topk_keep_last | topk_path_safe
        b[4..8].copy_from_slice(&50u32.to_le_bytes());
        let h = write_and_decode(&b);
        assert_eq!(h.shape, FastPathShape::TopK);
        assert!(h.topk_keep_last());
        assert!(h.topk_path_safe());
        assert_eq!(h.topk_budget, 50);
        // The lower flag bits are independent of the top-K bits.
        assert!(!h.range_on_leading_sort_field());
        assert!(!h.range_conjunct_strippable());
    }

    #[test]
    fn topk_flags_default_off() {
        // path_safe only (bit3), keep_last off (bit2 clear) — the keep-first case.
        let mut b = [0u8; FASTPATHHINTS_BYTE_SIZE];
        b[0] = 1;
        b[1] = 2; // TOPK
        b[2] = 0b0000_1000; // topk_path_safe only
        b[4..8].copy_from_slice(&8u32.to_le_bytes());
        let h = write_and_decode(&b);
        assert!(!h.topk_keep_last());
        assert!(h.topk_path_safe());
        assert_eq!(h.topk_budget, 8);
    }

    #[test]
    fn coerce_ms_to_ns_is_exact_widening() {
        // declared millis, physical nanos: lower *1e6, upper *1e6 + (1e6 - 1).
        let h = write_and_decode(&count_only_wire(2, 0b11, 100, 200));
        let r = sort_range_in_physical_unit(&h, TimeUnit::Nanosecond).unwrap();
        assert_eq!(r.lower_inclusive, 100_000_000);
        assert_eq!(r.upper_inclusive, 200_999_999);
    }

    #[test]
    fn coerce_equal_unit_widens_by_zero() {
        let h = write_and_decode(&count_only_wire(3 /* micros */, 0b11, 5, 9));
        let r = sort_range_in_physical_unit(&h, TimeUnit::Microsecond).unwrap();
        assert_eq!(r.lower_inclusive, 5);
        assert_eq!(r.upper_inclusive, 9);
    }

    #[test]
    fn coerce_ns_to_ms_rounds_lower_up_upper_down() {
        // declared nanos, physical millis, factor 1e6.
        // lower 1_500_000 => ceil = 2; upper 9_500_000 => floor = 9.
        let h = write_and_decode(&count_only_wire(
            4, /* nanos */
            0b11, 1_500_000, 9_500_000,
        ));
        let r = sort_range_in_physical_unit(&h, TimeUnit::Millisecond).unwrap();
        assert_eq!(r.lower_inclusive, 2);
        assert_eq!(r.upper_inclusive, 9);
    }

    #[test]
    fn coerce_absent_bounds_pass_through() {
        let h = write_and_decode(&count_only_wire(2, 0b1, i64::MIN, i64::MAX));
        let r = sort_range_in_physical_unit(&h, TimeUnit::Nanosecond).unwrap();
        assert_eq!(r.lower_inclusive, i64::MIN);
        assert_eq!(r.upper_inclusive, i64::MAX);
    }

    #[test]
    fn coerce_overflow_declines() {
        // ms -> ns widening of a huge bound overflows i64 => None.
        let h = write_and_decode(&count_only_wire(2, 0b1, i64::MAX / 2, i64::MAX - 1));
        assert!(sort_range_in_physical_unit(&h, TimeUnit::Nanosecond).is_none());
    }

    #[test]
    fn coerce_declines_when_unit_absent() {
        let h = write_and_decode(&count_only_wire(0 /* none */, 0b1, 1, 2));
        assert!(sort_range_in_physical_unit(&h, TimeUnit::Millisecond).is_none());
    }
}
