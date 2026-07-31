//! Per-query tuning knobs shared by the vanilla and indexed query paths.
//!
//! Populated from Java (cluster / index / request settings) and passed to
//! Rust once at query start via a `#[repr(C)]` wire struct. Read out at
//! setup time and copied into hot-path fields — never dereferenced on a
//! per-batch or per-row hot path.

/// Engine-internal point lookup driven through the normal `df_execute_query`
/// entry point. When active, the Substrait `plan_ptr` is ignored and the plan
/// is built natively via the DataFrame API with a single pushed-down filter on
/// a stored reserved column — no Substrait, no planner round-trip. Used by the
/// pluggable-dataformat get-by-id path (`GetService`), not by user search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternalSearch {
    /// Not an internal lookup — decode `plan_ptr` as Substrait as usual.
    Off,
    /// Get-by-row-id: `__row_id__ = bound`, single row. `bound` is the physical
    /// row position resolved from the secondary (Lucene) index.
    ByRowId(i64),
    /// Seq-no scan: `_seq_no > bound`, projecting only id/seq/term/version.
    /// Used by version-map restore on crash recovery.
    SeqNoAbove(i64),
}

impl InternalSearch {
    /// Decodes the FFM wire pair `(mode, bound)`. `mode`: 0 = Off, 1 = ByRowId,
    /// 2 = SeqNoAbove. Any other value is treated as Off (forward-compatible).
    pub fn from_wire(mode: i64, bound: i64) -> Self {
        match mode {
            1 => InternalSearch::ByRowId(bound),
            2 => InternalSearch::SeqNoAbove(bound),
            _ => InternalSearch::Off,
        }
    }

    /// Whether this is an engine-internal point lookup (i.e. not [`InternalSearch::Off`],
    /// the normal user-search path).
    pub fn is_internal_search(self) -> bool {
        !matches!(self, InternalSearch::Off)
    }
}

/// Query-scoped configuration. Owned by value after FFM decode.
#[derive(Debug, Clone)]
pub struct DatafusionQueryConfig {
    // Common
    pub batch_size: usize,
    // Single query concurrency
    pub target_partitions: usize,
    /// DataFusion's own decode-time predicate pushdown on the ListingTable path.
    pub listing_table_pushdown_filters: bool,

    // Indexed-only
    /// Whether IndexedStream asks parquet to apply the residual predicate
    /// during decode (via `RowFilter` pushdown).
    pub indexed_pushdown_filters: bool,
    pub cost_predicate: u32,
    pub cost_collector: u32,
    /// Skip runs shorter than this are absorbed into the surrounding `select`
    /// when candidates are dense enough to make the selector list the dominant
    /// cost. `1` disables coalescing. Applied only above
    /// [`Self::min_skip_run_selectivity_threshold`].
    pub min_skip_run_default: usize,
    /// Candidate selectivity (matched / row-group rows) below which selection
    /// stays row-granular. Below it the skips are long and few, so coalescing
    /// would over-read for nothing.
    pub min_skip_run_selectivity_threshold: f64,
    /// Pins the per-row-group granularity decision instead of letting
    /// selectivity choose. Diagnostics only; `None` in production.
    pub force_strategy: Option<FilterStrategy>,
    /// Whether refinement runs as a parquet `ArrowPredicate` during decode
    /// rather than on the fully decoded batch.
    ///
    /// Decode-time refinement decodes the refinement's own columns first, then
    /// decodes the projection for survivors only — two decode passes, which pays
    /// off exactly when refinement rejects most candidates. When the candidate
    /// stage is already near-exact (an indexed term match, typically) almost
    /// nothing is rejected and the second pass is pure overhead, so this is
    /// gated on measured refinement selectivity at runtime.
    pub indexed_decode_time_refinement: bool,
}

/// How to materialize a row group's candidate set for the decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStrategy {
    /// Row-granular: every non-candidate row becomes a `skip`
    /// (`min_skip_run = 1`).
    RowSelection,
    /// One whole-row-group `select`; the refinement drops non-candidates
    /// (`min_skip_run > rows`).
    BooleanMask,
}

impl FilterStrategy {
    /// Decodes the FFM wire value: `-1` = `None`, `0` = `RowSelection`,
    /// `1` = `BooleanMask`. Unknown values are treated as `None` so a newer
    /// Java side cannot force an unimplemented strategy.
    pub fn from_wire(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::RowSelection),
            1 => Some(Self::BooleanMask),
            _ => None,
        }
    }
}

/// FFM wire format. Must stay in lockstep with the Java `MemoryLayout`.
///
/// All fields have fixed sizes and natural alignment so Java and Rust
/// produce the same byte layout on all target platforms. Enum-ish
/// `Option<_>` fields are encoded with a `-1` sentinel for `None`.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct WireDatafusionQueryConfig {
    /// Layout guard. Java writes [`WIRE_CONFIG_ABI_VERSION`]; Rust rejects
    /// anything else.
    ///
    /// This struct is passed as raw bytes over FFM with no negotiation, so a
    /// Rust `.so` and a Java plugin built from different revisions would
    /// silently misread every field — a wrong `batch_size` or an inverted
    /// pushdown flag, not a crash. Bump the constant on both sides whenever a
    /// field is added, removed, reordered, or resized.
    pub abi_version: i32,
    /// Explicit padding so `batch_size` stays 8-byte aligned and the layout is
    /// identical on every target, rather than relying on implicit `repr(C)`
    /// padding that Java's `MemoryLayout` would have to mirror by guesswork.
    pub _pad: i32,
    pub batch_size: i64,
    pub target_partitions: i64,
    /// 0 = false, 1 = true
    pub listing_table_pushdown_filters: i32,
    /// 0 = false, 1 = true
    pub indexed_pushdown_filters: i32,
    pub cost_predicate: i32,
    pub cost_collector: i32,
    pub min_skip_run_default: i64,
    pub min_skip_run_selectivity_threshold: f64,
    /// `-1` = None (selectivity decides), `0` = RowSelection, `1` = BooleanMask.
    pub force_strategy: i32,
    /// 0 = false, 1 = true
    pub indexed_decode_time_refinement: i32,
}

/// Current FFM layout version for [`WireDatafusionQueryConfig`].
///
/// Bump on any field add, remove, reorder, or resize, and bump
/// `WireConfigSnapshot.ABI_VERSION` on the Java side in the same change.
/// Layouts before this field existed were unversioned, so there is no version
/// 0 to be compatible with — a mismatched pair simply fails the assert in
/// [`DatafusionQueryConfig::from_ffm_ptr`].
pub const WIRE_CONFIG_ABI_VERSION: i32 = 3;

impl DatafusionQueryConfig {
    /// Fallback values used when Java passes a null config pointer (0).
    /// Production code should always supply a real config via the wire
    /// struct; this exists only for the transitional period while Java
    /// wiring is incomplete.
    fn fallback() -> Self {
        Self {
            batch_size: 8192,
            target_partitions: 4,
            listing_table_pushdown_filters: false,
            indexed_pushdown_filters: true,
            cost_predicate: 1,
            cost_collector: 10,
            min_skip_run_default: 1024,
            min_skip_run_selectivity_threshold: 0.03,
            force_strategy: None,
            indexed_decode_time_refinement: false,
        }
    }

    /// Constructor with sensible defaults for tests and benchmarks.
    /// Production code should use `from_ffm_ptr` with a real wire config.
    pub fn test_default() -> Self {
        Self::fallback()
    }

    /// Returns a builder seeded with fallback defaults for test usage.
    #[cfg(test)]
    pub fn builder() -> DatafusionQueryConfigBuilder {
        DatafusionQueryConfigBuilder::new()
    }

    /// Decode from a raw FFM pointer.
    ///
    /// # Safety
    /// `ptr` must be a valid, non-zero pointer to a `WireDatafusionQueryConfig`
    /// whose memory is live for the duration of this call.
    ///
    /// # Panics
    /// Panics if `ptr` is 0 (null). Java must always supply a valid config pointer.
    pub unsafe fn from_ffm_ptr(ptr: i64) -> Self {
        assert!(
            ptr != 0,
            "from_ffm_ptr: null query config pointer — Java must always provide a valid config"
        );
        let wire = &*(ptr as *const WireDatafusionQueryConfig);
        assert_eq!(
            wire.abi_version, WIRE_CONFIG_ABI_VERSION,
            "query-config ABI mismatch: Java wrote version {} but this native \
             library expects {}. The Rust .so and the Java plugin are from \
             different builds; every field after the header would be misread.",
            wire.abi_version, WIRE_CONFIG_ABI_VERSION
        );
        Self::from_wire(wire)
    }

    fn from_wire(w: &WireDatafusionQueryConfig) -> Self {
        Self {
            batch_size: w.batch_size as usize,
            target_partitions: w.target_partitions as usize,
            listing_table_pushdown_filters: w.listing_table_pushdown_filters != 0,
            indexed_pushdown_filters: w.indexed_pushdown_filters != 0,
            cost_predicate: w.cost_predicate as u32,
            cost_collector: w.cost_collector as u32,
            min_skip_run_default: w.min_skip_run_default.max(1) as usize,
            min_skip_run_selectivity_threshold: w.min_skip_run_selectivity_threshold,
            force_strategy: FilterStrategy::from_wire(w.force_strategy),
            indexed_decode_time_refinement: w.indexed_decode_time_refinement != 0,
        }
    }
}

#[cfg(test)]
pub struct DatafusionQueryConfigBuilder(DatafusionQueryConfig);

#[cfg(test)]
impl DatafusionQueryConfigBuilder {
    fn new() -> Self {
        Self(DatafusionQueryConfig::fallback())
    }
    pub fn batch_size(mut self, v: usize) -> Self {
        self.0.batch_size = v;
        self
    }
    pub fn target_partitions(mut self, v: usize) -> Self {
        self.0.target_partitions = v;
        self
    }
    pub fn listing_table_pushdown_filters(mut self, v: bool) -> Self {
        self.0.listing_table_pushdown_filters = v;
        self
    }
    pub fn indexed_pushdown_filters(mut self, v: bool) -> Self {
        self.0.indexed_pushdown_filters = v;
        self
    }
    pub fn cost_predicate(mut self, v: u32) -> Self {
        self.0.cost_predicate = v;
        self
    }
    pub fn cost_collector(mut self, v: u32) -> Self {
        self.0.cost_collector = v;
        self
    }
    pub fn min_skip_run_default(mut self, v: usize) -> Self {
        self.0.min_skip_run_default = v;
        self
    }
    pub fn min_skip_run_selectivity_threshold(mut self, v: f64) -> Self {
        self.0.min_skip_run_selectivity_threshold = v;
        self
    }
    pub fn force_strategy(mut self, v: Option<FilterStrategy>) -> Self {
        self.0.force_strategy = v;
        self
    }
    pub fn indexed_decode_time_refinement(mut self, v: bool) -> Self {
        self.0.indexed_decode_time_refinement = v;
        self
    }
    pub fn build(self) -> DatafusionQueryConfig {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_matches_legacy_constants() {
        let c = DatafusionQueryConfig::test_default();
        assert_eq!(c.batch_size, 8192);
        assert_eq!(c.target_partitions, 4);
        assert!(!c.listing_table_pushdown_filters);
        assert!(c.indexed_pushdown_filters);
        assert_eq!(c.cost_predicate, 1);
        assert_eq!(c.cost_collector, 10);
    }

    #[test]
    #[should_panic(expected = "null query config pointer")]
    fn wire_decode_null_pointer_panics() {
        unsafe { DatafusionQueryConfig::from_ffm_ptr(0) };
    }

    #[test]
    fn wire_layout_matches_java_snapshot() {
        // Mirrors the offset table documented on Java's
        // `WireConfigSnapshot.writeTo`. Any change here must change both.
        assert_eq!(std::mem::size_of::<WireDatafusionQueryConfig>(), 64);
        for (name, got, want) in [
            (
                "abi_version",
                std::mem::offset_of!(WireDatafusionQueryConfig, abi_version),
                0,
            ),
            (
                "batch_size",
                std::mem::offset_of!(WireDatafusionQueryConfig, batch_size),
                8,
            ),
            (
                "target_partitions",
                std::mem::offset_of!(WireDatafusionQueryConfig, target_partitions),
                16,
            ),
            (
                "listing_table_pushdown_filters",
                std::mem::offset_of!(WireDatafusionQueryConfig, listing_table_pushdown_filters),
                24,
            ),
            (
                "indexed_pushdown_filters",
                std::mem::offset_of!(WireDatafusionQueryConfig, indexed_pushdown_filters),
                28,
            ),
            (
                "cost_predicate",
                std::mem::offset_of!(WireDatafusionQueryConfig, cost_predicate),
                32,
            ),
            (
                "cost_collector",
                std::mem::offset_of!(WireDatafusionQueryConfig, cost_collector),
                36,
            ),
            (
                "min_skip_run_default",
                std::mem::offset_of!(WireDatafusionQueryConfig, min_skip_run_default),
                40,
            ),
            (
                "min_skip_run_selectivity_threshold",
                std::mem::offset_of!(WireDatafusionQueryConfig, min_skip_run_selectivity_threshold),
                48,
            ),
            (
                "force_strategy",
                std::mem::offset_of!(WireDatafusionQueryConfig, force_strategy),
                56,
            ),
            (
                "indexed_decode_time_refinement",
                std::mem::offset_of!(WireDatafusionQueryConfig, indexed_decode_time_refinement),
                60,
            ),
        ] {
            assert_eq!(got, want, "offset of {name} drifted from the Java layout");
        }
    }

    #[test]
    fn internal_search_from_wire_decodes_modes() {
        assert_eq!(InternalSearch::from_wire(0, 99), InternalSearch::Off);
        assert_eq!(
            InternalSearch::from_wire(1, 42),
            InternalSearch::ByRowId(42)
        );
        assert_eq!(
            InternalSearch::from_wire(2, 7),
            InternalSearch::SeqNoAbove(7)
        );
        // Unknown modes are forward-compatible: treated as Off, bound ignored.
        assert_eq!(InternalSearch::from_wire(3, 5), InternalSearch::Off);
        assert!(!InternalSearch::Off.is_internal_search());
        assert!(InternalSearch::ByRowId(0).is_internal_search());
        assert!(InternalSearch::SeqNoAbove(0).is_internal_search());
    }

    #[test]
    fn wire_decode_round_trips_all_fields() {
        let wire = WireDatafusionQueryConfig {
            abi_version: WIRE_CONFIG_ABI_VERSION,
            _pad: 0,
            batch_size: 16384,
            target_partitions: 8,
            listing_table_pushdown_filters: 1,
            indexed_pushdown_filters: 0,
            cost_predicate: 3,
            cost_collector: 17,
            min_skip_run_default: 512,
            min_skip_run_selectivity_threshold: 0.07,
            force_strategy: 1,
            indexed_decode_time_refinement: 0,
        };
        let ptr = &wire as *const _ as i64;
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(ptr) };
        assert_eq!(c.batch_size, 16384);
        assert_eq!(c.target_partitions, 8);
        assert!(c.listing_table_pushdown_filters);
        assert!(!c.indexed_pushdown_filters);
        assert_eq!(c.cost_predicate, 3);
        assert_eq!(c.cost_collector, 17);
        assert_eq!(c.min_skip_run_default, 512);
        assert_eq!(c.min_skip_run_selectivity_threshold, 0.07);
        assert_eq!(c.force_strategy, Some(FilterStrategy::BooleanMask));
        assert!(!c.indexed_decode_time_refinement);
    }

    /// `min_skip_run_default` is clamped to at least 1: `0` would mean "coalesce
    /// nothing" in the wire encoding but underflow the `<= 1` disable check.
    #[test]
    fn min_skip_run_default_is_clamped_to_one() {
        let wire = WireDatafusionQueryConfig {
            abi_version: WIRE_CONFIG_ABI_VERSION,
            _pad: 0,
            batch_size: 8192,
            target_partitions: 4,
            listing_table_pushdown_filters: 0,
            indexed_pushdown_filters: 1,
            cost_predicate: 1,
            cost_collector: 10,
            min_skip_run_default: 0,
            min_skip_run_selectivity_threshold: 0.03,
            force_strategy: -1,
            indexed_decode_time_refinement: 1,
        };
        let ptr = &wire as *const _ as i64;
        let c = unsafe { DatafusionQueryConfig::from_ffm_ptr(ptr) };
        assert_eq!(c.min_skip_run_default, 1);
        assert_eq!(c.force_strategy, None);
    }

    #[test]
    fn filter_strategy_from_wire_decodes_sentinels() {
        assert_eq!(FilterStrategy::from_wire(-1), None);
        assert_eq!(
            FilterStrategy::from_wire(0),
            Some(FilterStrategy::RowSelection)
        );
        assert_eq!(
            FilterStrategy::from_wire(1),
            Some(FilterStrategy::BooleanMask)
        );
        // Unknown values are forward-compatible: treated as None.
        assert_eq!(FilterStrategy::from_wire(9), None);
    }
}
