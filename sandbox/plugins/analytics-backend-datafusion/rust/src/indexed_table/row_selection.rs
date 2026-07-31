//! Row-selection utilities for the indexed streaming path.
//!
//! Turns a candidate set of RG-relative doc positions into the parquet
//! [`RowSelection`] the decoder reads, plus a small packed-bits helper the
//! evaluators share.
//!
//! Selection granularity is a trade: a row-granular selection reads the fewest
//! bytes but produces one selector per candidate run, and the decoder's
//! per-selector cost is paid on every column. Once candidates are dense the
//! selector list dominates — reading a few extra rows inside a longer `select`
//! run is cheaper than a skip/select pair per gap.
//!
//! [`min_skip_run`] is that knob: skip runs shorter than it are absorbed into
//! the surrounding `select`, so the decoder sees fewer, longer runs. The rows it
//! over-reads are non-candidates, and the refinement stage drops them by row
//! position (see `eval::RowPositions`), so the answer is unchanged either way.
//!
//! `min_skip_run = 1` disables coalescing entirely and gives the row-granular
//! selection. The caller picks per row group from candidate selectivity — see
//! `access_provider::selection_from_rows`.
//!
//! [`min_skip_run`]: build_row_selection_with_min_skip_run

use datafusion::arrow::array::BooleanArray;
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use roaring::RoaringBitmap;

/// Build a row-granular `RowSelection` from a candidate `RoaringBitmap`.
///
/// Every maximal run of set bits becomes a `select`; every gap becomes a `skip`.
/// The selection covers `rg_num_rows` in total, as parquet requires.
///
/// Out-of-range set bits are ignored defensively rather than trusted, since the
/// candidate set originates outside this module (Lucene, page pruning).
pub fn build_row_selection(candidates: &RoaringBitmap, rg_num_rows: usize) -> RowSelection {
    if rg_num_rows == 0 {
        return RowSelection::from(Vec::<RowSelector>::new());
    }
    if candidates.is_empty() {
        return RowSelection::from(vec![RowSelector::skip(rg_num_rows)]);
    }

    let mut out: Vec<RowSelector> = Vec::new();
    let mut pos = 0u32;
    // RoaringBitmap.iter() yields set bits in ascending order.
    let mut iter = candidates.iter().peekable();
    while let Some(&start) = iter.peek() {
        if (start as usize) >= rg_num_rows {
            break;
        }
        if start > pos {
            out.push(RowSelector::skip((start - pos) as usize));
        }
        let mut run_end = start;
        iter.next();
        while let Some(&next) = iter.peek() {
            if next == run_end + 1 && (next as usize) < rg_num_rows {
                run_end = next;
                iter.next();
            } else {
                break;
            }
        }
        out.push(RowSelector::select((run_end - start + 1) as usize));
        pos = run_end + 1;
    }
    if (pos as usize) < rg_num_rows {
        out.push(RowSelector::skip(rg_num_rows - pos as usize));
    }
    RowSelection::from(out)
}

/// Like [`build_row_selection`], but absorbs skip runs shorter than
/// `min_skip_run` into the surrounding `select`.
///
/// `min_skip_run <= 1` is exactly [`build_row_selection`]. Larger values trade
/// reading some non-candidate rows for a shorter selector list; the refinement
/// stage drops the over-read rows, so the result set is identical.
pub fn build_row_selection_with_min_skip_run(
    candidates: &RoaringBitmap,
    rg_num_rows: usize,
    min_skip_run: usize,
) -> RowSelection {
    let raw = build_row_selection(candidates, rg_num_rows);
    if min_skip_run <= 1 {
        return raw;
    }
    coalesce_short_skips(raw.into(), min_skip_run)
}

/// Absorb sub-`min_skip_run` skips into the neighbouring `select`, merging
/// adjacent selects as they form.
///
/// Total row count is preserved — a dropped skip becomes selected rows, never
/// vanishing rows — which is what keeps the selection covering the whole row
/// group as parquet requires.
fn coalesce_short_skips(input: Vec<RowSelector>, min_skip_run: usize) -> RowSelection {
    if min_skip_run <= 1 || input.is_empty() {
        return RowSelection::from(input);
    }
    let mut out: Vec<RowSelector> = Vec::with_capacity(input.len());
    for selector in input {
        let absorb = selector.skip && selector.row_count < min_skip_run;
        if selector.skip && !absorb {
            out.push(selector);
            continue;
        }
        // A select, or a short skip being turned into one: extend the previous
        // select if there is one, else start a new one.
        match out.last_mut() {
            Some(last) if !last.skip => last.row_count += selector.row_count,
            _ => out.push(RowSelector::select(selector.row_count)),
        }
    }
    RowSelection::from(out)
}

/// Apply [`coalesce_short_skips`] to an already-built selection.
///
/// Used by the page-granular path, whose selection comes from the pruner rather
/// than from a candidate bitmap.
pub fn coalesce_row_selection_with_min_skip_run(
    selection: RowSelection,
    min_skip_run: usize,
) -> RowSelection {
    coalesce_short_skips(selection.into(), min_skip_run)
}

/// Wrap packed LSB0 `u64` words as a `BooleanArray` of `len` bits.
///
/// Shared by the collector and boolean-tree evaluators, both of which assemble
/// refinement masks word-at-a-time to avoid a `Vec<bool>` round trip.
pub fn packed_bits_to_boolean_array(bits: Vec<u64>, len: usize) -> BooleanArray {
    use datafusion::arrow::buffer::Buffer;
    let buffer = Buffer::from_vec(bits);
    let boolean = datafusion::arrow::buffer::BooleanBuffer::new(buffer, 0, len);
    BooleanArray::new(boolean, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::parquet::arrow::arrow_reader::RowSelector;

    fn selectors(sel: RowSelection) -> Vec<(bool, usize)> {
        Vec::<RowSelector>::from(sel)
            .into_iter()
            .map(|s| (s.skip, s.row_count))
            .collect()
    }

    #[test]
    fn empty_row_group_yields_no_selectors() {
        let bm = RoaringBitmap::new();
        assert!(selectors(build_row_selection(&bm, 0)).is_empty());
    }

    #[test]
    fn no_candidates_skips_whole_row_group() {
        let bm = RoaringBitmap::new();
        assert_eq!(selectors(build_row_selection(&bm, 10)), vec![(true, 10)]);
    }

    #[test]
    fn all_candidates_selects_whole_row_group() {
        let mut bm = RoaringBitmap::new();
        bm.insert_range(0..8);
        assert_eq!(selectors(build_row_selection(&bm, 8)), vec![(false, 8)]);
    }

    /// Every gap becomes a real skip — this is the row-granular guarantee that
    /// replaced the `min_skip_run` coalescing heuristic.
    #[test]
    fn each_gap_becomes_a_skip() {
        let mut bm = RoaringBitmap::new();
        for b in [1u32, 2, 5, 9] {
            bm.insert(b);
        }
        assert_eq!(
            selectors(build_row_selection(&bm, 12)),
            vec![
                (true, 1),
                (false, 2),
                (true, 2),
                (false, 1),
                (true, 3),
                (false, 1),
                (true, 2),
            ]
        );
    }

    #[test]
    fn trailing_candidate_needs_no_final_skip() {
        let mut bm = RoaringBitmap::new();
        bm.insert(3);
        assert_eq!(
            selectors(build_row_selection(&bm, 4)),
            vec![(true, 3), (false, 1)]
        );
    }

    /// Candidates past the row group are ignored rather than trusted: the set
    /// originates outside this module (Lucene, page pruning).
    #[test]
    fn out_of_range_candidates_are_ignored() {
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(50);
        assert_eq!(
            selectors(build_row_selection(&bm, 4)),
            vec![(true, 1), (false, 1), (true, 2)]
        );
    }

    #[test]
    fn selection_always_covers_every_row() {
        let mut bm = RoaringBitmap::new();
        for b in [0u32, 3, 4, 7] {
            bm.insert(b);
        }
        let total: usize = selectors(build_row_selection(&bm, 9))
            .iter()
            .map(|(_, n)| n)
            .sum();
        assert_eq!(total, 9, "parquet requires full coverage of the row group");
    }

    #[test]
    fn packed_bits_round_trip() {
        let mask = packed_bits_to_boolean_array(vec![0b1011], 4);
        assert_eq!(
            (0..4).map(|i| mask.value(i)).collect::<Vec<_>>(),
            vec![true, true, false, true]
        );
    }
}
