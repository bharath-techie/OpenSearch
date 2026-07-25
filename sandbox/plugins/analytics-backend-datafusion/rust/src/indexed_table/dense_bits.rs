/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Dense packed-word bitset for collector candidates.
//!
//! Java collectors return matches as a packed LSB0 `u64` bitset. Converting
//! that to a `RoaringBitmap` and walking it bit-by-bit dominated non-selective
//! collector scans (~65M bit iterations per query). This module keeps the
//! packed words as the candidate representation so downstream consumers work
//! word-at-a-time:
//!
//! - `RowSelection` construction via `trailing_ones`/`trailing_zeros` runs.
//! - Page-range intersection via edge-masked word fills.
//! - Batch masks as zero-copy Arrow `BooleanBuffer` slices (Arrow's packed
//!   boolean layout is the same LSB-first format).
//!
//! Row-ID scans still need random access to original positions; they convert
//! to `RoaringBitmap` via [`DenseBitset::to_roaring`] and use the established
//! bitmap path.

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::buffer::{BooleanBuffer, Buffer};
use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use roaring::RoaringBitmap;

/// Mutable packed bitset under construction. All positions are RG-relative
/// (bit 0 = first row of the row group).
pub struct DenseBitsetBuilder {
    words: Vec<u64>,
    len: usize,
}

impl DenseBitsetBuilder {
    pub fn zeros(len: usize) -> Self {
        Self {
            words: vec![0u64; len.div_ceil(64)],
            len,
        }
    }

    pub fn len_bits(&self) -> usize {
        self.len
    }

    /// OR `num_bits` bits of `src` (LSB0 packed) into this bitset starting at
    /// `bit_offset`. Bits of `src` beyond `num_bits` are ignored; bits landing
    /// beyond `self.len` are dropped.
    pub fn or_lsb0_words(&mut self, bit_offset: usize, src: &[u64], num_bits: usize) {
        let num_bits = num_bits.min(self.len.saturating_sub(bit_offset));
        if num_bits == 0 {
            return;
        }
        let word_off = bit_offset / 64;
        let shift = bit_offset % 64;
        let src_words = num_bits.div_ceil(64);
        for i in 0..src_words.min(src.len()) {
            let mut s = src[i];
            // Mask bits beyond num_bits in the last source word.
            let bits_before = i * 64;
            let valid = (num_bits - bits_before).min(64);
            if valid < 64 {
                s &= (1u64 << valid) - 1;
            }
            if s == 0 {
                continue;
            }
            let lo = word_off + i;
            if lo < self.words.len() {
                self.words[lo] |= s << shift;
            }
            if shift > 0 && lo + 1 < self.words.len() {
                self.words[lo + 1] |= s >> (64 - shift);
            }
        }
        // Defensively clear any bits at/beyond len in the last word.
        self.clear_tail();
    }

    /// Set every bit in `[start, end)`.
    pub fn set_range(&mut self, start: usize, end: usize) {
        let end = end.min(self.len);
        if start >= end {
            return;
        }
        let (sw, sb) = (start / 64, start % 64);
        let (ew, eb) = (end / 64, end % 64);
        if sw == ew {
            self.words[sw] |= (((1u128 << (eb - sb)) - 1) as u64) << sb;
            return;
        }
        self.words[sw] |= !0u64 << sb;
        for w in &mut self.words[sw + 1..ew] {
            *w = !0u64;
        }
        if eb > 0 {
            self.words[ew] |= (1u64 << eb) - 1;
        }
    }

    /// Keep only bits inside the union of `ranges` (RG-relative `[lo, hi)`);
    /// zero everything else. `ranges` must be sorted and non-overlapping.
    pub fn retain_ranges(&mut self, ranges: &[(usize, usize)]) {
        let mut keep = DenseBitsetBuilder::zeros(self.len);
        for &(lo, hi) in ranges {
            keep.set_range(lo, hi);
        }
        for (w, k) in self.words.iter_mut().zip(keep.words.iter()) {
            *w &= k;
        }
    }

    /// AND with `num_bits` bits of `src` (LSB0 packed) placed at `bit_offset`.
    /// Bits of `self` outside the `[bit_offset, bit_offset+num_bits)` window
    /// are cleared (the window is the peer's authoritative range).
    pub fn and_lsb0_words(&mut self, bit_offset: usize, src: &[u64], num_bits: usize) {
        let mut aligned = DenseBitsetBuilder::zeros(self.len);
        aligned.or_lsb0_words(bit_offset, src, num_bits);
        for (w, a) in self.words.iter_mut().zip(aligned.words.iter()) {
            *w &= a;
        }
    }

    fn clear_tail(&mut self) {
        let tail_bits = self.len % 64;
        if tail_bits > 0 {
            if let Some(last) = self.words.last_mut() {
                *last &= (1u64 << tail_bits) - 1;
            }
        }
    }

    pub fn freeze(mut self) -> DenseBitset {
        self.clear_tail();
        let count = self.words.iter().map(|w| w.count_ones() as usize).sum();
        DenseBitset {
            buffer: Buffer::from_vec(self.words),
            len: self.len,
            count,
        }
    }
}

/// Immutable packed bitset. The words live in an Arrow `Buffer` so batch
/// masks can be zero-copy `BooleanBuffer` slices.
pub struct DenseBitset {
    buffer: Buffer,
    len: usize,
    count: usize,
}

impl std::fmt::Debug for DenseBitset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DenseBitset")
            .field("len", &self.len)
            .field("count", &self.count)
            .finish()
    }
}

impl DenseBitset {
    pub fn words(&self) -> &[u64] {
        self.buffer.typed_data::<u64>()
    }

    pub fn len_bits(&self) -> usize {
        self.len
    }

    pub fn count_ones(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn get(&self, idx: usize) -> bool {
        idx < self.len && (self.words()[idx / 64] >> (idx % 64)) & 1 == 1
    }

    /// Position of the `n`-th (0-based) set bit, or `None` if `n >= count`.
    pub fn select(&self, n: usize) -> Option<usize> {
        if n >= self.count {
            return None;
        }
        let mut remaining = n;
        for (wi, &w) in self.words().iter().enumerate() {
            let ones = w.count_ones() as usize;
            if remaining < ones {
                // n-th set bit is inside this word.
                let mut w = w;
                for _ in 0..remaining {
                    w &= w - 1; // clear lowest set bit
                }
                return Some(wi * 64 + w.trailing_zeros() as usize);
            }
            remaining -= ones;
        }
        None
    }

    pub fn to_roaring(&self) -> RoaringBitmap {
        let bytes: &[u8] = self.buffer.as_slice();
        let mut bm = RoaringBitmap::from_lsb0_bytes(0, bytes);
        if (self.len as u64) < u32::MAX as u64 {
            bm.remove_range(self.len as u32..);
        }
        bm
    }

    /// Build a row-granular `RowSelection` by word-level run detection, then
    /// coalesce skips shorter than `min_skip_run` into adjacent selects.
    pub fn to_row_selection(&self, min_skip_run: usize) -> RowSelection {
        if self.len == 0 {
            return RowSelection::from(Vec::<RowSelector>::new());
        }
        if self.count == 0 {
            return RowSelection::from(vec![RowSelector::skip(self.len)]);
        }

        let mut raw: Vec<RowSelector> = Vec::new();
        // Current run accumulator: (is_select, length).
        let mut run_select = false;
        let mut run_len = 0usize;
        let mut flush = |raw: &mut Vec<RowSelector>, is_select: bool, len: usize| {
            if len == 0 {
                return;
            }
            if let Some(last) = raw.last_mut() {
                if last.skip != is_select {
                    last.row_count += len;
                    return;
                }
            }
            raw.push(if is_select {
                RowSelector::select(len)
            } else {
                RowSelector::skip(len)
            });
        };

        let words = self.words();
        let mut pos = 0usize;
        for (wi, &word) in words.iter().enumerate() {
            let bits_in_word = (self.len - wi * 64).min(64);
            if word == 0 {
                if run_select {
                    flush(&mut raw, true, run_len);
                    run_select = false;
                    run_len = 0;
                }
                run_len += bits_in_word;
            } else if word == !0u64 && bits_in_word == 64 {
                if !run_select {
                    flush(&mut raw, false, run_len);
                    run_select = true;
                    run_len = 0;
                }
                run_len += 64;
            } else {
                // Mixed word: walk transitions with trailing_ones/zeros.
                let mut w = word;
                let mut i = 0usize;
                while i < bits_in_word {
                    let is_one = w & 1 == 1;
                    let step = if is_one {
                        (w.trailing_ones() as usize).min(bits_in_word - i)
                    } else {
                        (w.trailing_zeros() as usize).min(bits_in_word - i)
                    };
                    if is_one != run_select {
                        flush(&mut raw, run_select, run_len);
                        run_select = is_one;
                        run_len = 0;
                    }
                    run_len += step;
                    w = if step >= 64 { 0 } else { w >> step };
                    i += step;
                }
            }
            pos += bits_in_word;
        }
        debug_assert_eq!(pos, self.len);
        flush(&mut raw, run_select, run_len);

        let selection = RowSelection::from(raw);
        if min_skip_run <= 1 {
            selection
        } else {
            super::row_selection::coalesce_row_selection_with_min_skip_run(selection, min_skip_run)
        }
    }

    /// Zero-copy `BooleanArray` over bits `[offset, offset+len)`. Used when
    /// delivered row `i` corresponds to RG position `offset + i` (Identity
    /// position map — full-RG decode).
    pub fn boolean_slice(&self, offset: usize, len: usize) -> BooleanArray {
        debug_assert!(offset + len <= self.len);
        BooleanArray::new(BooleanBuffer::new(self.buffer.clone(), offset, len), None)
    }

    /// Copy bits `[src_start, src_start+len)` of this bitset into `dst`
    /// starting at bit `dst_start`. Word-shifted; no per-bit iteration.
    pub fn copy_bits_into(&self, src_start: usize, len: usize, dst: &mut [u64], dst_start: usize) {
        if len == 0 {
            return;
        }
        let words = self.words();
        let src_shift = src_start % 64;
        let mut src_word = src_start / 64;
        let mut produced = 0usize;
        while produced < len {
            // Assemble the next aligned source word.
            let lo = words.get(src_word).copied().unwrap_or(0);
            let hi = if src_shift > 0 {
                words.get(src_word + 1).copied().unwrap_or(0)
            } else {
                0
            };
            let mut s = if src_shift > 0 {
                (lo >> src_shift) | (hi << (64 - src_shift))
            } else {
                lo
            };
            let take = (len - produced).min(64);
            if take < 64 {
                s &= (1u64 << take) - 1;
            }
            // OR into destination at dst_start + produced.
            let d = dst_start + produced;
            let dw = d / 64;
            let ds = d % 64;
            if dw < dst.len() {
                dst[dw] |= s << ds;
            }
            if ds > 0 && dw + 1 < dst.len() {
                dst[dw + 1] |= s >> (64 - ds);
            }
            produced += take;
            src_word += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference implementation on RoaringBitmap for differential checks.
    fn roaring_from(bits: &[usize]) -> RoaringBitmap {
        bits.iter().map(|&b| b as u32).collect()
    }

    fn dense_from(bits: &[usize], len: usize) -> DenseBitset {
        let mut b = DenseBitsetBuilder::zeros(len);
        for &bit in bits {
            let mut word = [0u64; 1];
            word[0] = 1;
            b.or_lsb0_words(bit, &word, 1);
        }
        b.freeze()
    }

    #[test]
    fn or_lsb0_unaligned_offset() {
        let mut b = DenseBitsetBuilder::zeros(200);
        // set bits 70..134 (64 bits at offset 70)
        b.or_lsb0_words(70, &[!0u64], 64);
        let d = b.freeze();
        assert_eq!(d.count_ones(), 64);
        assert!(!d.get(69));
        assert!(d.get(70));
        assert!(d.get(133));
        assert!(!d.get(134));
    }

    #[test]
    fn or_lsb0_num_bits_masks_source() {
        let mut b = DenseBitsetBuilder::zeros(100);
        b.or_lsb0_words(0, &[!0u64], 10);
        let d = b.freeze();
        assert_eq!(d.count_ones(), 10);
        assert!(d.get(9));
        assert!(!d.get(10));
    }

    #[test]
    fn or_lsb0_clips_to_len() {
        let mut b = DenseBitsetBuilder::zeros(70);
        b.or_lsb0_words(60, &[!0u64], 64);
        let d = b.freeze();
        assert_eq!(d.count_ones(), 10); // only 60..70 fit
    }

    #[test]
    fn set_range_and_retain_ranges() {
        let mut b = DenseBitsetBuilder::zeros(300);
        b.set_range(0, 300);
        b.retain_ranges(&[(10, 20), (100, 200)]);
        let d = b.freeze();
        assert_eq!(d.count_ones(), 10 + 100);
        assert!(!d.get(9));
        assert!(d.get(10));
        assert!(d.get(19));
        assert!(!d.get(20));
        assert!(d.get(100));
        assert!(d.get(199));
        assert!(!d.get(200));
    }

    #[test]
    fn and_lsb0_clears_outside_window() {
        let mut b = DenseBitsetBuilder::zeros(200);
        b.set_range(0, 200);
        // peer window covers [64, 128) with all bits set
        b.and_lsb0_words(64, &[!0u64], 64);
        let d = b.freeze();
        assert_eq!(d.count_ones(), 64);
        assert!(!d.get(63));
        assert!(d.get(64));
        assert!(d.get(127));
        assert!(!d.get(128));
    }

    #[test]
    fn select_matches_roaring() {
        let bits = [3usize, 64, 65, 130, 199, 250, 511];
        let d = dense_from(&bits, 512);
        let r = roaring_from(&bits);
        for n in 0..bits.len() {
            assert_eq!(
                d.select(n),
                r.select(n as u32).map(|v| v as usize),
                "select({n})"
            );
        }
        assert_eq!(d.select(bits.len()), None);
    }

    #[test]
    fn to_roaring_round_trip() {
        let bits = [0usize, 1, 63, 64, 100, 500, 899];
        let d = dense_from(&bits, 900);
        assert_eq!(d.to_roaring(), roaring_from(&bits));
    }

    #[test]
    fn to_row_selection_matches_reference() {
        use crate::indexed_table::row_selection::build_row_selection_with_min_skip_run;
        // Deterministic pseudo-random patterns at several densities.
        for (seed, density) in [(1u64, 0.9), (2, 0.5), (3, 0.05), (4, 1.0), (5, 0.0)] {
            let len = 1000usize;
            let mut state = seed;
            let mut bits = Vec::new();
            for i in 0..len {
                // xorshift
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if (state % 1000) as f64 / 1000.0 < density {
                    bits.push(i);
                }
            }
            let d = dense_from(&bits, len);
            let r = roaring_from(&bits);
            for min_skip_run in [1usize, 4, 100] {
                let got = d.to_row_selection(min_skip_run);
                let expected = build_row_selection_with_min_skip_run(&r, len, min_skip_run);
                assert_eq!(
                    got, expected,
                    "seed={seed} density={density} min_skip_run={min_skip_run}"
                );
            }
        }
    }

    #[test]
    fn to_row_selection_empty_and_full() {
        let d = dense_from(&[], 100);
        assert_eq!(
            d.to_row_selection(1),
            RowSelection::from(vec![RowSelector::skip(100)])
        );
        let all: Vec<usize> = (0..100).collect();
        let d = dense_from(&all, 100);
        assert_eq!(
            d.to_row_selection(1),
            RowSelection::from(vec![RowSelector::select(100)])
        );
    }

    #[test]
    fn boolean_slice_zero_copy_matches_bits() {
        let bits = [1usize, 65, 66, 200];
        let d = dense_from(&bits, 256);
        let arr = d.boolean_slice(64, 64);
        assert_eq!(arr.len(), 64);
        assert!(arr.value(1)); // bit 65
        assert!(arr.value(2)); // bit 66
        assert!(!arr.value(0));
        assert_eq!(arr.true_count(), 2);
    }

    #[test]
    fn copy_bits_into_unaligned() {
        let bits = [70usize, 71, 90];
        let d = dense_from(&bits, 128);
        let mut dst = vec![0u64; 2];
        // copy src bits [70, 100) to dst bit 5
        d.copy_bits_into(70, 30, &mut dst, 5);
        // 70 -> 5, 71 -> 6, 90 -> 25
        assert_eq!(dst[0] & (1 << 5), 1 << 5);
        assert_eq!(dst[0] & (1 << 6), 1 << 6);
        assert_eq!(dst[0] & (1 << 25), 1 << 25);
        let total: u32 = dst.iter().map(|w| w.count_ones()).sum();
        assert_eq!(total, 3);
    }
}
