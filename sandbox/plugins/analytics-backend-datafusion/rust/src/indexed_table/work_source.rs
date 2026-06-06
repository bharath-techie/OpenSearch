/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Dynamic work scheduling ("work-stealing") for the indexed scan.
//!
//! Port of DataFusion's [`SharedWorkSource`] (PR #21351) to our execution tree.
//! Instead of each partition draining a *fixed* set of chunks assigned at
//! planning time (`QueryShardExec.assignments`), reorderable scans share **one**
//! queue across all sibling partition streams of a single execution. Whichever
//! partition goes idle first pops the next chunk. This removes the idle-tail on
//! imbalanced scans — exactly the case dynamic filtering creates (one partition's
//! row groups get pruned away while another's don't).
//!
//! # Unit of work
//!
//! A [`WorkItem`] is one [`SegmentChunk`] (a contiguous RG subset within a single
//! segment) plus the `segment_idx` it belongs to. This is the same granularity
//! the per-partition loop uses today (`table_provider.rs`), so nothing downstream
//! changes — only *who* processes a chunk.
//!
//! # Correctness: ordering
//!
//! `QueryShardExec` advertises `EquivalenceProperties::new(..)` with **no output
//! ordering** (`table_provider.rs`), so DataFusion never assumes our emission
//! order. Any `ORDER BY` is enforced by a `SortExec` above us that re-sorts
//! regardless of the order chunks complete in. Therefore reordering / stealing
//! chunks can never produce a wrong answer — it only changes *when* work happens.
//! (This is the analogue of DataFusion's `preserve_order` opt-out, which for us
//! is always satisfied.)
//!
//! # Tier 2: statistics reorder (PR #21956)
//!
//! For a TopK, work-stealing pays off most when the most-promising segments are
//! processed first, so the dynamic-filter threshold tightens fast. The TopK's
//! filter is a bare `true` placeholder at `execute()` time and only tightens to
//! `col <op> threshold` once rows flow, so the sort *direction* isn't knowable up
//! front. We therefore start FIFO and do a **one-shot reorder of the remaining
//! queue** the first time the filter becomes concrete — see
//! [`SharedChunkQueue::reorder_remaining`]. Mirrors
//! `reorder_files_by_min_statistics` at segment ("file") granularity.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::partitioning::{PartitionAssignment, SegmentChunk};

/// One stealable unit of scan work: a chunk plus the segment it came from.
#[derive(Clone, Debug)]
pub(crate) struct WorkItem {
    pub segment_idx: usize,
    pub chunk: SegmentChunk,
}

/// Per-partition handle to scan work.
///
/// `Local` reproduces today's behaviour exactly (this partition's own chunks, in
/// assignment order, never shared). `Shared` pulls from the cross-partition pool.
#[derive(Clone, Debug)]
pub(crate) enum IndexedWorkSource {
    /// This partition's own chunks; no sharing (feature off, or unsafe to steal).
    Local(VecDeque<WorkItem>),
    /// Steal from the queue shared by all sibling partitions of this execution.
    Shared(SharedChunkQueue),
}

impl IndexedWorkSource {
    /// Pop the next chunk to process, or `None` when this source is drained.
    pub(crate) fn pop(&mut self) -> Option<WorkItem> {
        match self {
            Self::Local(items) => items.pop_front(),
            Self::Shared(q) => q.pop(),
        }
    }
}

/// Source of work shared by sibling `IndexedExec` streams of one execution.
///
/// Created once per execution (guarded by a `OnceLock` on `QueryShardExec`) and
/// cloned — by `Arc` — into every partition. Thread-safe via a single mutex; the
/// queue is touched once per *chunk* (many RGs of work), so contention is nil.
#[derive(Clone, Debug)]
pub(crate) struct SharedChunkQueue {
    inner: Arc<Mutex<SharedState>>,
}

#[derive(Debug)]
struct SharedState {
    items: VecDeque<WorkItem>,
    /// Set once the Tier-2 reorder has run, so it happens at most once.
    reordered: bool,
}

impl SharedChunkQueue {
    /// Flatten every partition's chunks into one FIFO queue (Tier 1 order).
    pub(crate) fn from_assignments(assignments: &[PartitionAssignment]) -> Self {
        let items: VecDeque<WorkItem> = assignments
            .iter()
            .flat_map(|a| {
                a.chunks.iter().map(|chunk| WorkItem {
                    segment_idx: chunk.segment_idx,
                    chunk: chunk.clone(),
                })
            })
            .collect();
        Self {
            inner: Arc::new(Mutex::new(SharedState {
                items,
                reordered: false,
            })),
        }
    }

    /// Pop the next chunk off the front of the shared queue.
    pub(crate) fn pop(&self) -> Option<WorkItem> {
        self.inner.lock().unwrap().items.pop_front()
    }

    /// One-shot Tier-2 reorder of the *remaining* queued chunks (PR #21956,
    /// segment granularity). Sorts the still-unprocessed items by their
    /// segment's `min(sort_col)` statistic: ascending for an ASC TopK, descending
    /// for DESC. Segments whose key is unknown (`None`) sort to the end so
    /// known-stat segments run first. Idempotent — runs at most once; later calls
    /// are a cheap locked no-op.
    ///
    /// `key_for_segment` maps a `segment_idx` to its reorder key (the segment's
    /// `min` for the sort column), or `None` when stats are missing. Computed by
    /// the caller (which holds the `ParquetMetaData`); kept out of this module so
    /// the queue stays dependency-free and easy to test.
    pub(crate) fn reorder_remaining<F>(&self, descending: bool, key_for_segment: F)
    where
        F: Fn(usize) -> Option<ReorderKey>,
    {
        let mut state = self.inner.lock().unwrap();
        if state.reordered {
            return;
        }
        state.reordered = true;

        // Cache one key lookup per distinct segment (chunks of the same segment
        // share a key), then stable-sort so same-segment chunks keep their
        // relative order.
        let mut items: Vec<WorkItem> = state.items.drain(..).collect();
        items.sort_by(|a, b| {
            let ka = key_for_segment(a.segment_idx);
            let kb = key_for_segment(b.segment_idx);
            match (ka, kb) {
                (Some(va), Some(vb)) => {
                    let cmp = va.cmp(&vb);
                    if descending {
                        cmp.reverse()
                    } else {
                        cmp
                    }
                }
                // Known stats sort before unknown, regardless of direction.
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
        state.items = items.into();
    }

    /// Whether the Tier-2 reorder has already run (test/diagnostic).
    #[cfg(test)]
    pub(crate) fn was_reordered(&self) -> bool {
        self.inner.lock().unwrap().reordered
    }

    /// Number of items still queued (test/diagnostic).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().unwrap().items.len()
    }
}

/// Sort key for Tier-2 segment reorder: a segment's `min(sort_col)` mapped to a
/// totally-ordered scalar. We support the numeric/temporal column types a TopK
/// sort is realistically on; anything else yields `None` (segment sorts last,
/// i.e. the reorder is a no-op for it — always correctness-safe).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReorderKey {
    /// All signed integer + temporal (date/time/timestamp) mins, widened to i128.
    Int(i128),
    /// Float mins, stored as ordered bits so the key is `Ord`.
    Float(OrderedF64),
}

/// `f64` wrapper with a total order (NaN sorts greatest), so [`ReorderKey`] can
/// derive `Ord`. Only used as a sort key; never read back as a float.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OrderedF64(pub f64);

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal
    }
}
impl Eq for OrderedF64 {}
impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::partitioning::SegmentChunk;

    fn chunk(seg: usize, doc_min: i32) -> SegmentChunk {
        SegmentChunk {
            segment_idx: seg,
            doc_min,
            doc_max: doc_min + 10,
            row_group_indices: vec![0],
        }
    }

    fn assignment(chunks: Vec<SegmentChunk>) -> PartitionAssignment {
        PartitionAssignment { chunks }
    }

    #[test]
    fn fifo_order_preserved_without_reorder() {
        let q = SharedChunkQueue::from_assignments(&[
            assignment(vec![chunk(0, 0), chunk(0, 10)]),
            assignment(vec![chunk(1, 0)]),
        ]);
        assert_eq!(q.len(), 3);
        assert_eq!(q.pop().unwrap().segment_idx, 0);
        assert_eq!(q.pop().unwrap().chunk.doc_min, 10);
        assert_eq!(q.pop().unwrap().segment_idx, 1);
        assert!(q.pop().is_none());
    }

    #[test]
    fn shared_queue_is_drained_once_across_clones() {
        // Two "partitions" sharing one Arc-backed queue must not double-process.
        let q = SharedChunkQueue::from_assignments(&[
            assignment(vec![chunk(0, 0)]),
            assignment(vec![chunk(1, 0)]),
            assignment(vec![chunk(2, 0)]),
        ]);
        let mut a = IndexedWorkSource::Shared(q.clone());
        let mut b = IndexedWorkSource::Shared(q.clone());
        let mut seen = std::collections::HashSet::new();
        // Interleave pops from both handles; every chunk appears exactly once.
        while let Some(item) = a.pop().or_else(|| b.pop()) {
            assert!(seen.insert(item.segment_idx), "chunk processed twice");
        }
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn reorder_desc_puts_highest_min_first() {
        // segments 0,1,2 with mins 5,30,10. DESC → order 1(30),2(10),0(5).
        let q = SharedChunkQueue::from_assignments(&[
            assignment(vec![chunk(0, 0)]),
            assignment(vec![chunk(1, 0)]),
            assignment(vec![chunk(2, 0)]),
        ]);
        let mins = [5i128, 30, 10];
        q.reorder_remaining(true, |seg| Some(ReorderKey::Int(mins[seg])));
        assert!(q.was_reordered());
        assert_eq!(q.pop().unwrap().segment_idx, 1);
        assert_eq!(q.pop().unwrap().segment_idx, 2);
        assert_eq!(q.pop().unwrap().segment_idx, 0);
    }

    #[test]
    fn reorder_asc_and_missing_stats_sort_last() {
        let q = SharedChunkQueue::from_assignments(&[
            assignment(vec![chunk(0, 0)]), // min 5
            assignment(vec![chunk(1, 0)]), // missing
            assignment(vec![chunk(2, 0)]), // min 2
        ]);
        q.reorder_remaining(false, |seg| match seg {
            0 => Some(ReorderKey::Int(5)),
            2 => Some(ReorderKey::Int(2)),
            _ => None,
        });
        // ASC by min: 2(seg2), 5(seg0), then missing (seg1) last.
        assert_eq!(q.pop().unwrap().segment_idx, 2);
        assert_eq!(q.pop().unwrap().segment_idx, 0);
        assert_eq!(q.pop().unwrap().segment_idx, 1);
    }

    #[test]
    fn reorder_runs_at_most_once() {
        let q = SharedChunkQueue::from_assignments(&[
            assignment(vec![chunk(0, 0)]),
            assignment(vec![chunk(1, 0)]),
        ]);
        q.reorder_remaining(true, |seg| Some(ReorderKey::Int(seg as i128)));
        // A second call with the opposite direction must be a no-op.
        q.reorder_remaining(false, |seg| Some(ReorderKey::Int(seg as i128)));
        assert_eq!(q.pop().unwrap().segment_idx, 1); // still DESC order
        assert_eq!(q.pop().unwrap().segment_idx, 0);
    }
}
