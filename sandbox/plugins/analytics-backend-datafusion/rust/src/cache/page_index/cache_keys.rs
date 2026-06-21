/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cache key types for the two scoped page-index caches.

use std::fmt::Display;
use std::sync::Arc;

use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;

/// Page-index cache key — one decoded `Vec<T>` (all row groups) per `(file,
/// column)`. Used by BOTH the ColumnIndex and OffsetIndex caches. The page
/// index for a given column is an intrinsic property of the file: it is
/// identical no matter which *other* columns a query filters on, which literal a
/// predicate uses, or which row groups survive footer-stats pruning. Keying at
/// column granularity means a column's index is decoded and stored **once per
/// file**, then reused by every query that touches it — regardless of the
/// predicate-column *combination*, projection set, or surviving-row-group set.
///
/// Both scan paths resolve the same `(file, col)` for the same logical request,
/// so entries are shared across paths → cross-path sharing.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct PageIndexColKey {
    pub(crate) path: Arc<str>,
    pub(crate) col: usize,
}

impl Display for PageIndexColKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.path, self.col)
    }
}

/// Type aliases for the two caches — same key, different value types.
pub(crate) type CiCellKey = PageIndexColKey;
pub(crate) type OiCellKey = PageIndexColKey;

/// One column's ColumnIndex across all row groups (indexed by RG). The value
/// type of [`COLUMN_INDEX_CACHE`].
pub(crate) type CiColumn = Vec<ColumnIndexMetaData>;

/// One column's OffsetIndex across all row groups (indexed by RG). The value
/// type of [`OFFSET_INDEX_CACHE`].
pub(crate) type OiColumn = Vec<OffsetIndexMetaData>;
