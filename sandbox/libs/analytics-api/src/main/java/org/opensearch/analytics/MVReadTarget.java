/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

/**
 * A materialized view matched to the current query, carried from the front-end into
 * planning. The engine's read rewrite replaces the query's shard-side PARTIAL aggregate
 * subtree with a scan of the view's state columns; the untouched FINAL half then folds
 * the stored partial states exactly as it folds shard-produced ones.
 *
 * @param viewIndex the view's backing index
 * @param specJson  the view's {@code index.parquet.mv.spec} describe document — key
 *                  columns, per-aggregate function names and input types, and the
 *                  state-column schema (see the MV architecture doc)
 *
 * @opensearch.internal
 */
public record MVReadTarget(String viewIndex, String specJson) {
}
