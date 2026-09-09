/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

/**
 * The leading {@code index.sort.field} of a fragment's backing index together with its
 * {@code index.sort.order}. Resolved once at conversion time (see the resolver on
 * {@link FragmentConversionDriver#convertAll}) and consumed by {@link FastPathHintExtractor}.
 *
 * <p>The order matters for the TOPK fast path: rows inside a row group are stored in index sort
 * order, so a query asking for the "newest N" on an ASC index keeps the LAST N of each row group.
 * That decision needs the index order, which only the resolver (with cluster state) knows.
 *
 * @param field      the leading sort field name
 * @param descending true when {@code index.sort.order} is {@code desc} for the leading field
 *
 * @opensearch.internal
 */
public record LeadingSortInfo(String field, boolean descending) {
}
