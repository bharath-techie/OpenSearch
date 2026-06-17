/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.ActionType;

/**
 * Action type for the broadcast "clear scoped page-index caches" transport action.
 *
 * <p>The scoped ColumnIndex and OffsetIndex caches are process-global singletons,
 * one per node. Clearing them must fan out to <b>every</b> node, otherwise a
 * single-node REST handler would leave the other nodes' caches populated — which
 * is both surprising operationally and breaks cluster-aggregated stats assertions.
 *
 * @opensearch.internal
 */
public class ClearScopedPageIndexCacheActionType extends ActionType<ClearScopedPageIndexCacheNodesResponse> {

    public static final String NAME = "cluster:admin/_analytics_backend_datafusion/cache/scoped_page_index/clear";
    public static final ClearScopedPageIndexCacheActionType INSTANCE = new ClearScopedPageIndexCacheActionType();

    private ClearScopedPageIndexCacheActionType() {
        super(NAME, ClearScopedPageIndexCacheNodesResponse::new);
    }
}
