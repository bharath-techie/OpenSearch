/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Broadcast transport action that clears the process-global scoped page-index
 * caches (ColumnIndex + OffsetIndex) on every target node. Mirrors
 * {@link TransportDataFusionStatsAction}'s node fan-out so the operation reaches
 * all nodes — not just the one that received the REST request.
 *
 * @opensearch.internal
 */
public class TransportClearScopedPageIndexCacheAction extends TransportNodesAction<
    ClearScopedPageIndexCacheNodesRequest,
    ClearScopedPageIndexCacheNodesResponse,
    ClearScopedPageIndexCacheNodeRequest,
    ClearScopedPageIndexCacheNodeResponse> {

    @Inject
    public TransportClearScopedPageIndexCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            ClearScopedPageIndexCacheActionType.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClearScopedPageIndexCacheNodesRequest::new,
            ClearScopedPageIndexCacheNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ClearScopedPageIndexCacheNodeResponse.class
        );
    }

    @Override
    protected ClearScopedPageIndexCacheNodesResponse newResponse(
        ClearScopedPageIndexCacheNodesRequest request,
        List<ClearScopedPageIndexCacheNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ClearScopedPageIndexCacheNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ClearScopedPageIndexCacheNodeRequest newNodeRequest(ClearScopedPageIndexCacheNodesRequest request) {
        return new ClearScopedPageIndexCacheNodeRequest();
    }

    @Override
    protected ClearScopedPageIndexCacheNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClearScopedPageIndexCacheNodeResponse(in);
    }

    @Override
    protected ClearScopedPageIndexCacheNodeResponse nodeOperation(ClearScopedPageIndexCacheNodeRequest request) {
        NativeBridge.clearScopedPageIndexCache();
        return new ClearScopedPageIndexCacheNodeResponse(clusterService.localNode());
    }
}
