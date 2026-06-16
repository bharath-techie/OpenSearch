/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Clears the process-global scoped page-index caches (ColumnIndex + OffsetIndex)
 * across ALL nodes in the cluster (drops entries + resets counters, keeps the
 * configured budgets).
 *
 * <p>Operational/testing convenience: reset the caches and re-measure via the
 * stats endpoint without a cluster restart. Because the caches are per-node
 * process-global singletons, this broadcasts to every node via
 * {@link ClearScopedPageIndexCacheActionType} — clearing only the receiving node
 * would leave the cluster-aggregated stats non-zero.
 *
 * <pre>POST /_plugins/_analytics_backend_datafusion/cache/scoped_page_index/_clear</pre>
 *
 * @opensearch.internal
 */
public class RestClearScopedPageIndexCacheAction extends BaseRestHandler {

    private static final String ROUTE = "/_plugins/_analytics_backend_datafusion/cache/scoped_page_index/_clear";

    @Override
    public String getName() {
        return "datafusion_clear_scoped_page_index_cache_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, ROUTE));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Empty node-ids → broadcast to all nodes.
        ClearScopedPageIndexCacheNodesRequest nodesRequest = new ClearScopedPageIndexCacheNodesRequest();
        return channel -> client.execute(
            ClearScopedPageIndexCacheActionType.INSTANCE,
            nodesRequest,
            new NodesResponseRestListener<>(channel)
        );
    }
}
