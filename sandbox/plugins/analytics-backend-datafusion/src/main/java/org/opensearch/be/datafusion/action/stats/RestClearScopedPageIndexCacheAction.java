/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Clears the process-global scoped page-index cache on the local node (drops
 * entries + resets counters, keeps the configured budget).
 *
 * <p>Operational/testing convenience: reset the cache and re-measure via the
 * stats endpoint without a cluster restart.
 *
 * <pre>POST /_plugins/_analytics_backend_datafusion/cache/scoped_page_index/_clear</pre>
 *
 * <p>Note: this acts on the node that receives the request only. For a single-node
 * benchmark cluster that is exactly what's wanted; for multi-node clusters, send
 * it to each node (or restart).
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
        return channel -> {
            NativeBridge.clearScopedPageIndexCache();
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("cleared", "scoped_page_index_cache");
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
