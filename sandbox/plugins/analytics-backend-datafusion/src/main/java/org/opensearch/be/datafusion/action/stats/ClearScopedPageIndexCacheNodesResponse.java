/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Aggregated cluster-wide response for the clear-scoped-cache broadcast. The
 * {@code NodesResponseRestListener} wrapper supplies the {@code _nodes} header and
 * {@code cluster_name}; this fragment adds only {@code acknowledged} and the list
 * of node IDs that cleared.
 *
 * @opensearch.internal
 */
public class ClearScopedPageIndexCacheNodesResponse extends BaseNodesResponse<ClearScopedPageIndexCacheNodeResponse>
    implements
        ToXContentFragment {

    public ClearScopedPageIndexCacheNodesResponse(
        ClusterName clusterName,
        List<ClearScopedPageIndexCacheNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    public ClearScopedPageIndexCacheNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<ClearScopedPageIndexCacheNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ClearScopedPageIndexCacheNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClearScopedPageIndexCacheNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("acknowledged", failures() == null || failures().isEmpty());
        builder.field("cleared", "scoped_page_index_cache");
        builder.startArray("cleared_nodes");
        for (ClearScopedPageIndexCacheNodeResponse node : getNodes()) {
            builder.value(node.getNode().getId());
        }
        builder.endArray();
        return builder;
    }
}
