/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Per-node response confirming the scoped page-index caches were cleared on this
 * node. Carries only the node identity (the operation has no per-node payload).
 *
 * @opensearch.internal
 */
public class ClearScopedPageIndexCacheNodeResponse extends BaseNodeResponse {

    public ClearScopedPageIndexCacheNodeResponse(DiscoveryNode node) {
        super(node);
    }

    public ClearScopedPageIndexCacheNodeResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
