/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Cluster-level request to clear the scoped page-index caches on the target nodes
 * (empty {@code nodesIds} means all nodes). Carries no payload — clearing is
 * unconditional.
 *
 * @opensearch.internal
 */
public class ClearScopedPageIndexCacheNodesRequest extends BaseNodesRequest<ClearScopedPageIndexCacheNodesRequest> {

    public ClearScopedPageIndexCacheNodesRequest(String... nodesIds) {
        super(nodesIds);
    }

    public ClearScopedPageIndexCacheNodesRequest(StreamInput in) throws IOException {
        super(in);
    }
}
