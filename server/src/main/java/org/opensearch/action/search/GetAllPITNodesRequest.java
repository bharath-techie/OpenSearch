/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetAllPITNodesRequest extends BaseNodesRequest<GetAllPITNodesRequest> {

    //DiscoveryNode[] concreteNodes;
    @Inject
    public GetAllPITNodesRequest(DiscoveryNode... concreteNodes) {
    super(concreteNodes);
  }

    public GetAllPITNodesRequest(StreamInput in) throws IOException {
        super(in);
        //this.concreteNodes = in.readOptionalArray(DiscoveryNode::new, DiscoveryNode[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        //out.writeOptionalArray(concreteNodes);
    }
}
