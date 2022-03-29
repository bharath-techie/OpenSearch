/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetAllPITNodesResponse extends BaseNodesResponse<GetAllPITNodeResponse> implements ToXContentObject, StatusToXContentObject {

    List<String> pitIds = new ArrayList<>();

    @Inject
    public GetAllPITNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetAllPITNodesResponse(ClusterName clusterName, List<GetAllPITNodeResponse> getAllPITNodeResponses, List<FailedNodeException> failures) {
        super(clusterName, getAllPITNodeResponses, failures);
        for(GetAllPITNodeResponse response : getAllPITNodeResponses) {
            pitIds.addAll(response.getPitIds());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public List<GetAllPITNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetAllPITNodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<GetAllPITNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public RestStatus status() {
        return null;
    }

    public List<String> getPITIDs() {
        return pitIds;
    }
}
