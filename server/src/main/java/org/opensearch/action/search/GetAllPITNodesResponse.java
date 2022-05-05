/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Response structure to hold all active PIT contexts information from all nodes
 */
public class GetAllPITNodesResponse extends BaseNodesResponse<GetAllPITNodeResponse> implements ToXContentObject {

    List<PitInfo> pitsInfo = new ArrayList<>();

    @Inject
    public GetAllPITNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetAllPITNodesResponse(
        ClusterName clusterName,
        List<GetAllPITNodeResponse> getAllPITNodeResponses,
        List<FailedNodeException> failures
    ) {
        super(clusterName, getAllPITNodeResponses, failures);
        Set<String> uniquePitIds = new HashSet<>();
        pitsInfo.addAll(
            getAllPITNodeResponses.stream()
                .flatMap(p -> p.getPitsInfo().stream().filter(t -> uniquePitIds.add(t.getPitId())))
                .collect(Collectors.toList())
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("pitsInfo");
        for (PitInfo pit : pitsInfo) {
            pit.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public List<GetAllPITNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetAllPITNodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<GetAllPITNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    public List<PitInfo> getPITIDs() {
        return new ArrayList<>(pitsInfo);
    }
}
