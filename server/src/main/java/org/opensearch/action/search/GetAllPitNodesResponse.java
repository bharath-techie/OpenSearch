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
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class transforms active PIT objects from all nodes to unique PIT objects
 */
public class GetAllPitNodesResponse extends BaseNodesResponse<GetAllPitNodeResponse> implements ToXContentObject {

    /**
     * List of unique PITs across all nodes
     */
    private final Set<ListPitInfo> pitInfos = new HashSet<>();

    public GetAllPitNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetAllPitNodesResponse(
        ClusterName clusterName,
        List<GetAllPitNodeResponse> getAllPitNodeResponse,
        List<FailedNodeException> failures
    ) {
        super(clusterName, getAllPitNodeResponse, failures);
        Set<String> uniquePitIds = new HashSet<>();
        pitInfos.addAll(
            getAllPitNodeResponse.stream()
                .flatMap(p -> p.getPitInfos().stream().filter(t -> uniquePitIds.add(t.getPitId())))
                .collect(Collectors.toList())
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("pitInfos");
        for (ListPitInfo pit : pitInfos) {
            pit.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<GetAllPitNodesResponse, Void> PARSER = new ConstructingObjectParser<>(
            "get_all_pits_response",
            true,
            (Object[] parsedObjects) -> {
                @SuppressWarnings("unchecked")
                int i=0;
                ClusterName clusterName = (ClusterName) parsedObjects[i++];
                List<GetAllPitNodeResponse> getAllPitNodeResponse = (List<GetAllPitNodeResponse>) parsedObjects[i++];
                List<FailedNodeException> failures = ( List<FailedNodeException> ) parsedObjects[i++];
                return new GetAllPitNodesResponse(clusterName, getAllPitNodeResponse, failures);
            }
    );
    static {
        PARSER.declareString(constructorArg(), new ParseField("creationTime"));
        PARSER.declareObjectArray(constructorArg(), ListPitInfo.PARSER, new ParseField("pits"));
    }

    public static GetAllPitNodesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public List<GetAllPitNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetAllPitNodeResponse::new);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<GetAllPitNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    public List<ListPitInfo> getPitInfos() {
        return Collections.unmodifiableList(new ArrayList<>(pitInfos));
    }
}
