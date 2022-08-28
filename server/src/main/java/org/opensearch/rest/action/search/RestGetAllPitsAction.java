/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action for retrieving all active PIT IDs across all nodes
 */
public class RestGetAllPitsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_all_pit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        boolean includeAll = request.paramAsBoolean("include_all", false);
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest();
        return channel -> client.admin()
            .cluster()
            .getAllPits(getAllPITNodesRequest, new RestResponseListener<GetAllPitNodesResponse>(channel) {
                @Override
                public RestResponse buildResponse(final GetAllPitNodesResponse getAllPITNodesResponse) throws Exception {
                    try (XContentBuilder builder = channel.newBuilder()) {
                        builder.startObject();
                        if (getAllPITNodesResponse.hasFailures()) {
                            builder.startArray("failures");
                            for (int idx = 0; idx < getAllPITNodesResponse.failures().size(); idx++) {
                                builder.field(
                                    getAllPITNodesResponse.failures().get(idx).nodeId(),
                                    getAllPITNodesResponse.failures().get(idx).getDetailedMessage()
                                );
                            }
                            builder.endArray();
                        }
                        builder.field("pits", getAllPITNodesResponse.getPitInfos());
                        if (includeAll) {
                            builder.startArray("nodeResults");
                            for (int idx = 0; idx < getAllPITNodesResponse.getNodes().size(); idx++) {
                                builder.value(getAllPITNodesResponse.getNodes().get(idx));
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.OK, builder);
                    }
                }
            });
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(Collections.singletonList(new Route(GET, "/_search/point_in_time/all")));
    }

}
