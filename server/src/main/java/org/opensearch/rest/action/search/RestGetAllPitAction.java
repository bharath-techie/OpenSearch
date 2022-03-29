/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.search.GetAllPITNodesRequest;
import org.opensearch.action.search.GetAllPITNodesResponse;
import org.opensearch.action.search.GetAllPITsAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestGetAllPitAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "get_all_pit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) throws IOException {
                final List<DiscoveryNode> nodes = new LinkedList<>();
                for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().getDataNodes().values()) {
                    DiscoveryNode node = cursor.value;
                    nodes.add(node);
                }
                DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
                nodes.toArray(disNodesArr);
                GetAllPITNodesRequest getAllPITNodesRequest = new GetAllPITNodesRequest(disNodesArr);
                client.execute(GetAllPITsAction.INSTANCE, getAllPITNodesRequest, new RestResponseListener<GetAllPITNodesResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(final GetAllPITNodesResponse getAllPITNodesResponse) throws Exception {
                        return null;
                    }
                });
            }
        });
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(Collections.singletonList(
            new Route(GET, "/_pit/all")));
    }
}
