/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tp
 */
public class TransportPitGetAllPitsAction extends HandledTransportAction<GetAllPitNodesRequest, GetAllPitNodesResponse> {
    private static final Logger logger = LogManager.getLogger(TransportPitGetAllPitsAction.class);

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final TransportService transportService;

    @Inject
    public TransportPitGetAllPitsAction(ClusterService clusterService,
                                        ActionFilters actionFilters,
                                        SearchTransportService searchTransportService, TransportService transportService) {
        super(PitGetAllPitsAction.NAME, transportService, actionFilters, in -> new GetAllPitNodesRequest(in));
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, GetAllPitNodesRequest request, ActionListener<GetAllPitNodesResponse> listener) {
        getAllPits(listener);
    }

    /**
     * Get all active point in time contexts
     */
    public void getAllPits(ActionListener<GetAllPitNodesResponse> getAllPitsListener) {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (ObjectCursor<DiscoveryNode> cursor : clusterService.state().nodes().getDataNodes().values()) {
            DiscoveryNode node = cursor.value;
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = nodes.toArray(new DiscoveryNode[nodes.size()]);
        transportService.sendRequest(
                transportService.getLocalNode(),
                NodesGetAllPitsAction.NAME,
                new GetAllPitNodesRequest(disNodesArr),
                new TransportResponseHandler<GetAllPitNodesResponse>() {

                    @Override
                    public void handleResponse(GetAllPitNodesResponse response) {
                        getAllPitsListener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        getAllPitsListener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public GetAllPitNodesResponse read(StreamInput in) throws IOException {
                        return new GetAllPitNodesResponse(in);
                    }
                }
        );
    }
}
