/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.search.SearchService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action to get all PIT contexts
 */
public class TransportGetAllPITsAction extends TransportNodesAction<
    GetAllPITNodesRequest,
    GetAllPITNodesResponse,
    GetAllPITNodeRequest,
    GetAllPITNodeResponse> {

    private final SearchService searchService;

    @Inject
    public TransportGetAllPITsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SearchService searchService
    ) {
        super(
            GetAllPITsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            GetAllPITNodesRequest::new,
            GetAllPITNodeRequest::new,
            ThreadPool.Names.SAME,
            GetAllPITNodeResponse.class
        );
        this.searchService = searchService;
    }

    @Override
    protected GetAllPITNodesResponse newResponse(
        GetAllPITNodesRequest request,
        List<GetAllPITNodeResponse> getAllPITNodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetAllPITNodesResponse(clusterService.getClusterName(), getAllPITNodeResponses, failures);
    }

    @Override
    protected GetAllPITNodeRequest newNodeRequest(GetAllPITNodesRequest request) {
        return new GetAllPITNodeRequest(request);
    }

    @Override
    protected GetAllPITNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new GetAllPITNodeResponse(in);
    }

    /**
     * This node specific operation retrieves all node specific information
     */
    @Override
    protected GetAllPITNodeResponse nodeOperation(GetAllPITNodeRequest request) {
        GetAllPITNodeResponse nodeResponse = new GetAllPITNodeResponse(
            transportService.getLocalNode(),
            searchService.getAllPITReaderContexts()
        );
        return nodeResponse;
    }
}
