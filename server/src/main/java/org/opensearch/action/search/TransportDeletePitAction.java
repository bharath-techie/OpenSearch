/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport action for deleting point in time searches - supports deleting list and all point in time searches
 */
public class TransportDeletePitAction extends HandledTransportAction<DeletePitRequest, DeletePitResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final PitService pitService;

    @Inject
    public TransportDeletePitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportSearchAction transportSearchAction,
        ClusterService clusterService,
        SearchTransportService searchTransportService,
        PitService pitService
    ) {
        super(DeletePitAction.NAME, transportService, actionFilters, DeletePitRequest::new);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.pitService = pitService;
    }

    /**
     * Invoke 'delete all pits' or 'delete list of pits' workflow based on request
     */
    @Override
    protected void doExecute(Task task, DeletePitRequest request, ActionListener<DeletePitResponse> listener) {
        List<String> pitIds = request.getPitIds();
        // when security plugin intercepts the request, if PITs are not present in the cluster the PIT IDs in request will be empty
        // and in this case return empty response
        if (pitIds.isEmpty()) {
            listener.onResponse(new DeletePitResponse(new ArrayList<>()));
        } else {
            deletePits(listener, request);
        }
    }

    /**
     * Deletes one or more point in time search contexts.
     */
    private void deletePits(ActionListener<DeletePitResponse> listener, DeletePitRequest request) {
        Map<String, List<PitSearchContextIdForNode>> nodeToContextsMap = new HashMap<>();
        for (String pitId : request.getPitIds()) {
            SearchContextId contextId = SearchContextId.decode(namedWriteableRegistry, pitId);
            for (SearchContextIdForNode contextIdForNode : contextId.shards().values()) {
                PitSearchContextIdForNode pitSearchContext = new PitSearchContextIdForNode(pitId, contextIdForNode);
                List<PitSearchContextIdForNode> contexts = nodeToContextsMap.getOrDefault(contextIdForNode.getNode(), new ArrayList<>());
                contexts.add(pitSearchContext);
                nodeToContextsMap.put(contextIdForNode.getNode(), contexts);
            }
        }
        pitService.deletePitContexts(nodeToContextsMap, listener);
    }
}
