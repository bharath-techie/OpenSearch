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
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get all active PIT contexts in the cluster
 */
public class TransportPitGetAllPitsAction extends HandledTransportAction<GetAllPitNodesRequest, GetAllPitNodesResponse> {

    private final PitService pitService;

    @Inject
    public TransportPitGetAllPitsAction(ActionFilters actionFilters, TransportService transportService, PitService pitService) {
        super(PitGetAllPitsAction.NAME, transportService, actionFilters, in -> new GetAllPitNodesRequest(in));
        this.pitService = pitService;
    }

    @Override
    protected void doExecute(Task task, GetAllPitNodesRequest request, ActionListener<GetAllPitNodesResponse> listener) {
        pitService.getAllPits(listener);
    }
}
