/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportResponse;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DeletePITController implements Runnable {
    private final DiscoveryNodes nodes;
    private final SearchTransportService searchTransportService;
    private final CountDown expectedOps;
    private final ActionListener<DeletePITResponse> listener;
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);
    private final AtomicInteger freedSearchContexts = new AtomicInteger(0);
    private final ClusterService clusterService;
    private final Runnable runner;

    public DeletePITController(
        DeletePITRequest request,
        ActionListener<DeletePITResponse> listener,
        ClusterService clusterService,
        SearchTransportService searchTransportService
    ) {
        this.nodes = clusterService.state().getNodes();
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.listener = listener;
        List<String> pitIds = request.getPitIds();
        final int expectedOps;
        if (pitIds.size() == 1 && "_all".equals(pitIds.get(0))) {
            expectedOps = nodes.getSize();
            runner = this::deleteAllPits;
        } else {
            // TODO: replace this with #closeContexts
            List<SearchContextIdForNode> contexts = new ArrayList<>();
            for (String scrollId : request.getPitIds()) {
                SearchContextIdForNode[] context = TransportSearchHelper.parseScrollId(scrollId).getContext();
                Collections.addAll(contexts, context);
            }
            if (contexts.isEmpty()) {
                expectedOps = 0;
                runner = () -> listener.onResponse(new DeletePITResponse(true));
            } else {
                expectedOps = contexts.size();
                runner = () -> ClearScrollController.closeContexts(
                    clusterService.state().nodes(),
                    searchTransportService,
                    contexts,
                    ActionListener.wrap(r -> listener.onResponse(new DeletePITResponse(true)), listener::onFailure)
                );
            }
        }
        this.expectedOps = new CountDown(expectedOps);

    }

    @Override
    public void run() {
        runner.run();
    }

    void deleteAllPits() {
        for (final DiscoveryNode node : clusterService.state().getNodes()) {
            try {
                Transport.Connection connection = searchTransportService.getConnection(null, node);
                searchTransportService.sendDeleteAllPitContexts(connection, new ActionListener<TransportResponse>() {
                    @Override
                    public void onResponse(TransportResponse response) {
                        onFreedContext(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailedFreedContext(e, node);
                    }
                });
            } catch (Exception e) {
                onFailedFreedContext(e, node);
            }
        }
    }

    public static class PITSinglePhaseSearchResult extends SearchPhaseResult {
        public void setContextId(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }
    }

    private void onFreedContext(boolean freed) {
        if (freed) {
            freedSearchContexts.incrementAndGet();
        }
        if (expectedOps.countDown()) {
            boolean succeeded = hasFailed.get() == false;
            listener.onResponse(new DeletePITResponse(succeeded));
        }
    }

    private void onFailedFreedContext(Throwable e, DiscoveryNode node) {
        /*
         * We have to set the failure marker before we count down otherwise we can expose the failure marker before we have set it to a
         * racing thread successfully freeing a context. This would lead to that thread responding that the clear scroll succeeded.
         */
        hasFailed.set(true);
        if (expectedOps.countDown()) {
            listener.onResponse(new DeletePITResponse(false));
        }
    }
}
