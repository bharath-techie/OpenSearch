/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Transport action for deleting pit reader context - supports deleting list and all pit contexts
 */
public class TransportDeletePitAction extends HandledTransportAction<DeletePitRequest, DeletePitResponse> {
    private final NamedWriteableRegistry namedWriteableRegistry;
    private TransportSearchAction transportSearchAction;
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private static final Logger logger = LogManager.getLogger(TransportDeletePitAction.class);

    @Inject
    public TransportDeletePitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportSearchAction transportSearchAction,
        ClusterService clusterService,
        SearchTransportService searchTransportService
    ) {
        super(DeletePitAction.NAME, transportService, actionFilters, DeletePitRequest::new);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transportSearchAction = transportSearchAction;
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
    }

    /**
     * Invoke delete all pits or delete list of pits workflow based on request
     */
    @Override
    protected void doExecute(Task task, DeletePitRequest request, ActionListener<DeletePitResponse> listener) {
        List<SearchContextIdForNode> contexts = new ArrayList<>();
        List<String> pitIds = request.getPitIds();
        if (pitIds.size() == 1 && "_all".equals(pitIds.get(0))) {
            deleteAllPits(listener);
        } else {
            for (String pitId : request.getPitIds()) {
                SearchContextId contextId = SearchContextId.decode(namedWriteableRegistry, pitId);
                contexts.addAll(contextId.shards().values());
            }
            deletePits(contexts, ActionListener.wrap(r -> {
                if (r == contexts.size()) {
                    listener.onResponse(new DeletePitResponse(true));
                } else {
                    logger.debug(
                        () -> new ParameterizedMessage("Delete PITs failed. " + "Cleared {} contexts out of {}", r, contexts.size())
                    );
                    listener.onResponse(new DeletePitResponse(false));
                }
            }, e -> {
                logger.debug("Delete PITs failed ", e);
                listener.onResponse(new DeletePitResponse(false));
            }));
        }
    }

    /**
     * Delete all active PIT reader contexts
     */
    void deleteAllPits(ActionListener<DeletePitResponse> listener) {
        int size = clusterService.state().getNodes().getSize();
        ActionListener groupedActionListener = new GroupedActionListener<SearchTransportService.SearchFreeContextResponse>(
            new ActionListener<>() {
                @Override
                public void onResponse(final Collection<SearchTransportService.SearchFreeContextResponse> responses) {
                    // final SetOnce<Boolean> succeeded = new SetOnce<>();
                    boolean hasFailures = responses.stream().anyMatch(r -> !r.isFreed());
                    listener.onResponse(new DeletePitResponse(!hasFailures));
                    // for (SearchTransportService.SearchFreeContextResponse response : responses) {
                    // if (!response.isFreed()) {
                    // succeeded.set(false);
                    // break;
                    // }
                    // }
                    // succeeded.trySet(true);
                    // listener.onResponse(new DeletePitResponse(succeeded.get()));
                }

                @Override
                public void onFailure(final Exception e) {
                    logger.debug("Delete all PITs failed ", e);
                    listener.onResponse(new DeletePitResponse(false));
                }
            },
            size
        );
        for (final DiscoveryNode node : clusterService.state().getNodes()) {
            try {
                Transport.Connection connection = searchTransportService.getConnection(null, node);
                searchTransportService.sendFreeAllPitContexts(connection, groupedActionListener);
            } catch (Exception e) {
                groupedActionListener.onFailure(e);
            }
        }
    }

    /**
     * Delete list of pits, return success if all reader contexts are deleted ( or not found ).
     */
    void deletePits(List<SearchContextIdForNode> contexts, ActionListener<Integer> listener) {
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = getLookupListener(contexts);
        lookupListener.whenComplete(nodeLookup -> {
            final GroupedActionListener<Boolean> groupedListener = new GroupedActionListener<>(
                ActionListener.delegateFailure(
                    listener,
                    (l, result) -> l.onResponse(Math.toIntExact(result.stream().filter(r -> r).count()))
                ),
                contexts.size()
            );

            for (SearchContextIdForNode contextId : contexts) {
                final DiscoveryNode node = nodeLookup.apply(contextId.getClusterAlias(), contextId.getNode());
                if (node == null) {
                    groupedListener.onFailure(new OpenSearchException("node not found"));
                } else {
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(contextId.getClusterAlias(), node);
                        searchTransportService.sendFreePITContext(
                            connection,
                            contextId.getSearchContextId(),
                            ActionListener.wrap(r -> groupedListener.onResponse(r.isFreed()), e -> groupedListener.onResponse(false))
                        );
                    } catch (Exception e) {
                        logger.debug("Delete PIT failed ", e);
                        groupedListener.onResponse(false);
                    }
                }
            }
        }, listener::onFailure);
    }

    private StepListener<BiFunction<String, String, DiscoveryNode>> getLookupListener(List<SearchContextIdForNode> contexts) {
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();
        final Set<String> clusters = contexts.stream()
            .filter(ctx -> Strings.isEmpty(ctx.getClusterAlias()) == false)
            .map(SearchContextIdForNode::getClusterAlias)
            .collect(Collectors.toSet());
        if (clusters.isEmpty() == false) {
            searchTransportService.getRemoteClusterService().collectNodes(clusters, lookupListener);
        } else {
            lookupListener.onResponse((cluster, nodeId) -> clusterService.state().getNodes().get(nodeId));
        }
        return lookupListener;
    }
}
