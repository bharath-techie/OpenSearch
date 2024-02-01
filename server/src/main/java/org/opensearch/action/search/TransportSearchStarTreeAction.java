/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;


/**
 * Transport action for creating PIT reader context
 */
public class TransportSearchStarTreeAction extends HandledTransportAction<SearchRequest, SearchStarTreeResponse> {

    public static final String CREATE_PIT_ACTION = "create_pit";
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final CreatePitController createPitController;

    @Inject
    public TransportSearchStarTreeAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry,
        CreatePitController createPitController
    ) {
        super(SearchStarTreeAction.NAME, transportService, actionFilters, in -> new SearchRequest(in));
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.createPitController = createPitController;
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchStarTreeResponse> listener) {
//        final StepListener<SearchResponse> createPitListener = new StepListener<>();
//        SearchTask searchTask = searchRequest.createTask(
//            task.getId(),
//            task.getType(),
//            task.getAction(),
//            task.getParentTaskId(),
//            Collections.emptyMap()
//        );
//        executeSearchStarTree(searchTask, searchRequest, createPitListener);
//        createPitListener.whenComplete(
//            r -> {
//                System.out.println("logs");
//                executeSearchStarTree(searchTask, searchRequest, r, );
//            }, e -> {
//                createPitListener.onFailure(e);
//            }
//        );


        final StepListener<SearchResponse> createPitListener = new StepListener<>();
        final ActionListener<SearchStarTreeResponse> updatePitIdListener = ActionListener.wrap(r -> listener.onResponse(r), e -> {
            logger.error(
                () -> new ParameterizedMessage(
                    "Star tree search failed [{}]",
                    e.getMessage()
                )
            );
            listener.onFailure(e);
        });
      //  createPitController.executeCreatePit(searchRequest, task, createPitListener, updatePitIdListener);


    }

    void executeSearchStarTree(Task task, SearchRequest searchRequest, StepListener<SearchResponse> createPitListener) {
        logger.debug(
            () -> new ParameterizedMessage("Executing creation of PIT context for indices [{}]", Arrays.toString(searchRequest.indices()))
        );
        transportSearchAction.executeRequest(
            task,
            searchRequest,
            TransportCreatePitAction.CREATE_PIT_ACTION,
            true,
            new TransportSearchAction.SinglePhaseSearchAction() {
                @Override
                public void executeOnShardTarget(
                    SearchTask searchTask,
                    SearchShardTarget target,
                    Transport.Connection connection,
                    ActionListener<SearchPhaseResult> searchPhaseResultActionListener
                ) {
                    ShardSearchRequest req = new ShardSearchRequest(target.getShardId(), 1, null);

//                    searchTransportService.sendExecuteStarTreeQuery(
//                        connection,
//                        req,
//                        searchTask,
//                        ActionListener.wrap(r -> searchPhaseResultActionListener.onResponse(r), searchPhaseResultActionListener::onFailure)
//                    );
                }
            },
            createPitListener
        );
    }



}
