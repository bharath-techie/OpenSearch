/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.action.support.PlainActionFuture.newFuture;

/**
 * Functional tests for transport delete pit action
 */
public class TransportDeletePITActionTests extends OpenSearchTestCase {

    DiscoveryNode node1 = null;
    DiscoveryNode node2 = null;
    DiscoveryNode node3 = null;
    String pitId = null;
    TransportSearchAction transportSearchAction = null;
    Task task = null;
    DiscoveryNodes nodes = null;
    NamedWriteableRegistry namedWriteableRegistry = null;
    ClusterService clusterServiceMock = null;

    @Before
    public void setupData() {
        node1 = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        node2 = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        node3 = new DiscoveryNode("node_3", buildNewFakeTransportAddress(), Version.CURRENT);
        pitId = CreatePitControllerTests.getPitId();
        namedWriteableRegistry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new)
            )
        );
        nodes = DiscoveryNodes.builder().add(node1).add(node2).add(node3).build();
        transportSearchAction = mock(TransportSearchAction.class);
        task = new Task(
            randomLong(),
            "transport",
            SearchAction.NAME,
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
        InternalSearchResponse response = new InternalSearchResponse(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            1
        );

        clusterServiceMock = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);

        final Settings keepAliveSettings = Settings.builder()
            .put(CreatePITController.CREATE_PIT_TEMPORARY_KEEPALIVE_SETTING.getKey(), 30000)
            .build();
        when(clusterServiceMock.getSettings()).thenReturn(keepAliveSettings);

        when(state.getMetadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(state.metadata()).thenReturn(Metadata.EMPTY_METADATA);
        when(clusterServiceMock.state()).thenReturn(state);
        when(state.getNodes()).thenReturn(nodes);
    }

    /**
     * Test if transport call for update pit is made to all nodes present as part of PIT ID returned from phase one of create pit
     */
    public void testDeletePitSuccess() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {

                @Override
                public void sendFreePITContext(
                    Transport.Connection connection,
                    ShardSearchContextId contextId,
                    ActionListener<SearchFreeContextResponse> listener
                ) {
                    deleteNodesInvoked.add(connection.getNode());
                    Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                    t.start();
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };

            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest(pitId);
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(true, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDeleteAllPITSuccess() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {
                public void sendFreeAllPitContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
                    deleteNodesInvoked.add(connection.getNode());
                    Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                    t.start();
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };

            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest("_all");
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(true, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDeletePitWhenNodeIsDown() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {

                @Override
                public void sendFreePITContext(
                    Transport.Connection connection,
                    ShardSearchContextId contextId,
                    ActionListener<SearchFreeContextResponse> listener
                ) {
                    deleteNodesInvoked.add(connection.getNode());

                    if (connection.getNode().getId() == "node_3") {
                        Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 down")));
                        t.start();
                    } else {
                        Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                        t.start();
                    }
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest(pitId);
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(false, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDeletePitWhenAllNodesAreDown() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {

                @Override
                public void sendFreePITContext(
                    Transport.Connection connection,
                    ShardSearchContextId contextId,
                    ActionListener<SearchFreeContextResponse> listener
                ) {
                    deleteNodesInvoked.add(connection.getNode());
                    Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 down")));
                    t.start();
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest(pitId);
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(false, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDeletePitFailure() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {

                @Override
                public void sendFreePITContext(
                    Transport.Connection connection,
                    ShardSearchContextId contextId,
                    ActionListener<SearchFreeContextResponse> listener
                ) {
                    deleteNodesInvoked.add(connection.getNode());

                    if (connection.getNode().getId() == "node_3") {
                        Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(false)));
                        t.start();
                    } else {
                        Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                        t.start();
                    }
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest(pitId);
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(false, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDeleteAllPitWhenNodeIsDown() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {
                @Override
                public void sendFreeAllPitContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
                    deleteNodesInvoked.add(connection.getNode());
                    if (connection.getNode().getId() == "node_3") {
                        Thread t = new Thread(() -> listener.onFailure(new Exception("node 3 down")));
                        t.start();
                    } else {
                        Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                        t.start();
                    }
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest("_all");
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(false, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDeleteAllPitWhenAllNodesAreDown() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {

                @Override
                public void sendFreeAllPitContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
                    deleteNodesInvoked.add(connection.getNode());
                    Thread t = new Thread(() -> listener.onFailure(new Exception("node down")));
                    t.start();
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest("_all");
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(false, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

    public void testDeleteAllPitFailure() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> deleteNodesInvoked = new CopyOnWriteArrayList<>();
        Settings settings = Settings.builder().put("node.name", TransportMultiSearchActionTests.class.getSimpleName()).build();
        ActionFilters actionFilters = mock(ActionFilters.class);
        when(actionFilters.filters()).thenReturn(new ActionFilter[0]);
        ThreadPool threadPool = new ThreadPool(settings);
        try {
            SearchTransportService searchTransportService = new SearchTransportService(null, null) {

                public void sendFreeAllPitContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
                    deleteNodesInvoked.add(connection.getNode());
                    if (connection.getNode().getId() == "node_3") {
                        Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(false)));
                        t.start();
                    } else {
                        Thread t = new Thread(() -> listener.onResponse(new SearchFreeContextResponse(true)));
                        t.start();
                    }
                }

                @Override
                public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }
            };
            TransportService transportService = new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
                null,
                Collections.emptySet()
            ) {
                @Override
                public TaskManager getTaskManager() {
                    return taskManager;
                }
            };
            TransportDeletePITAction action = new TransportDeletePITAction(
                transportService,
                actionFilters,
                namedWriteableRegistry,
                transportSearchAction,
                clusterServiceMock,
                searchTransportService
            );
            DeletePITRequest deletePITRequest = new DeletePITRequest("_all");
            PlainActionFuture<DeletePITResponse> future = newFuture();
            action.execute(task, deletePITRequest, future);
            DeletePITResponse dr = future.get();
            assertEquals(false, dr.isSucceeded());
            assertEquals(3, deleteNodesInvoked.size());
        } finally {
            assertTrue(OpenSearchTestCase.terminate(threadPool));
        }
    }

}
