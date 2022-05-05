/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.search.CreatePITAction;
import org.opensearch.action.search.CreatePITRequest;
import org.opensearch.action.search.CreatePITResponse;
import org.opensearch.action.search.GetAllPITNodesRequest;
import org.opensearch.action.search.GetAllPITNodesResponse;
import org.opensearch.action.search.GetAllPITsAction;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class PitMultiNodeTests extends OpenSearchIntegTestCase {

    @Before
    public void setupIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).execute().get();
        ensureGreen();
    }

    @After
    public void clearIndex() {
        client().admin().indices().prepareDelete("index").get();
    }

    public void testPit() throws Exception {
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertEquals(2, searchResponse.getSuccessfulShards());
        assertEquals(2, searchResponse.getTotalShards());
    }

    public void testGetAllPits() throws Exception {
        client().admin().indices().prepareCreate("index1").get();
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        CreatePITResponse pitResponse1 = client().execute(CreatePITAction.INSTANCE, request).get();
        CreatePITResponse pitResponse2 = client().execute(CreatePITAction.INSTANCE, request).get();
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>();
        for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().getDataNodes().values()) {
            DiscoveryNode node = cursor.value;
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        GetAllPITNodesRequest getAllPITNodesRequest = new GetAllPITNodesRequest(disNodesArr);
        ActionFuture<GetAllPITNodesResponse> execute1 = client().execute(GetAllPITsAction.INSTANCE, getAllPITNodesRequest);
        GetAllPITNodesResponse getPitResponse = execute1.get();
        assertEquals(3, getPitResponse.getPITIDs().size());
        List<String> resultPitIds = getPitResponse.getPITIDs().stream().map(p -> p.getPitId()).collect(Collectors.toList());
        Assert.assertTrue(resultPitIds.contains(pitResponse.getId()));
        Assert.assertTrue(resultPitIds.contains(pitResponse1.getId()));
        Assert.assertTrue(resultPitIds.contains(pitResponse2.getId()));
    }

    public void testCreatePitWhileNodeDropWithAllowPartialCreationFalse() throws Exception {
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), false);
        request.setIndices(new String[] { "index" });
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
                ExecutionException ex = expectThrows(ExecutionException.class, execute::get);
                assertTrue(ex.getMessage().contains("Failed to execute phase [create_pit]"));
                assertTrue(ex.getMessage().contains("Partial shards failure"));
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testCreatePitWhileNodeDropWithAllowPartialCreationTrue() throws Exception {
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
                CreatePITResponse pitResponse = execute.get();
                assertEquals(1, pitResponse.getSuccessfulShards());
                assertEquals(2, pitResponse.getTotalShards());
                SearchResponse searchResponse = client().prepareSearch("index")
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .get();
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals(1, searchResponse.getTotalShards());
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitSearchWithNodeDrop() throws Exception {
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                SearchResponse searchResponse = client().prepareSearch()
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .get();
                assertEquals(1, searchResponse.getSuccessfulShards());
                assertEquals(1, searchResponse.getFailedShards());
                assertEquals(0, searchResponse.getSkippedShards());
                assertEquals(2, searchResponse.getTotalShards());
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitSearchWithNodeDropWithPartialSearchResultsFalse() throws Exception {
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                ActionFuture<SearchResponse> execute = client().prepareSearch()
                    .setSize(2)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .setAllowPartialSearchResults(false)
                    .execute();
                ExecutionException ex = expectThrows(ExecutionException.class, execute::get);
                assertTrue(ex.getMessage().contains("Partial shards failure"));
                return super.onNodeStopped(nodeName);
            }
        });
    }

    public void testPitInvalidDefaultKeepAlive() {
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("pit.max_keep_alive", "1m").put("search.default_keep_alive", "2m"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (2m > 1m)"));

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "5m").put("pit.max_keep_alive", "5m"))
                .get()
        );

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "2m"))
                .get()
        );

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("pit.max_keep_alive", "2m"))
                .get()
        );

        exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "3m"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (3m > 2m)"));

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("search.default_keep_alive", "1m"))
                .get()
        );

        exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("pit.max_keep_alive", "30s"))
                .get()
        );
        assertThat(exc.getMessage(), containsString("was (1m > 30s)"));

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );

    }

}
