/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segments;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.action.search.GetAllPitsAction;
import org.opensearch.action.search.ListPitInfo;
import org.opensearch.action.search.SearchContextId;
import org.opensearch.action.search.SearchContextIdForNode;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.action.search.SearchContextId.decode;

public class TransportPitSegmentsAction extends TransportBroadcastByNodeAction<PitSegmentsRequest, IndicesSegmentResponse, ShardSegments> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final SearchService searchService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportService transportService;


    @Inject
    public TransportPitSegmentsAction(ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      SearchService searchService,
                                      NamedWriteableRegistry namedWriteableRegistry) {
        super(PitSegmentsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            PitSegmentsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transportService = transportService;

    }

    @Override
    protected void doExecute(Task task, PitSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener) {
        //TODO : Refactor to use a util method to "get all PITs" once such a method is implemented.
        if (request.getPitIds().isEmpty()) {
            final List<DiscoveryNode> nodes = new LinkedList<>();
            for (ObjectCursor<DiscoveryNode> cursor : clusterService.state().nodes().getDataNodes().values()) {
                DiscoveryNode node = cursor.value;
                nodes.add(node);
            }
            DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
            nodes.toArray(disNodesArr);
            transportService.sendRequest(transportService.getLocalNode(), GetAllPitsAction.NAME,
                new GetAllPitNodesRequest(disNodesArr), new TransportResponseHandler<GetAllPitNodesResponse>() {
                    @Override
                    public void handleResponse(GetAllPitNodesResponse response) {
                        request.setPitIds(response.getPITIDs().stream().map(ListPitInfo::getPitId).collect(Collectors.toList()));
                        getDoExecute(task, request, listener);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public GetAllPitNodesResponse read(StreamInput in) throws IOException {
                        return new GetAllPitNodesResponse(in);
                    }
                });
        } else {
            getDoExecute(task, request, listener);
        }
    }

    private void getDoExecute(Task task, PitSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, PitSegmentsRequest request, String[] concreteIndices) {
        final ArrayList<ShardRouting> iterators = new ArrayList<>();
        for (String pitId : request.getPitIds()) {
            SearchContextId searchContext = decode(namedWriteableRegistry, pitId);
            for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
                final SearchContextIdForNode perNode = entry.getValue();
                if (Strings.isEmpty(perNode.getClusterAlias())) {
                    final ShardId shardId = entry.getKey();
                    iterators.add(new PitAwareShardRouting(pitId, shardId, perNode.getNode(), null, true, ShardRoutingState.STARTED,
                        null, null, null, -1L));
                }
            }
        }
        return new PlainShardsIterator(iterators);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PitSegmentsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PitSegmentsRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected Writeable.Reader<TransportBroadcastByNodeAction<PitSegmentsRequest, IndicesSegmentResponse, ShardSegments>.NodeRequest> getNodeRequestWriteable() {
        return NodeRequest::new;
    }

    @Override
    protected ShardSegments readShardResult(StreamInput in) throws IOException {
        return new ShardSegments(in);
    }

    @Override
    protected IndicesSegmentResponse newResponse(PitSegmentsRequest request, int totalShards, int successfulShards, int failedShards,
                                                 List<ShardSegments> results, List<DefaultShardOperationFailedException> shardFailures,
                                                 ClusterState clusterState) {
        return new IndicesSegmentResponse(results.toArray(new ShardSegments[results.size()]), totalShards, successfulShards, failedShards,
            shardFailures);
    }

    @Override
    protected PitSegmentsRequest readRequestFrom(StreamInput in) throws IOException {
        return new PitSegmentsRequest(in);
    }

    @Override
    protected ShardSegments shardOperation(PitSegmentsRequest request, ShardRouting shardRouting) {
        PitAwareShardRouting pitAwareShardRouting = (PitAwareShardRouting) shardRouting;
        SearchContextIdForNode searchContextIdForNode = decode(namedWriteableRegistry,
            pitAwareShardRouting.getPitId()).shards().get(shardRouting.shardId());
        PitReaderContext pitReaderContext = searchService.getPitReaderContext(searchContextIdForNode.getSearchContextId());
        return new ShardSegments(pitReaderContext.getShardRouting(), pitReaderContext.getSegments());
    }

    public class PitAwareShardRouting extends ShardRouting {

        private final String pitId;

        public PitAwareShardRouting(StreamInput in) throws IOException {
            super(in);
            this.pitId = in.readString();
        }

        public PitAwareShardRouting(
            String pitId,
            ShardId shardId,
            String currentNodeId,
            String relocatingNodeId,
            boolean primary,
            ShardRoutingState state,
            RecoverySource recoverySource,
            UnassignedInfo unassignedInfo,
            AllocationId allocationId,
            long expectedShardSize
        ) {
            super(shardId, currentNodeId, relocatingNodeId, primary, state, recoverySource, unassignedInfo,
                allocationId, expectedShardSize);
            this.pitId = pitId;
        }

        public String getPitId() {
            return pitId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(pitId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            super.toXContent(builder, params);
            builder.field("pitId", pitId);
            return builder.endObject();
        }
    }


}
