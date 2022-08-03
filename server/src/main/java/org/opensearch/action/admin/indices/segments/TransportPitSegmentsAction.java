/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.ListPitInfo;
import org.opensearch.action.search.PitService;
import org.opensearch.action.search.SearchContextId;
import org.opensearch.action.search.SearchContextIdForNode;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.action.search.SearchContextId.decode;

public class TransportPitSegmentsAction extends TransportBroadcastByNodeAction<PitSegmentsRequest, IndicesSegmentResponse, ShardSegments> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final SearchService searchService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportService transportService;
    private final PitService pitService;

    @Inject
    public TransportPitSegmentsAction(ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      SearchService searchService,
                                      NamedWriteableRegistry namedWriteableRegistry, PitService pitService) {
        super(PitSegmentsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                PitSegmentsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transportService = transportService;
        this.pitService = pitService;
    }

    @Override
    protected void doExecute(Task task, PitSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener) {
        if (request.getPitIds().isEmpty()) {
            pitService.getAllPits(ActionListener.wrap(response -> {
                request.setPitIds(response.getPitInfos().stream().map(ListPitInfo::getPitId).collect(Collectors.toList()));
                getDoExecute(task, request, listener);
            }, listener::onFailure));
        } else {
            getDoExecute(task, request, listener);
        }
    }

    private void getDoExecute(Task task, PitSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener) {
        super.doExecute(task, request, listener);
    }

    /**
     * This adds list of shards on which we need to retrieve pit segments details
     * @param clusterState    the cluster state
     * @param request         the underlying request
     * @param concreteIndices the concrete indices on which to execute the operation
     * @return
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, PitSegmentsRequest request, String[] concreteIndices) {
        final ArrayList<ShardRouting> iterators = new ArrayList<>();
        ShardsIterator shardsIterator = clusterState.routingTable().allShards(concreteIndices);
        for (String pitId : request.getPitIds()) {
            SearchContextId searchContext = decode(namedWriteableRegistry, pitId);
            for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
                final SearchContextIdForNode perNode = entry.getValue();
                if (Strings.isEmpty(perNode.getClusterAlias())) {
                    final ShardId shardId = entry.getKey();
                    Optional<ShardRouting> shardRoutingOptional = shardsIterator.getShardRoutings().stream().filter(r -> r.shardId().equals(shardId)).findFirst();
                    ShardRouting shardRouting = shardRoutingOptional.get();
                    PitAwareShardRouting pitAwareShardRouting = new PitAwareShardRouting(shardRouting, pitId);
                    iterators.add(pitAwareShardRouting);
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
    public List<ShardRouting> getShardsFromInputStream(StreamInput in) throws IOException {
        return in.readList(PitAwareShardRouting::new);
    }

    /**
     * This retrieves segment details of PIT context
     * @param request      the node-level request
     * @param shardRouting the shard on which to execute the operation
     * @return
     */
    @Override
    protected ShardSegments shardOperation(PitSegmentsRequest request, ShardRouting shardRouting) {
        PitAwareShardRouting pitAwareShardRouting = (PitAwareShardRouting) shardRouting;
        SearchContextIdForNode searchContextIdForNode = decode(namedWriteableRegistry,
                pitAwareShardRouting.getPitId()).shards().get(shardRouting.shardId());
        PitReaderContext pitReaderContext = searchService.getPitReaderContext(searchContextIdForNode.getSearchContextId());
        return new ShardSegments(pitReaderContext.getShardRouting(), pitReaderContext.getSegments());
    }

    /**
     * This holds PIT id which is later used to perform broadcast operation in PIT shards to retrieve segments
     */
    public class PitAwareShardRouting extends ShardRouting {

        private final String pitId;

        public PitAwareShardRouting(StreamInput in) throws IOException {
            super(in);
            this.pitId = in.readString();
        }

        public PitAwareShardRouting(
                ShardRouting shardRouting,
                String pitId
        ) {
            super(shardRouting.shardId(), shardRouting.currentNodeId(), shardRouting.relocatingNodeId(),
                    shardRouting.primary(), shardRouting.state(), shardRouting.recoverySource(),
                    shardRouting.unassignedInfo(),
                    shardRouting.allocationId(), shardRouting.getExpectedShardSize());
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