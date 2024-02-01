/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.action.RestActions;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.ShardSearchContextId;


public class SearchStarTreeResponse extends ActionResponse implements StatusToXContentObject {

    private Map<String, Long> resultMap = new HashMap<>();
    private final int totalShards;
    private final int successfulShards;
    private final int failedShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;

    public SearchStarTreeResponse(StreamInput in) throws IOException {
        resultMap = in.readMap(StreamInput::readString, StreamInput::readLong);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        skippedShards = in.readVInt();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = ShardSearchFailure.readShardSearchFailure(in);
            }
        }
    }

    public SearchStarTreeResponse(Map<String, Long> map, ShardSearchContextId shardSearchContextId) throws IOException {
        resultMap = map;
        totalShards = 0;
        successfulShards = 0;
        failedShards = 0;
        skippedShards = 0;
        int size = 0;
        shardFailures = ShardSearchFailure.EMPTY_ARRAY;
    }
    @Override
    public void writeTo(StreamOutput out)
        throws IOException {
        out.writeMap(resultMap, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(failedShards);
        out.writeVInt(skippedShards);
        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }
    }

    public Map<String, Long> getResultMap() {
        return resultMap;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
        throws IOException {
        builder.startObject();
        builder.field("result", resultMap);
        RestActions.buildBroadcastShardsHeader(
            builder,
            params,
            getTotalShards(),
            getSuccessfulShards(),
            getSkippedShards(),
            getFailedShards(),
            getShardFailures()
        );
        builder.endObject();
        return builder;
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }

    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the create pit operation was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    public int getSkippedShards() {
        return skippedShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }
}
