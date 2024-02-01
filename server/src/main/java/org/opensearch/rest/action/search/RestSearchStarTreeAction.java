/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import java.io.IOException;
import java.util.List;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchStarTreeAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.search.builder.SearchSourceBuilder;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.action.RestActions.buildBroadcastShardsHeader;
import static org.opensearch.search.internal.SearchContext.DEFAULT_TERMINATE_AFTER;


public class RestSearchStarTreeAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "search_star_tree_action";
    }
    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/_search/star_tree")));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {


        SearchRequest countRequest = new SearchRequest(Strings.splitStringByCommaToArray(request.param("index")));
        countRequest.indicesOptions(IndicesOptions.fromRequest(request, countRequest.indicesOptions()));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0).trackTotalHits(true);
        countRequest.source(searchSourceBuilder);
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser == null) {
                QueryBuilder queryBuilder = RestActions.urlParamsToQueryBuilder(request);
                if (queryBuilder != null) {
                    searchSourceBuilder.query(queryBuilder);
                }
            } else {
                searchSourceBuilder.query(RestActions.getQueryContent(parser));
            }
        });
        countRequest.routing(request.param("routing"));
        float minScore = request.paramAsFloat("min_score", -1f);
        if (minScore != -1f) {
            searchSourceBuilder.minScore(minScore);
        }

        countRequest.preference(request.param("preference"));

        final int terminateAfter = request.paramAsInt("terminate_after", DEFAULT_TERMINATE_AFTER);
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be > 0");
        } else if (terminateAfter > 0) {
            searchSourceBuilder.terminateAfter(terminateAfter);
        }
        return channel -> client.searchStarTree(countRequest, new RestStatusToXContentListener<>(channel));

//        return channel -> client.searchStarTree(countRequest, new RestBuilderListener<SearchResponse>(channel) {
//            @Override
//            public RestResponse buildResponse(SearchResponse response, XContentBuilder builder) throws Exception {
//                builder.startObject();
//                if (terminateAfter != DEFAULT_TERMINATE_AFTER) {
//                    builder.field("terminated_early", response.isTerminatedEarly());
//                }
//                builder.field("count", response.getHits().getTotalHits().value);
//                buildBroadcastShardsHeader(
//                    builder,
//                    request,
//                    response.getTotalShards(),
//                    response.getSuccessfulShards(),
//                    0,
//                    response.getFailedShards(),
//                    response.getShardFailures()
//                );
//
//                builder.endObject();
//                return new BytesRestResponse(response.status(), builder);
//            }
//        });
    }
}
