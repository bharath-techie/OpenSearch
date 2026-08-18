/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * REST entry for materializing a PPL query into a target index.
 *
 * <pre>
 * POST /_plugins/_ppl/_materialize
 * {
 *   "query": "source=logs | stats count() by status",
 *   "target_index": "logs_by_status",
 *   "key_columns": ["status"]
 * }
 * </pre>
 */
public class RestMaterializeAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "ppl_materialize_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_plugins/_ppl/_materialize"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String query = null;
        String targetIndex = null;
        List<String> keyColumns = new ArrayList<>();
        try (XContentParser parser = request.contentParser()) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected a JSON object body");
            }
            String fieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if ("query".equals(fieldName) && token.isValue()) {
                    query = parser.text();
                } else if ("target_index".equals(fieldName) && token.isValue()) {
                    targetIndex = parser.text();
                } else if ("key_columns".equals(fieldName) && token == XContentParser.Token.START_ARRAY) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        keyColumns.add(parser.text());
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }
        MaterializeRequest materializeRequest = new MaterializeRequest(query, targetIndex, keyColumns);
        return channel -> client.execute(MaterializeAction.INSTANCE, materializeRequest, new RestToXContentListener<>(channel));
    }
}
