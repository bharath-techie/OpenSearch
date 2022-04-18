/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.DeletePITRequest;
import org.opensearch.action.search.DeletePITResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.DELETE;

public class RestDeletePITAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_pit_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String pitIds = request.param("pit_id");
        DeletePITRequest deletePITRequest = new DeletePITRequest();
        deletePITRequest.setPitIds(asList(Strings.splitStringByCommaToArray(pitIds)));
        request.withContentOrSourceParamParserOrNull((xContentParser -> {
            if (xContentParser != null) {
                // NOTE: if rest request with xcontent body has request parameters, values parsed from request body have the precedence
                try {
                    deletePITRequest.fromXContent(xContentParser);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to parse request body", e);
                }
            }
        }));
        return channel -> client.deletePit(deletePITRequest, new RestStatusToXContentListener<DeletePITResponse>(channel));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(DELETE, "/_search/_point_in_time"), new Route(DELETE, "/_search/_point_in_time/{id}")));
    }
}
