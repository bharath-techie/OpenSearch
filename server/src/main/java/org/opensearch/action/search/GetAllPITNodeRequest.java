/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to get all active PITs in a node
 */
public class GetAllPITNodeRequest extends BaseNodeRequest {
    GetAllPITNodesRequest request;

    @Inject
    public GetAllPITNodeRequest(GetAllPITNodesRequest request) {
        this.request = request;
    }

    public GetAllPITNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new GetAllPITNodesRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
