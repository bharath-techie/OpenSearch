/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class GetAllPITNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private List<String> pitIds;


    @Inject
    public GetAllPITNodeResponse(StreamInput in, List<String> pitIds) throws IOException {
        super(in);
        this.pitIds = pitIds;
    }

    public GetAllPITNodeResponse(DiscoveryNode node, List<String> pitIds) {
        super(node);
        this.pitIds = pitIds;
    }
    public GetAllPITNodeResponse(StreamInput in) throws IOException {
        super(in);
        pitIds = in.readList(StreamInput::readString);
    }

    public List<String> getPitIds() {
        return pitIds;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
