/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.index.Fields;
import org.apache.lucene.util.CharsRefBuilder;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

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
