/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Result of a materialize request: how many result rows the engine produced, how many
 * documents landed in the target index, and how many bulk requests carried them.
 */
public class MaterializeResponse extends ActionResponse implements ToXContentObject {

    private final long rowsProduced;
    private final long docsIndexed;
    private final long bulkRequests;
    private final long tookMillis;

    public MaterializeResponse(long rowsProduced, long docsIndexed, long bulkRequests, long tookMillis) {
        this.rowsProduced = rowsProduced;
        this.docsIndexed = docsIndexed;
        this.bulkRequests = bulkRequests;
        this.tookMillis = tookMillis;
    }

    public MaterializeResponse(StreamInput in) throws IOException {
        this.rowsProduced = in.readVLong();
        this.docsIndexed = in.readVLong();
        this.bulkRequests = in.readVLong();
        this.tookMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rowsProduced);
        out.writeVLong(docsIndexed);
        out.writeVLong(bulkRequests);
        out.writeVLong(tookMillis);
    }

    public long getRowsProduced() {
        return rowsProduced;
    }

    public long getDocsIndexed() {
        return docsIndexed;
    }

    public long getBulkRequests() {
        return bulkRequests;
    }

    public long getTookMillis() {
        return tookMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("rows_produced", rowsProduced)
            .field("docs_indexed", docsIndexed)
            .field("bulk_requests", bulkRequests)
            .field("took_millis", tookMillis)
            .endObject();
    }
}
