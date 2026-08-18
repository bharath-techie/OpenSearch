/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Pins the materialize action's cross-plugin wire contract: an orchestrator's
 * wire-compatible request class (e.g. index-management's MV refresh client) arrives by
 * reference over local dispatch and must convert losslessly to {@link MaterializeRequest}.
 */
public class MaterializeRequestWireCompatTests extends OpenSearchTestCase {

    public void testLocalRequestPassesThrough() throws IOException {
        MaterializeRequest request = new MaterializeRequest("source=logs | stats count() by status", "target", List.of("status"));
        assertSame(request, TransportMaterializeAction.asMaterializeRequest(request));
    }

    public void testForeignWireCompatibleRequestConverts() throws IOException {
        // Simulates an external plugin's duplicate request class: different type, identical
        // stream format (super + query + targetIndex + keyColumns).
        ActionRequest foreign = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString("source=logs | stats count() as cnt by status");
                out.writeString("logs_by_status-mv-123");
                out.writeStringCollection(List.of("status", "region"));
                out.writeBoolean(true);
            }
        };

        MaterializeRequest converted = TransportMaterializeAction.asMaterializeRequest(foreign);

        assertEquals("source=logs | stats count() as cnt by status", converted.getPplQuery());
        assertEquals("logs_by_status-mv-123", converted.getTargetIndex());
        assertEquals(List.of("status", "region"), converted.getKeyColumns());
        assertNull(converted.validate());
    }

    public void testRequestRoundTrip() throws IOException {
        MaterializeRequest request = new MaterializeRequest("source=x | head 5", "t", List.of());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            MaterializeRequest streamed = new MaterializeRequest(out.bytes().streamInput());
            assertEquals(request.getPplQuery(), streamed.getPplQuery());
            assertEquals(request.getTargetIndex(), streamed.getTargetIndex());
            assertEquals(request.getKeyColumns(), streamed.getKeyColumns());
        }
    }

    public void testResponseRoundTrip() throws IOException {
        MaterializeResponse response = new MaterializeResponse(101, 100, 3, 4567);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            MaterializeResponse streamed = new MaterializeResponse(out.bytes().streamInput());
            assertEquals(101, streamed.getRowsProduced());
            assertEquals(100, streamed.getDocsIndexed());
            assertEquals(3, streamed.getBulkRequests());
            assertEquals(4567, streamed.getTookMillis());
        }
    }
}
