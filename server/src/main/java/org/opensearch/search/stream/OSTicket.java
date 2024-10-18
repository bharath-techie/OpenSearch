/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream;

import org.opensearch.arrow.StreamTicket;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@ExperimentalApi
public class OSTicket implements Writeable, ToXContentFragment {

    private final StreamTicket streamTicket;

    public OSTicket(String ticketID, String nodeID) {
        this.streamTicket = new StreamTicket(ticketID, nodeID);
    }

    public OSTicket(StreamInput in) throws IOException {
        byte[] bytes = in.readByteArray();
        this.streamTicket = StreamTicket.fromBytes(bytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        byte[] bytes = streamTicket.toBytes();
        return builder.value(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(streamTicket.toBytes());
    }

    @Override
    public String toString() {
        return "OSTicket{" +
            "ticketID='" + streamTicket.getTicketID() + '\'' +
            ", nodeID='" + streamTicket.getNodeID() + '\'' +
            '}';
    }
}
