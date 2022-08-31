/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This holds information about pit reader context such as pit id and creation time
 */
public class ListPitInfo implements ToXContentFragment, Writeable {
    private final String pitId;
    private final long creationTime;
    private final long keepAlive;

    public ListPitInfo(String pitId, long creationTime, long keepAlive) {
        this.pitId = pitId;
        this.creationTime = creationTime;
        this.keepAlive = keepAlive;
    }

    public ListPitInfo(StreamInput in) throws IOException {
        this.pitId = in.readString();
        this.creationTime = in.readLong();
        this.keepAlive = in.readLong();
    }

    static final ConstructingObjectParser<ListPitInfo, Void> PARSER = new ConstructingObjectParser<>(
        "list_pit_info",
        true,
        args -> new ListPitInfo((String) args[0], (long) args[1], (long) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("pit_id"));
        PARSER.declareString(constructorArg(), new ParseField("creation_time"));
        PARSER.declareString(constructorArg(), new ParseField("keep_alive"));
    }

    private static final ParseField CREATION_TIME = new ParseField("creation_time");
    private static final ParseField PIT_ID = new ParseField("pit_id");
    private static final ParseField KEEP_ALIVE = new ParseField("keep_alive");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PIT_ID.getPreferredName(), pitId);
        builder.field(CREATION_TIME.getPreferredName(), creationTime);
        builder.field(KEEP_ALIVE.getPreferredName(), keepAlive);
        builder.endObject();
        return builder;
    }

    public String getPitId() {
        return pitId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeLong(creationTime);
        out.writeLong(keepAlive);
    }
}
