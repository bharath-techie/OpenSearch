/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request to delete one or more PIT contexts based on IDs.
 */
public class DeletePITRequest extends ActionRequest implements ToXContentObject {

    private List<String> pitIds;

    public DeletePITRequest(StreamInput in) throws IOException {
        super(in);
        pitIds = Arrays.asList(in.readStringArray());
    }

    public DeletePITRequest(String... pitIds) {
        if (pitIds != null) {
            this.pitIds = Arrays.asList(pitIds);
        }
    }

    public DeletePITRequest(List<String> pitIds) {
        if (pitIds != null) {
            this.pitIds = pitIds;
        }
    }

    public DeletePITRequest() {}

    public List<String> getPitIds() {
        return pitIds;
    }

    public void setPitIds(List<String> pitIds) {
        this.pitIds = pitIds;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pitIds == null || pitIds.isEmpty()) {
            validationException = addValidationError("no pit ids specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (pitIds == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(pitIds.toArray(new String[pitIds.size()]));
        }
    }

    public void addPitId(String pitId) {
        if (pitIds == null) {
            pitIds = new ArrayList<>();
        }
        pitIds.add(pitId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("pit_id");
        for (String pitId : pitIds) {
            builder.value(pitId);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        pitIds = null;
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("pit_id".equals(currentFieldName)) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException("pit_id array element should only contain pit_id");
                            }
                            addPitId(parser.text());
                        }
                    } else {
                        if (token.isValue() == false) {
                            throw new IllegalArgumentException("pit_id element should only contain pit_id");
                        }
                        addPitId(parser.text());
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Unknown parameter [" + currentFieldName + "] in request body or parameter is of the wrong type[" + token + "] "
                    );
                }
            }
        }
    }

}
