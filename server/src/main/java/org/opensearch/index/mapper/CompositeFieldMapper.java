///*
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// */
//
//package org.opensearch.index.mapper;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import org.opensearch.cluster.metadata.DataStream;
//import org.opensearch.core.ParseField;
//import org.opensearch.core.common.io.stream.StreamInput;
//import org.opensearch.core.common.io.stream.StreamOutput;
//import org.opensearch.core.common.io.stream.Writeable;
//import org.opensearch.core.xcontent.ConstructingObjectParser;
//import org.opensearch.core.xcontent.ToXContentObject;
//import org.opensearch.core.xcontent.XContentBuilder;
//
//
///**
// * Dims
// * Metrics
// * Timestamp field - Granularities
// */
//
//public class CompositeFieldMapper extends MetadataFieldMapper{
//    public static final String NAME = "_composite_field";
//    public static final String CONTENT_TYPE = "_composite_field";
//
//    // We need timestamp field and also the list of granular columns
//    // associated with the timestamp field.
//    protected CompositeFieldMapper(MappedFieldType mappedFieldType) {
//        super(mappedFieldType);
//    }
//
//    protected CompositeFieldMapper(String timestampField) {
//
//    }
//
//    @Override
//    protected String contentType() {
//        return null;
//    }
//
//
//    private final TimestampField timestampField;
//    public static final class Builder extends MetadataFieldMapper.Builder {
//        final Parameter<Boolean> enabledParam = Parameter.boolParam("enabled", false, mapper -> toType(mapper).enabled, DataStreamFieldMapper.Defaults.ENABLED);
//
//        final Parameter<TimestampField> timestampFieldParam = new Parameter<>(
//            "timestamp_field",
//            false,
//            () -> DataStreamFieldMapper.Defaults.TIMESTAMP_FIELD,
//            (n, c, o) -> new TimestampField((String) ((Map<?, ?>) o).get("name")),
//            mapper -> toType(mapper).timestampField
//        );
//
//        private static CompositeFieldMapper toType(FieldMapper in) {
//            return (CompositeFieldMapper) in;
//        }
//
//        protected Builder() {
//            super(NAME);
//        }
//        @Override
//        protected List<Parameter<?>> getParameters() {
//            return Collections.unmodifiableList(Arrays.asList(enabledParam, timestampFieldParam));
//        }
//
//        @Override
//        public MetadataFieldMapper build(BuilderContext context) {
//            return new CompositeFieldMapper(enabledParam.getValue(), timestampFieldParam.getValue());
//        }
//    }
//
//    /**
//     * Called before {@link FieldMapper#parse(ParseContext)} on the {@link RootObjectMapper}.
//     */
//    public void preParse(ParseContext context) throws IOException {
//        // do nothing
//    }
//
//    @Override
//    public void postParse(ParseContext context) throws IOException {
//        // do nothing
//    }
//
////    @Override
////    protected void parseCreateField(ParseContext context) throws IOException {
////        throw new MapperParsingException(
////            "Field [" + name() + "] is a metadata field and cannot be added inside" + " a document. Use the index API request parameters."
////        );
////    }
//
//    public static final class TimestampField implements Writeable, ToXContentObject {
//
//        static ParseField NAME_FIELD = new ParseField("name");
//
//        @SuppressWarnings("unchecked")
//        public static final ConstructingObjectParser<TimestampField, Void> PARSER = new ConstructingObjectParser<>(
//            "timestamp_field",
//            args -> new TimestampField((String) args[0])
//        );
//
//        static {
//            PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
//        }
//
//        private final String name;
//
//        public TimestampField(String name) {
//            this.name = name;
//        }
//
//        public TimestampField(StreamInput in) throws IOException {
//            this.name = in.readString();
//        }
//
//        @Override
//        public void writeTo(StreamOutput out) throws IOException {
//            out.writeString(name);
//        }
//
//        @Override
//        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
//            builder.startObject();
//            builder.field(NAME_FIELD.getPreferredName(), name);
//            builder.endObject();
//            return builder;
//        }
//
//        public Map<String, Object> toMap() {
//            return Collections.singletonMap(NAME_FIELD.getPreferredName(), name);
//        }
//
//        public String getName() {
//            return name;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            TimestampField that = (TimestampField) o;
//            return name.equals(that.name);
//        }
//
//        @Override
//        public int hashCode() {
//            return Objects.hash(name);
//        }
//    }
//}
