/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.opensearch.index.mapper;

import org.apache.lucene.search.Query;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.compositeindex.CompositeField;
import org.opensearch.index.compositeindex.CompositeFieldSpec;
import org.opensearch.index.compositeindex.CompositeIndexConfig;
import org.opensearch.index.compositeindex.DateDimension;
import org.opensearch.index.compositeindex.Dimension;
import org.opensearch.index.compositeindex.Metric;
import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.index.compositeindex.StarTreeFieldSpec;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StarTreeMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "startree";
    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), objBuilder).init(this);

    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private ObjectMapper.Builder objbuilder;

        @SuppressWarnings("unchecked")
        private final Parameter<CompositeField> config = new Parameter<>("config", false, () -> null, (n, c, o) -> {
            int maxLeafDocs = 10000;
            if(o instanceof Map<?, ?>) {
                maxLeafDocs = XContentMapValues.nodeIntegerValue(((Map<String, Object>) o).get("max_leaf_docs"));
            }
            List<Dimension> dimensions = buildDimensions(o, c);
            List<Metric> metrics = buildMetrics(o, c);

            CompositeFieldSpec spec = new StarTreeFieldSpec(maxLeafDocs, new ArrayList<>(), StarTreeFieldSpec.StarTreeBuildMode.OFF_HEAP);
            return new CompositeField(this.name, dimensions, metrics, spec);
        }, m -> toType(m).compositeField);

        private List<Dimension> buildDimensions(Object o, Mapper.TypeParser.ParserContext c) {
            Object dims = XContentMapValues.extractValue("ordered_dimensions", (Map<?, ?>) o);
            if (dims == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "ordered_dimensions is required for star tree field [%s]", this.name)
                );
            }
            List<Dimension> dimensions = new ArrayList<>();
            if (dims instanceof LinkedHashMap<?, ?>) {
                if (((LinkedHashMap<?, ?>) dims).size() > c.getSettings()
                    .getAsInt(CompositeIndexConfig.COMPOSITE_INDEX_MAX_DIMENSIONS_SETTING.getKey(), 10)) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "ordered_dimensions cannot have more than %s dimensions for star tree field [%s]",
                            c.getSettings().getAsInt(CompositeIndexConfig.COMPOSITE_INDEX_MAX_DIMENSIONS_SETTING.getKey(), 10),
                            this.name
                        )
                    );
                }
                for (Map.Entry<String, Object> dim : ((LinkedHashMap<String, Object>) dims).entrySet()) {
                    if(this.objbuilder == null || this.objbuilder.mappersBuilders == null) {
                        if (dim.getValue() instanceof Map<?, ?>) {
                            Map<String, Object> dimObj = ((Map<String, Object>) dim.getValue());
                            String type = XContentMapValues.nodeStringValue(dimObj.get("type"));
                            dimensions.add(getDimension(type, dim, c));
                        } else {
                            throw new MapperParsingException(
                                String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", this.name)
                            );
                        }
                    } else {
                        Optional<Mapper.Builder> dimType = findMapperBuilderByName(dim.getKey(), this.objbuilder.mappersBuilders);
                        if (dimType.isEmpty()) {
                            throw new IllegalArgumentException("no such field" + dim.getKey());
                        }
                        dimensions.add(getDimension(dimType.get(), dim, c));
                    }
                }
            } else if(dims instanceof List<?>) {

            }
            else {
                throw new MapperParsingException(
                    String.format(Locale.ROOT, "unable to parse ordered_dimensions for star tree field [%s]", this.name)
                );
            }
            return dimensions;
        }

        private Dimension getDimension(Mapper.Builder builder, Map.Entry<String, Object> dim, Mapper.TypeParser.ParserContext c) {
            String name = dim.getKey();
            Dimension dimension;
            if (builder instanceof DateFieldMapper.Builder) {
                dimension = new DateDimension(dim, c);
            }
            // Numeric dimension - default
            else {
                dimension = new Dimension(name);
            }
            return dimension;
        }

        private Dimension getDimension(String type, Map.Entry<String, Object> dim, Mapper.TypeParser.ParserContext c) {
            String name = dim.getKey();
            Dimension dimension;
            if (type.equals("date")) {
                dimension = new DateDimension(dim, c);
            }
            // Numeric dimension - default
            else {
                dimension = new Dimension(name);
            }
            return dimension;
        }

        @SuppressWarnings("unchecked")
        private List<Metric> buildMetrics(Object o, Mapper.TypeParser.ParserContext c) {
            List<Metric> metrics = new ArrayList<>();
            Object metricsFromInput = XContentMapValues.extractValue("metrics", (Map<?, ?>) o);
            if (metricsFromInput == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "metrics is required for star tree field [%s]", this.name));
            }
            if (metricsFromInput instanceof LinkedHashMap<?, ?>) {
                for (Map.Entry<String, Object> metric : ((LinkedHashMap<String, Object>) metricsFromInput).entrySet()) {
                    if(objbuilder == null || objbuilder.mappersBuilders == null) {
                        metrics.add(getMetric(metric, c));
                    } else {
                        Optional<Mapper.Builder> metricType = findMapperBuilderByName(metric.getKey(), this.objbuilder.mappersBuilders);
                        if (metricType.isEmpty()) {
                            throw new IllegalArgumentException("no such field" + metric.getKey());
                        }
                        metrics.add(getMetric(metricType.get(), metric, c));
                    }
                }
            } else {
                throw new MapperParsingException(String.format(Locale.ROOT, "unable to parse metrics for star tree field [%s]", this.name));
            }

            return metrics;
        }

        @SuppressWarnings("unchecked")
        private Metric getMetric(Mapper.Builder builder, Map.Entry<String, Object> m, Mapper.TypeParser.ParserContext c) {
            String name = m.getKey();
            Metric metric;
            List<MetricType> metricTypes = new ArrayList<>();
            if (builder instanceof NumberFieldMapper.Builder) {
                List<String> metricStrings = XContentMapValues.extractRawValues("metrics", (Map<String, Object>) m.getValue())
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());

                if (metricStrings.isEmpty()) {
                    metricTypes = new ArrayList<>(CompositeIndexConfig.DEFAULT_METRICS_LIST.get(c.getSettings()));
                } else {
                    for (String metricString : metricStrings) {
                        metricTypes.add(MetricType.fromTypeName(metricString));
                    }
                }
            } else {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "metrics specified for non numeric fields in composite field [%s]",
                    this.name));
            }
            return new Metric(name, metricTypes);
        }

        private Metric getMetric(Map.Entry<String, Object> m, Mapper.TypeParser.ParserContext c) {
            String name = m.getKey();
            Metric metric;
            List<MetricType> metricTypes = new ArrayList<>();
            List<String> metricStrings = XContentMapValues.extractRawValues("metrics", (Map<String, Object>) m.getValue())
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());

            if (metricStrings.isEmpty()) {
                metricTypes = new ArrayList<>(CompositeIndexConfig.DEFAULT_METRICS_LIST.get(c.getSettings()));
            } else {
                for (String metricString : metricStrings) {
                    metricTypes.add(MetricType.fromTypeName(metricString));
                }
            }
            return new Metric(name, metricTypes);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            // return List.of(indexMode, dimensionsOrder, metrics, startreeSpec, dimensionsConfig);
            return List.of(config);
        }

        private Optional<Mapper.Builder> findMapperBuilderByName(String field, List<Mapper.Builder> mappersBuilders) {
            return mappersBuilders.stream().filter(builder -> builder.name().equals(field)).findFirst();
        }

        public Builder(String name, ObjectMapper.Builder objBuilder) {
            super(name);
            this.objbuilder = objBuilder;
        }

        @Override
        public ParametrizedFieldMapper build(BuilderContext context) {
            System.out.println("StarTreeMapper build");
            System.out.println(context.path());
            StarTreeFieldType type = new StarTreeFieldType(name, this.config.get());
            return new StarTreeMapper(name, type, this, objbuilder);
        }
    }

    private static StarTreeMapper toType(FieldMapper in) {
        return (StarTreeMapper) in;
    }

    public static class TypeParser implements Mapper.TypeParser {

        /**
         * default constructor of VectorFieldMapper.TypeParser
         */
        public TypeParser() {}

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context) throws MapperParsingException {
            // we need builder to be passed to
            Builder builder = new StarTreeMapper.Builder(name, null);
            builder.parse(name, context, node);
            return builder;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext context, ObjectMapper.Builder objBuilder)
            throws MapperParsingException {
            Builder builder = new StarTreeMapper.Builder(name, objBuilder);
            builder.parse(name, context, node);
            return builder;
        }
    }
    private final CompositeField compositeField;

    private final ObjectMapper.Builder objBuilder;
    protected StarTreeMapper(String simpleName, StarTreeFieldType type, Builder builder,
        ObjectMapper.Builder objbuilder) {
        super(simpleName, type, MultiFields.empty(), CopyTo.empty());
        this.compositeField = builder.config.get();
        this.objBuilder = objbuilder;
    }

    @Override
    public StarTreeFieldType fieldType() {
        return (StarTreeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new MapperParsingException(
            "Field [" + name() + "] is a startree field and cannot be added inside" + " a document. Use the index API request parameters."
        );
    }

    /**
     *
     */
    public static final class StarTreeFieldType extends CompositeAggregateFieldType {

        private final CompositeField compositeField;
        public StarTreeFieldType(String name, CompositeField compositeField) {
            super(name, compositeField.getDimensionsOrder(), compositeField.getMetrics());
            this.compositeField = compositeField;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            // TODO : evaluate later
            throw new UnsupportedOperationException("Cannot fetch values for star tree field [" + name() + "].");
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            // TODO : evaluate later
            throw new UnsupportedOperationException("Cannot perform terms query on star tree field [" + name() + "].");
        }
    }

}
