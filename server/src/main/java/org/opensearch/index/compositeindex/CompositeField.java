/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import java.io.IOException;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;


/**
 * Composite field which contains dimensions, metrics and index mode specific specs
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeField implements ToXContent {
    private final String name;
    private final List<Dimension> dimensionsOrder;
    private final List<Metric> metrics;
    private final CompositeFieldSpec compositeFieldSpec;

    public CompositeField(String name, List<Dimension> dimensions, List<Metric> metrics, CompositeFieldSpec compositeFieldSpec) {
        this.name = name;
        this.dimensionsOrder = dimensions;
        this.metrics = metrics;
        this.compositeFieldSpec = compositeFieldSpec;
    }

    public String getName() {
        return name;
    }

    public List<Dimension> getDimensionsOrder() {
        return dimensionsOrder;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public CompositeFieldSpec getSpec() {
        return compositeFieldSpec;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
        throws IOException {
        builder.startObject();
        builder.field("name", name);
        if (dimensionsOrder != null && !dimensionsOrder.isEmpty()) {
            builder.startObject("ordered_dimensions");
            for (Dimension dimension : dimensionsOrder) {
                dimension.toXContent(builder, params);
            }
            builder.endObject();
        }
        if(metrics != null && !metrics.isEmpty()) {
            builder.startObject("metrics");
            for (Metric metric : metrics) {
                metric.toXContent(builder, params);
            }
            builder.endObject();
        }
        compositeFieldSpec.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
