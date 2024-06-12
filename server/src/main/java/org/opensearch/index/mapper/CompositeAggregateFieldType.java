/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.index.compositeindex.Dimension;
import org.opensearch.index.compositeindex.Metric;


public abstract class CompositeAggregateFieldType extends CompositeMappedFieldType {
    private final List<Dimension> dimensions;
    private final List<Metric> metrics;
    public CompositeAggregateFieldType(String name, List<Dimension> dims, List<Metric> metrics) {
        super(name, getFields(dims, metrics));
        this.dimensions = dims;
        this.metrics = metrics;
    }

    private static List<String> getFields(List<Dimension> dims, List<Metric> metrics) {
        Set<String> fields = new HashSet<>();
        for(Dimension dim : dims) {
            fields.add(dim.getField());
        }
        for(Metric metric : metrics) {
            fields.add(metric.getField());
        }
        return new ArrayList<>(fields);
    }
}
