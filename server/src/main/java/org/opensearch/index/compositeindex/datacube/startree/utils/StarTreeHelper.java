/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;

/**
 * This class contains helper methods used throughout the Star Tree index implementation.
 *
 * @opensearch.experimental
 */
public class StarTreeHelper {

    /**
     * The suffix appended to dimension field names in the Star Tree index.
     */
    public static final String DIMENSION_SUFFIX = "_dim";

    /**
     * The suffix appended to metric field names in the Star Tree index.
     */
    public static final String METRIC_SUFFIX = "_metric";

    public static String fullFieldNameForStarTreeDimensionsDocValues(String starTreeFieldName, String dimensionName) {
        return starTreeFieldName + "_" + dimensionName + "_" + DIMENSION_SUFFIX;
    }

    public static String fullFieldNameForStarTreeMetricsDocValues(String name, String fieldName, String metricName) {
        return MetricAggregatorInfo.toFieldName(name, fieldName, metricName) + "_" + METRIC_SUFFIX;
    }

}
