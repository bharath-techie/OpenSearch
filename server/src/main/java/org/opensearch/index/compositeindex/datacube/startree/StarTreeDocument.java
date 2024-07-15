/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Star tree document
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeDocument {
    public Long[] dimensions;
    public Object[] metrics;
    public double[] doubleMetrics;
    public long[] longMetrics;

    public StarTreeDocument(Long[] dimensions, Object[] metrics) {
        this.dimensions = dimensions;
        this.metrics = metrics;
    }
}
