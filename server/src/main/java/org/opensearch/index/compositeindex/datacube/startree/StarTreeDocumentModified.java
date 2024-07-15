/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Arrays;

/**
 * Star tree document
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeDocumentModified {
    public Long[] dimensions;
    public long[] dims;
    public double[] doubleMetrics;
    public long[] longMetrics;

    public StarTreeDocumentModified(long[] dimensions, double[] metrics) {
        this.dims = dimensions;
        this.doubleMetrics = metrics;
        this.longMetrics = null;
    }

    public StarTreeDocumentModified(long[] dimensions, long[] metrics) {
        this.dims = dimensions;
        this.longMetrics = metrics;
        this.doubleMetrics = null;
    }

    @Override
    public String toString() {
        return Arrays.toString(dims) + " | " + Arrays.toString(doubleMetrics);
    }
}
