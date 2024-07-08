/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.meta;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricEntry;

import java.io.IOException;
import java.util.List;

/**
 * An interface for metadata of the star-tree
 *
 * @opensearch.experimental
 */
public interface TreeMetadata {

    /**
     * Reads the count of dimensions in the star-tree.
     *
     * @return the count of dimensions
     * @throws IOException if an I/O error occurs while reading the dimensions count
     */
    int readDimensionsCount() throws IOException;

    /**
     * Reads the list of dimension ordinals in the star-tree.
     *
     * @return the list of dimension ordinals
     * @throws IOException if an I/O error occurs while reading the dimension ordinals
     */
    List<Integer> readStarTreeDimensions() throws IOException;

    /**
     * Reads the count of metrics in the star-tree.
     *
     * @return the count of metrics
     * @throws IOException if an I/O error occurs while reading the metrics count
     */
    int readMetricsCount() throws IOException;

    /**
     * Reads the list of metric entries in the star-tree.
     *
     * @return the list of metric entries
     * @throws IOException if an I/O error occurs while reading the metric entries
     */
    List<MetricEntry> readMetricEntries() throws IOException;

    /**
     * Reads the aggregated document count for the segment in the star-tree.
     *
     * @return the aggregated document count for the segment
     * @throws IOException if an I/O error occurs while reading the aggregated document count
     */
    int readSegmentAggregatedDocCount() throws IOException;

    /**
     * Reads the file pointer to the start of the star-tree data.
     *
     * @return the file pointer to the start of the star-tree data
     * @throws IOException if an I/O error occurs while reading the star-tree data start file pointer
     */
    long readDataStartFilePointer() throws IOException;

    /**
     * Reads the length of the data of the star-tree.
     *
     * @return the length of the data
     * @throws IOException if an I/O error occurs while reading the data length
     */
    long readDataLength() throws IOException;
}
