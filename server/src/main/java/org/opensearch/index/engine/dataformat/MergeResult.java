/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.Map;
import java.util.Optional;

/**
 * Result of a merge operation containing merged writer file sets.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeResult {

    private final Map<DataFormat, WriterFileSet> mergedWriterFileSet;
    private final RowIdMapping rowIdMapping;
    private final boolean aggregating;

    /**
     * Constructs a merge result with the given merged writer file sets.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        this(mergedWriterFileSet, null, false);
    }

    /**
     * Constructs a merge result with the given merged writer file sets and row ID mapping.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     * @param rowIdMapping the row ID mapping produced during the merge
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet, RowIdMapping rowIdMapping) {
        this(mergedWriterFileSet, rowIdMapping, false);
    }

    /**
     * Constructs a merge result with the given merged writer file sets, row ID mapping,
     * and aggregation marker.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     * @param rowIdMapping the row ID mapping produced during the merge, or null
     * @param aggregating whether this merge aggregated rows (e.g. a materialized-view
     *                    merge that collapses rows sharing the same grouping keys), in
     *                    which case row-count conservation between input and output
     *                    segments is intentionally not preserved
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet, RowIdMapping rowIdMapping, boolean aggregating) {
        this.mergedWriterFileSet = mergedWriterFileSet;
        this.rowIdMapping = rowIdMapping;
        this.aggregating = aggregating;
    }

    /**
     * Gets all merged writer file sets.
     *
     * @return map of data formats to merged writer file sets
     */
    public Map<DataFormat, WriterFileSet> getMergedWriterFileSet() {
        return mergedWriterFileSet;
    }

    /**
     * Gets the merged writer file set for a specific data format.
     *
     * @param dataFormat the data format
     * @return the merged writer file set for the specified format
     */
    public WriterFileSet getMergedWriterFileSetForDataformat(DataFormat dataFormat) {
        return mergedWriterFileSet.get(dataFormat);
    }

    /**
     * Gets the row id mapping.
     *
     * @return the row id mapping
     */
    public Optional<RowIdMapping> rowIdMapping() {
        return Optional.ofNullable(rowIdMapping);
    }

    /**
     * Whether this merge aggregated rows across input segments (e.g. a materialized-view
     * aggregating merge). When {@code true}, the merged output's row count is expected
     * to be less than or equal to the sum of input row counts and row-count conservation
     * checks must not be applied.
     *
     * @return {@code true} if this was an aggregating merge
     */
    public boolean isAggregating() {
        return aggregating;
    }
}
