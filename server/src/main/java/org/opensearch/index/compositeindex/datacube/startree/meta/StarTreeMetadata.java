/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.meta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Holds the associated metadata for the building of star-tree
 *
 * @opensearch.experimental
 */
public class StarTreeMetadata implements TreeMetadata {
    private static final Logger logger = LogManager.getLogger(TreeMetadata.class);
    private final IndexInput meta;
    private final String starTreeFieldName;
    private final String starTreeFieldType;
    private final List<Integer> dimensionFieldNumbers;
    private final List<MetricEntry> metricEntries;
    private final Integer segmentAggregatedDocCount;
    private final Integer maxLeafDocs;
    private final Set<Integer> skipStarNodeCreationInDims;
    private final StarTreeFieldConfiguration.StarTreeBuildMode starTreeBuildMode;
    private final long dataStartFilePointer;
    private final long dataLength;

    public StarTreeMetadata(IndexInput meta, String compositeFieldName, String compositeFieldType) throws IOException {
        this.meta = meta;
        try {
            this.starTreeFieldName = compositeFieldName;
            this.starTreeFieldType = compositeFieldType;
            this.dimensionFieldNumbers = readStarTreeDimensions();
            this.metricEntries = readMetricEntries();
            this.segmentAggregatedDocCount = readSegmentAggregatedDocCount();
            this.maxLeafDocs = readMaxLeafDocs();
            this.skipStarNodeCreationInDims = readSkipStarNodeCreationInDims();
            this.starTreeBuildMode = readBuildMode();
            this.dataStartFilePointer = readDataStartFilePointer();
            this.dataLength = readDataLength();
        } catch (Exception e) {
            logger.error("Unable to read star-tree metadata from the file");
            throw new CorruptIndexException("Unable to read star-tree metadata from the file", meta);
        }
    }

    @Override
    public int readDimensionsCount() throws IOException {
        return meta.readInt();
    }

    @Override
    public List<Integer> readStarTreeDimensions() throws IOException {
        int dimensionCount = readDimensionsCount();
        List<Integer> dimensionFieldNumbers = new ArrayList<>();

        for (int i = 0; i < dimensionCount; i++) {
            dimensionFieldNumbers.add(meta.readInt());
        }

        return dimensionFieldNumbers;
    }

    @Override
    public int readMetricsCount() throws IOException {
        return meta.readInt();
    }

    @Override
    public List<MetricEntry> readMetricEntries() throws IOException {
        int metricCount = readMetricsCount();
        List<MetricEntry> metricEntries = new ArrayList<>();

        for (int i = 0; i < metricCount; i++) {
            String metricName = meta.readString();
            String metricStat = meta.readString();
            metricEntries.add(new MetricEntry(metricName, MetricStat.fromTypeName(metricStat)));
        }

        return metricEntries;
    }

    @Override
    public int readSegmentAggregatedDocCount() throws IOException {
        return meta.readInt();
    }

    @Override
    public int readMaxLeafDocs() throws IOException {
        return meta.readInt();
    }

    @Override
    public int readSkipStarNodeCreationInDimsCount() throws IOException {
        return meta.readInt();
    }

    @Override
    public Set<Integer> readSkipStarNodeCreationInDims() throws IOException {

        int skipStarNodeCreationInDimsCount = readSkipStarNodeCreationInDimsCount();
        Set<Integer> skipStarNodeCreationInDims = new HashSet<>();
        for (int i = 0; i < skipStarNodeCreationInDimsCount; i++) {
            skipStarNodeCreationInDims.add(meta.readInt());
        }
        return skipStarNodeCreationInDims;
    }

    @Override
    public StarTreeFieldConfiguration.StarTreeBuildMode readBuildMode() throws IOException {
        return StarTreeFieldConfiguration.StarTreeBuildMode.fromTypeName(meta.readString());
    }

    @Override
    public long readDataStartFilePointer() throws IOException {
        return meta.readLong();
    }

    @Override
    public long readDataLength() throws IOException {
        return meta.readLong();
    }

    /**
     * Returns the name of the star-tree field.
     *
     * @return star-tree field name
     */
    public String getStarTreeFieldName() {
        return starTreeFieldName;
    }

    /**
     * Returns the type of the star tree field.
     *
     * @return star-tree field type
     */
    public String getStarTreeFieldType() {
        return starTreeFieldType;
    }

    /**
     * Returns the list of dimension field numbers.
     *
     * @return star-tree dimension field numbers
     */
    public List<Integer> getDimensionFieldNumbers() {
        return dimensionFieldNumbers;
    }

    /**
     * Returns the list of metric entries.
     *
     * @return star-tree metric entries
     */
    public List<MetricEntry> getMetricEntries() {
        return metricEntries;
    }

    /**
     * Returns the aggregated document count for the star-tree.
     *
     * @return the aggregated document count for the star-tree.
     */
    public Integer getSegmentAggregatedDocCount() {
        return segmentAggregatedDocCount;
    }

    /**
     * Returns the max leaf docs for the star-tree.
     *
     * @return the max leaf docs.
     */
    public Integer getMaxLeafDocs() {
        return maxLeafDocs;
    }

    /**
     * Returns the set of dimensions for which star node will not be created in the star-tree.
     *
     * @return the set of dimensions.
     */
    public Set<Integer> getSkipStarNodeCreationInDims() {
        return skipStarNodeCreationInDims;
    }

    /**
     * Returns the build mode for the star-tree.
     *
     * @return the star-tree build mode.
     */
    public StarTreeFieldConfiguration.StarTreeBuildMode getStarTreeBuildMode() {
        return starTreeBuildMode;
    }

    /**
     * Returns the file pointer to the start of the star-tree data.
     *
     * @return start file pointer for star-tree data
     */
    public long getDataStartFilePointer() {
        return dataStartFilePointer;
    }

    /**
     * Returns the length of star-tree data
     *
     * @return star-tree length
     */
    public long getDataLength() {
        return dataLength;
    }
}
