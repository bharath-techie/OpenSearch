/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.List;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.MAGIC_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;

/**
 * The utility class for serializing the metadata of a star-tree data structure.
 * The metadata includes information about the dimensions, metrics, and other relevant details
 * related to the star tree.
 *
 * @opensearch.experimental
 */
public class StarTreeMetaWriter {

    private static final Logger logger = LogManager.getLogger(StarTreeMetaWriter.class);

    /**
     * Writes the star-tree metadata.
     *
     * @param metaOut                the IndexOutput to write the metadata
     * @param starTreeField          the star-tree field
     * @param writeState             the segment write state
     * @param metricAggregatorInfos  the list of metric aggregator information
     * @param segmentAggregatedCount the aggregated document count for the segment
     * @param dataFilePointer        the file pointer to the start of the star tree data
     * @param dataFileLength         the length of the star tree data file
     * @throws IOException if an I/O error occurs while serializing the metadata
     */
    public static void writeStarTreeMetadata(
        IndexOutput metaOut,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        Integer segmentAggregatedCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {

        long initialMetaFilePointer = metaOut.getFilePointer();

        writeMetaHeader(metaOut, CompositeMappedFieldType.CompositeFieldType.STAR_TREE, starTreeField.getName());
        writeMeta(metaOut, writeState, metricAggregatorInfos, starTreeField, segmentAggregatedCount, dataFilePointer, dataFileLength);

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Star tree meta size in bytes : {} for star-tree field {}",
                metaOut.getFilePointer() - initialMetaFilePointer,
                starTreeField.getName()
            );
        }
    }

    /**
     * Writes the star-tree metadata header.
     *
     * @param metaOut            the IndexOutput to write the header
     * @param compositeFieldType the composite field type of the star-tree field
     * @param starTreeFieldName  the name of the star-tree field
     * @throws IOException if an I/O error occurs while writing the header
     */
    private static void writeMetaHeader(
        IndexOutput metaOut,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        String starTreeFieldName
    ) throws IOException {
        // magic marker for sanity
        metaOut.writeVLong(MAGIC_MARKER);

        // version
        metaOut.writeVInt(VERSION);

        // star tree field name
        metaOut.writeString(starTreeFieldName);

        // star tree field type
        metaOut.writeString(compositeFieldType.getName());
    }

    /**
     * Writes the star-tree metadata.
     *
     * @param metaOut                   the IndexOutput to write the metadata
     * @param writeState                the segment write state
     * @param metricAggregatorInfos     the list of metric aggregator information
     * @param starTreeField             the star tree field
     * @param segmentAggregatedDocCount the aggregated document count for the segment
     * @param dataFilePointer           the file pointer to the start of the star-tree data
     * @param dataFileLength            the length of the star-tree data file
     * @throws IOException if an I/O error occurs while writing the metadata
     */
    private static void writeMeta(
        IndexOutput metaOut,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        StarTreeField starTreeField,
        Integer segmentAggregatedDocCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {

        // number of dimensions
        metaOut.writeVInt(starTreeField.getDimensionsOrder().size());

        // dimensions
        for (Dimension dimension : starTreeField.getDimensionsOrder()) {
            int dimensionFieldNumber = writeState.fieldInfos.fieldInfo(dimension.getField()).getFieldNumber();
            metaOut.writeVInt(dimensionFieldNumber);
        }

        // number of metrics
        metaOut.writeVInt(metricAggregatorInfos.size());

        // metric - metric stat pair
        for (MetricAggregatorInfo metricAggregatorInfo : metricAggregatorInfos) {
            String metricName = metricAggregatorInfo.getField();
            int metricFieldNumber = writeState.fieldInfos.fieldInfo(metricName).getFieldNumber();
            int metricStatOrdinal = metricAggregatorInfo.getMetricStat().getMetricOrdinal();
            metaOut.writeVInt(metricFieldNumber);
            metaOut.writeVInt(metricStatOrdinal);
        }

        // segment aggregated document count
        metaOut.writeVInt(segmentAggregatedDocCount);

        // max leaf docs
        metaOut.writeVInt(starTreeField.getStarTreeConfig().maxLeafDocs());

        // number of skip star node creation dimensions
        metaOut.writeVInt(starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims().size());

        // skip star node creations
        for (String dimension : starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims()) {
            int dimensionFieldNumber = writeState.fieldInfos.fieldInfo(dimension).getFieldNumber();
            metaOut.writeVInt(dimensionFieldNumber);
        }

        // star tree build-mode
        metaOut.writeByte(starTreeField.getStarTreeConfig().getBuildMode().getBuildModeOrdinal());

        // star-tree data file pointer
        metaOut.writeVLong(dataFilePointer);

        // star-tree data file length
        metaOut.writeVLong(dataFileLength);

    }
}
