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
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo1;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.MAGIC_MARKER;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.VERSION;

/**
 * The utility class for serializing the metadata of a star-tree data structure.
 * The metadata includes information about the dimensions, metrics, and other relevant details
 * related to the star tree.
 *
 * @opensearch.experimental
 */
public class StarTreeMetaSerializer {

    private static final Logger logger = LogManager.getLogger(StarTreeMetaSerializer.class);

    /**
     * Serializes the star-tree metadata.
     *
     * @param metaOut                the IndexOutput to write the metadata
     * @param compositeFieldType     the composite field type of the star-tree field
     * @param starTreeField          the star-tree field
     * @param writeState             the segment write state
     * @param metricAggregatorInfos  the list of metric aggregator information
     * @param segmentAggregatedCount the aggregated document count for the segment
     * @param dataFilePointer        the file pointer to the start of the star tree data
     * @param dataFileLength         the length of the star tree data file
     * @throws IOException if an I/O error occurs while serializing the metadata
     */
    public static void serializeStarTreeMetadata(
        IndexOutput metaOut,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        Integer segmentAggregatedCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {
        long totalSizeInBytes = 0;

        // header size
        totalSizeInBytes += computeHeaderByteSize(compositeFieldType, starTreeField.getName());
        // number of dimensions
        totalSizeInBytes += Integer.BYTES;
        // dimension field numbers
        totalSizeInBytes += (long) starTreeField.getDimensionsOrder().size() * Integer.BYTES;
        // metric count
        totalSizeInBytes += Integer.BYTES;
        // metric - metric stat pair
        totalSizeInBytes += computeMetricEntriesSizeInBytes(metricAggregatorInfos);
        // segment aggregated document count
        totalSizeInBytes += Integer.BYTES;
        // max leaf docs
        totalSizeInBytes += Integer.BYTES;
        // skip star node creation dimensions count
        totalSizeInBytes += Integer.BYTES;
        // skip star node creation dimensions field numbers
        totalSizeInBytes += (long) starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims().size() * Integer.BYTES;
        // data start file pointer
        totalSizeInBytes += Long.BYTES;
        // data length
        totalSizeInBytes += Long.BYTES;

        logger.info("Star tree size in bytes : {}", totalSizeInBytes);

        writeMetaHeader(metaOut, compositeFieldType, starTreeField.getName());
        writeMeta(metaOut, writeState, metricAggregatorInfos, starTreeField, segmentAggregatedCount, dataFilePointer, dataFileLength);
    }

    public static void serializeStarTreeMetadata1(
        IndexOutput metaOut,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo1> metricAggregatorInfos,
        Integer segmentAggregatedCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {
        long totalSizeInBytes = 0;

        // header size
        totalSizeInBytes += computeHeaderByteSize(compositeFieldType, starTreeField.getName());
        // number of dimensions
        totalSizeInBytes += Integer.BYTES;
        // dimension field numbers
        totalSizeInBytes += (long) starTreeField.getDimensionsOrder().size() * Integer.BYTES;
        // metric count
        totalSizeInBytes += Integer.BYTES;
        // metric - metric stat pair
        totalSizeInBytes += computeMetricEntriesSizeInBytes1(metricAggregatorInfos);
        // segment aggregated document count
        totalSizeInBytes += Integer.BYTES;
        // max leaf docs
        totalSizeInBytes += Integer.BYTES;
        // skip star node creation dimensions count
        totalSizeInBytes += Integer.BYTES;
        // skip star node creation dimensions field numbers
        totalSizeInBytes += (long) starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims().size() * Integer.BYTES;
        // data start file pointer
        totalSizeInBytes += Long.BYTES;
        // data length
        totalSizeInBytes += Long.BYTES;

        logger.info("Star tree size in bytes : {}", totalSizeInBytes);

        writeMetaHeader(metaOut, compositeFieldType, starTreeField.getName());
        writeMeta1(metaOut, writeState, metricAggregatorInfos, starTreeField, segmentAggregatedCount, dataFilePointer, dataFileLength);
    }

    /**
     * Computes the byte size required to store the star-tree metric entry.
     *
     * @param metricAggregatorInfos the list of metric aggregator information
     * @return the byte size required to store the metric-metric stat pairs
     */
    private static long computeMetricEntriesSizeInBytes(List<MetricAggregatorInfo> metricAggregatorInfos) {

        long totalMetricEntriesSize = 0;

        for (MetricAggregatorInfo metricAggregatorInfo : metricAggregatorInfos) {
            totalMetricEntriesSize += metricAggregatorInfo.getField().getBytes(UTF_8).length;
            totalMetricEntriesSize += metricAggregatorInfo.getMetricStat().getTypeName().getBytes(UTF_8).length;
        }

        return totalMetricEntriesSize;
    }

    private static long computeMetricEntriesSizeInBytes1(List<MetricAggregatorInfo1> metricAggregatorInfos) {

        long totalMetricEntriesSize = 0;

        for (MetricAggregatorInfo1 metricAggregatorInfo : metricAggregatorInfos) {
            totalMetricEntriesSize += metricAggregatorInfo.getField().getBytes(UTF_8).length;
            totalMetricEntriesSize += metricAggregatorInfo.getMetricStat().getTypeName().getBytes(UTF_8).length;
        }

        return totalMetricEntriesSize;
    }

    /**
     * Computes the byte size of the star-tree metadata header.
     *
     * @param compositeFieldType the composite field type of the star-tree field
     * @param starTreeFieldName  the name of the star-tree field
     * @return the byte size of the star-tree metadata header
     */
    private static int computeHeaderByteSize(CompositeMappedFieldType.CompositeFieldType compositeFieldType, String starTreeFieldName) {
        // Magic marker (8), version (4), size of header (4)
        int headerSizeInBytes = 16;

        // For star-tree field name
        headerSizeInBytes += starTreeFieldName.getBytes(UTF_8).length;

        // For star tree field type
        headerSizeInBytes += compositeFieldType.getName().getBytes(UTF_8).length;

        return headerSizeInBytes;
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
        metaOut.writeLong(MAGIC_MARKER);

        // version
        metaOut.writeInt(VERSION);

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
        metaOut.writeInt(starTreeField.getDimensionsOrder().size());

        // dimensions
        for (Dimension dimension : starTreeField.getDimensionsOrder()) {
            int dimensionFieldNumber = writeState.fieldInfos.fieldInfo(dimension.getField()).getFieldNumber();
            metaOut.writeInt(dimensionFieldNumber);
        }

        // number of metrics
        metaOut.writeInt(metricAggregatorInfos.size());

        // metric - metric stat pair
        for (MetricAggregatorInfo metricAggregatorInfo : metricAggregatorInfos) {
            String metricName = metricAggregatorInfo.getField();
            String metricStatName = metricAggregatorInfo.getMetricStat().getTypeName();
            metaOut.writeString(metricName);
            metaOut.writeString(metricStatName);
        }

        // segment aggregated document count
        metaOut.writeInt(segmentAggregatedDocCount);

        // max leaf docs
        metaOut.writeInt(starTreeField.getStarTreeConfig().maxLeafDocs());

        // number of skip star node creation dimensions
        metaOut.writeInt(starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims().size());

        // skip star node creations
        for (String dimension : starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims()) {
            int dimensionFieldNumber = writeState.fieldInfos.fieldInfo(dimension).getFieldNumber();
            metaOut.writeInt(dimensionFieldNumber);
        }

        // star tree build-mode
        metaOut.writeString(starTreeField.getStarTreeConfig().getBuildMode().getTypeName());

        // star-tree data file pointer
        metaOut.writeLong(dataFilePointer);

        // star-tree data file length
        metaOut.writeLong(dataFileLength);

    }

    private static void writeMeta1(
        IndexOutput metaOut,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo1> metricAggregatorInfos,
        StarTreeField starTreeField,
        Integer segmentAggregatedDocCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {

        // number of dimensions
        metaOut.writeInt(starTreeField.getDimensionsOrder().size());

        // dimensions
        for (Dimension dimension : starTreeField.getDimensionsOrder()) {
            int dimensionFieldNumber = writeState.fieldInfos.fieldInfo(dimension.getField()).getFieldNumber();
            metaOut.writeInt(dimensionFieldNumber);
        }

        // number of metrics
        metaOut.writeInt(metricAggregatorInfos.size());

        // metric - metric stat pair
        for (MetricAggregatorInfo1 metricAggregatorInfo : metricAggregatorInfos) {
            String metricName = metricAggregatorInfo.getField();
            String metricStatName = metricAggregatorInfo.getMetricStat().getTypeName();
            metaOut.writeString(metricName);
            metaOut.writeString(metricStatName);
        }

        // segment aggregated document count
        metaOut.writeInt(segmentAggregatedDocCount);

        // max leaf docs
        metaOut.writeInt(starTreeField.getStarTreeConfig().maxLeafDocs());

        // number of skip star node creation dimensions
        metaOut.writeInt(starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims().size());

        // skip star node creations
        for (String dimension : starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims()) {
            int dimensionFieldNumber = writeState.fieldInfos.fieldInfo(dimension).getFieldNumber();
            metaOut.writeInt(dimensionFieldNumber);
        }

        // star tree build-mode
        metaOut.writeString(starTreeField.getStarTreeConfig().getBuildMode().getTypeName());

        // star-tree data file pointer
        metaOut.writeLong(dataFilePointer);

        // star-tree data file length
        metaOut.writeLong(dataFileLength);

    }
}
