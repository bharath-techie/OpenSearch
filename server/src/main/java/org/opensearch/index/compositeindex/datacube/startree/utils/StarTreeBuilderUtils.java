/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo1;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Util class for building star tree
 *
 * @opensearch.experimental
 */
public class StarTreeBuilderUtils {

    private StarTreeBuilderUtils() {}

    public static final int ALL = -1;

    /**
     * Represents a node in a tree data structure, specifically designed for a star-tree implementation.
     * A star-tree node will represent both star and non-star nodes.
     */
    @ExperimentalApi
    public static class TreeNode {

        /**
         * The dimension id for the dimension (field) associated with this star-tree node.
         */
        public int dimensionId = ALL;

        /**
         * The starting document id (inclusive) associated with this star-tree node.
         */
        public int startDocId = ALL;

        /**
         * The ending document id (exclusive) associated with this star-tree node.
         */
        public int endDocId = ALL;

        /**
         * The aggregated document id associated with this star-tree node.
         */
        public int aggregatedDocId = ALL;

        /**
         * The child dimension identifier associated with this star-tree node.
         */
        public int childDimensionId = ALL;

        /**
         * The value of the dimension associated with this star-tree node.
         */
        public long dimensionValue = ALL;

        /**
         * A flag indicating whether this node is a star node (a node that represents an aggregation of all dimensions).
         */
        public boolean isStarNode = false;

        /**
         * A map containing the child nodes of this star-tree node, keyed by their dimension id.
         */
        public Map<Long, TreeNode> children;
    }

    public static long serializeStarTree(IndexOutput dataOut, TreeNode rootNode, int numNodes) throws IOException {
        return StarTreeDataSerializer.serializeStarTree(dataOut, rootNode, numNodes);
    }

    public static void serializeStarTreeMetadata(
        IndexOutput metaOut,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        Integer segmentAggregatedCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {
        StarTreeMetaSerializer.serializeStarTreeMetadata(
            metaOut,
            CompositeMappedFieldType.CompositeFieldType.STAR_TREE,
            starTreeField,
            writeState,
            metricAggregatorInfos,
            segmentAggregatedCount,
            dataFilePointer,
            dataFileLength
        );
    }

    public static void serializeStarTreeMetadata1(
        IndexOutput metaOut,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        List<MetricAggregatorInfo1> metricAggregatorInfos,
        Integer segmentAggregatedCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {
        StarTreeMetaSerializer.serializeStarTreeMetadata1(
            metaOut,
            CompositeMappedFieldType.CompositeFieldType.STAR_TREE,
            starTreeField,
            writeState,
            metricAggregatorInfos,
            segmentAggregatedCount,
            dataFilePointer,
            dataFileLength
        );
    }

}
