/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.MapperService;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Builder to construct star-trees based on multiple star-tree fields.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreesBuilder implements Closeable {

    private static final Logger logger = LogManager.getLogger(StarTreesBuilder.class);

    private final List<StarTreeField> starTreeFields;
    private final StarTreeFieldConfiguration.StarTreeBuildMode buildMode;
    private final SegmentWriteState state;
    private final Map<String, DocValuesProducer> fieldProducerMap;
    private final MapperService mapperService;

    public StarTreesBuilder(
        List<StarTreeField> starTreeFields,
        StarTreeFieldConfiguration.StarTreeBuildMode buildMode,
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) {
        this.starTreeFields = starTreeFields;
        if (starTreeFields == null || starTreeFields.isEmpty()) {
            throw new IllegalArgumentException("Must provide star-tree field to build star trees");
        }
        this.buildMode = buildMode;
        this.fieldProducerMap = fieldProducerMap;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
    }

    /**
     * Builds the star-trees.
     */
    public void build() throws IOException {
        long startTime = System.currentTimeMillis();
        int numStarTrees = starTreeFields.size();
        logger.debug("Starting building {} star-trees with star-tree fields using {} builder", numStarTrees, buildMode);

        // Build all star-trees
        for (int i = 0; i < numStarTrees; i++) {
            StarTreeField starTreeField = starTreeFields.get(i);
            try (StarTreeBuilder starTreeBuilder = getSingleTreeBuilder(starTreeField, buildMode, fieldProducerMap, state, mapperService)) {
                starTreeBuilder.build();
            }
        }
        logger.debug(
            "Took {} ms to building {} star-trees with star-tree fields using {} builder",
            System.currentTimeMillis() - startTime,
            numStarTrees,
            buildMode
        );
    }

    @Override
    public void close() throws IOException {

    }

    private static StarTreeBuilder getSingleTreeBuilder(
        StarTreeField starTreeField,
        StarTreeFieldConfiguration.StarTreeBuildMode buildMode,
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {
        switch (buildMode) {
            case ON_HEAP:
                return new OnHeapStarTreeBuilder(starTreeField, fieldProducerMap, state, mapperService);
            default:
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "No star tree implementation is available for [%s] build mode", buildMode)
                );
        }
    }
}
