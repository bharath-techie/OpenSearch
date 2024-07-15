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
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builder to construct star-trees based on multiple star-tree fields.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreesBuilder implements Closeable {

    private static final Logger logger = LogManager.getLogger(StarTreesBuilder.class);

    private final List<StarTreeField> starTreeFields;
    private final SegmentWriteState state;
    private final MapperService mapperService;
    private AtomicInteger fieldNumberAcrossStarTrees;

    public StarTreesBuilder(SegmentWriteState segmentWriteState, MapperService mapperService) {
        List<StarTreeField> starTreeFields = new ArrayList<>();
        for (CompositeMappedFieldType compositeMappedFieldType : mapperService.getCompositeFieldTypes()) {
            if (compositeMappedFieldType instanceof StarTreeMapper.StarTreeFieldType) {
                StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) compositeMappedFieldType;
                starTreeFields.add(
                    new StarTreeField(
                        starTreeFieldType.name(),
                        starTreeFieldType.getDimensions(),
                        starTreeFieldType.getMetrics(),
                        starTreeFieldType.getStarTreeConfig()
                    )
                );
            }
        }
        this.starTreeFields = starTreeFields;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        this.fieldNumberAcrossStarTrees = new AtomicInteger();
    }

    /**
     * Builds the star-trees.
     */
    public void build(
        IndexOutput metaOut,
        IndexOutput dataOut,
        Map<String, DocValuesProducer> fieldProducerMap,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        if (starTreeFields.isEmpty()) {
            logger.debug("no star-tree fields found, returning from star-tree builder");
            return;
        }
        long startTime = System.currentTimeMillis();

        int numStarTrees = starTreeFields.size();
        logger.debug("Starting building {} star-trees with star-tree fields", numStarTrees);

        // Build all star-trees
        for (int i = 0; i < numStarTrees; i++) {
            StarTreeField starTreeField = starTreeFields.get(i);
            try (StarTreeBuilder starTreeBuilder = getSingleTreeBuilder(metaOut, dataOut, starTreeField, state, mapperService)) {
                starTreeBuilder.build(fieldProducerMap, fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
                starTreeBuilder.close();
            }
        }
        logger.debug("Took {} ms to building {} star-trees with star-tree fields", System.currentTimeMillis() - startTime, numStarTrees);
    }

    @Override
    public void close() throws IOException {
        // TODO : close files
    }

    /**
     * Merges star tree fields from multiple segments
     *
     * @param metaOut
     * @param dataOut
     * @param starTreeFieldMap             StarTreeField configuration per field
     * @param starTreeValuesSubsPerField   starTreeValuesSubs per field
     * @param starTreeDocValuesConsumer
     */
    public void buildDuringMerge(
        IndexOutput metaOut,
        IndexOutput dataOut,
        final Map<String, StarTreeField> starTreeFieldMap,
        final Map<String, List<StarTreeValues>> starTreeValuesSubsPerField,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        for (Map.Entry<String, List<StarTreeValues>> entry : starTreeValuesSubsPerField.entrySet()) {
            List<StarTreeValues> starTreeValuesList = entry.getValue();
            StarTreeField starTreeField = starTreeFieldMap.get(entry.getKey());
            StarTreeBuilder builder = getSingleTreeBuilder(metaOut, dataOut, starTreeField, state, mapperService);
            builder.build(starTreeValuesList, fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
            builder.close();
        }
    }

    /**
     * Get star-tree builder based on build mode.
     */
    private static StarTreeBuilder getSingleTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {
        switch (starTreeField.getStarTreeConfig().getBuildMode()) {
            case ON_HEAP:
                // return new OnHeapStarTreeBuilder(metaOut, dataOut, starTreeField, state, mapperService);
            case OFF_HEAP:
                return new OffHeapIntroSortBuilder(metaOut, dataOut, starTreeField, state, mapperService);
            default:
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "No star tree implementation is available for [%s] build mode",
                        starTreeField.getStarTreeConfig().getBuildMode()
                    )
                );
        }
    }
}
