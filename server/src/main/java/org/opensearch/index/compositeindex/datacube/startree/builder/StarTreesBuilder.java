/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.opensearch.index.mapper.StarTreeMapper;


/**
 * Builder to construct star-trees based on multiple star-tree fields.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreesBuilder implements Closeable {

    private static final Logger logger = LogManager.getLogger(StarTreesBuilder.class);
    private final SegmentWriteState state;
    private final Map<String, DocValuesProducer> fieldProducerMap;
    private final MapperService mapperService;
    private final List<StarTreeMapper.StarTreeFieldType> starTreeFields;

    public StarTreesBuilder(
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) {
        this.fieldProducerMap = fieldProducerMap;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        starTreeFields = new ArrayList<>();
        for(CompositeMappedFieldType type : mapperService.getCompositeFieldTypes()) {
            if(type instanceof StarTreeMapper.StarTreeFieldType) {
                starTreeFields.add((StarTreeMapper.StarTreeFieldType) type);
            }
        }
    }

    /**
     * Builds the star-trees.
     */
    public void build() throws IOException {
        long startTime = System.currentTimeMillis();
        int numStarTrees = starTreeFields.size();
        // Build all star-trees
        for (int i = 0; i < numStarTrees; i++) {
            StarTreeMapper.StarTreeFieldType starTreeField = starTreeFields.get(i);
            try (StarTreeBuilder starTreeBuilder = getSingleTreeBuilder(starTreeField, starTreeField.getStarTreeConfig()
                .getBuildMode(), fieldProducerMap, state, mapperService)) {
                starTreeBuilder.build();
            }
        }
    }

    @Override
    public void close() throws IOException {

    }

    private static StarTreeBuilder getSingleTreeBuilder(
        StarTreeMapper.StarTreeFieldType starTreeField,
        StarTreeFieldConfiguration.StarTreeBuildMode buildMode,
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {
        switch (buildMode) {
            case ON_HEAP:
                StarTreeField field = new StarTreeField(starTreeField.name(), starTreeField.getDimensions(), starTreeField.getMetrics(),
                    starTreeField.getStarTreeConfig());
                return new OnHeapStarTreeBuilder(field, fieldProducerMap, state, mapperService);
            default:
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "No star tree implementation is available for [%s] build mode", buildMode)
                );
        }
    }
}
