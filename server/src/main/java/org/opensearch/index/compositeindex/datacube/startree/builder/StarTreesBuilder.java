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
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexValues;
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

    public StarTreesBuilder(
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) {
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
    }


    /**
     * Builds the star-trees.
     */
    public void build(Map<String, DocValuesProducer> fieldProducerMap) throws IOException {
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
            try (StarTreeBuilder starTreeBuilder = getSingleTreeBuilder(starTreeField, fieldProducerMap, state, mapperService)) {
                starTreeBuilder.build();
            }
        }
        logger.debug("Took {} ms to building {} star-trees with star-tree fields", System.currentTimeMillis() - startTime, numStarTrees);
    }

    @Override
    public void close() throws IOException {

    }

    public void build(Map<String, List<CompositeIndexValues>> compositeIndexValuesPerField,
        Map<String, CompositeIndexFieldInfo> compositeIndexFieldInfoMap) {
        for(Map.Entry<String, List<CompositeIndexValues>> entry : compositeIndexValuesPerField.entrySet()) {

        }
    }

//    private Iterator<Record> mergeRecords(List<StarTreeAggregatedValues> aggrList) throws IOException {
//        List<BaseSingleTreeBuilder.Record> records = new ArrayList<>();
//        for (StarTreeAggregatedValues starTree : aggrList) {
//            boolean endOfDoc = false;
//            while (!endOfDoc) {
//                long[] dims = new long[starTree.dimensionValues.size()];
//                int i = 0;
//                for (Map.Entry<String, SortedNumericDocValues> dimValue : starTree.dimensionValues.entrySet()) {
//                    int doc = dimValue.getValue().nextDoc();
//                    long val = dimValue.getValue().nextValue();
//                    endOfDoc = doc == DocIdSetIterator.NO_MORE_DOCS || val == -1;
//                    if (endOfDoc) {
//                        break;
//                    }
//                    dims[i] = val;
//                    i++;
//                }
//                if (endOfDoc) {
//                    break;
//                }
//                i = 0;
//                Object[] metrics = new Object[starTree.metricValues.size()];
//                for (Map.Entry<String, SortedNumericDocValues> metricValue : starTree.metricValues.entrySet()) {
//                    metricValue.getValue().nextDoc();
//                    metrics[i] = metricValue.getValue().nextValue();
//                    i++;
//                }
//                BaseSingleTreeBuilder.Record record = new BaseSingleTreeBuilder.Record(dims, metrics);
//                records.add(record);
//            }
//        }
//        BaseSingleTreeBuilder.Record[] recordsArr = new BaseSingleTreeBuilder.Record[records.size()];
//        records.toArray(recordsArr);
//        records = null;
//        return sortRecords(recordsArr);
//    }

    private static StarTreeBuilder getSingleTreeBuilder(
        StarTreeField starTreeField,
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {
        switch (starTreeField.getStarTreeConfig().getBuildMode()) {
            case ON_HEAP:
                //return new OnHeapStarTreeBuilder(starTreeField, fieldProducerMap, state, mapperService);
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
