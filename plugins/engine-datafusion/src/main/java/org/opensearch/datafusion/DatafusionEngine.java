/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionQueryPhaseExecutor;
import org.opensearch.datafusion.search.DatafusionReader;
import org.opensearch.datafusion.search.DatafusionReaderManager;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.datafusion.search.DatafusionSearcherSupplier;
import org.opensearch.index.engine.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.mapper.*;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QueryPhaseExecutor;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.search.query.GenericQueryPhaseSearcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;

public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher,
    DatafusionReaderManager, DatafusionQuery> {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;
    private DataFusionService datafusionService;

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot, DataFusionService dataFusionService) throws IOException {
        this.dataFormat = dataFormat;
        this.datafusionReaderManager = new DatafusionReaderManager("/Users/animodak/Documents/logs/", formatCatalogSnapshot); // TODO: FigureOutPath
        this.datafusionService = dataFusionService;
    }

    @Override
    public GenericQueryPhaseSearcher<DatafusionContext, DatafusionSearcher, DatafusionQuery> getQueryPhaseSearcher() {
        return new DatafusionQueryPhaseSearcher();
    }

    @Override
    public QueryPhaseExecutor<DatafusionContext> getQueryPhaseExecutor() {
        return new DatafusionQueryPhaseExecutor();
    }

    @Override
    public DatafusionContext createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTask task) throws IOException {
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, request, task, this);
        // Parse source
        datafusionContext.datafusionQuery(new DatafusionQuery(request.source().queryPlanIR(), new ArrayList<>()));
        return datafusionContext;
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        return acquireSearcherSupplier(wrapper, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper, Engine.SearcherScope scope) throws EngineException {
        // TODO : wrapper is ignored
        EngineSearcherSupplier<DatafusionSearcher> searcher = null;
        // TODO : refcount needs to be revisited - add proper tests for exception etc
        try {
            DatafusionReader reader = datafusionReaderManager.acquire();
            searcher = new DatafusionSearcherSupplier(null) {
                @Override
                protected DatafusionSearcher acquireSearcherInternal(String source) {
                    return new DatafusionSearcher(source, reader, () -> {});
                }

                @Override
                protected void doClose() {
                    try {
                        reader.decRef();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        } catch (Exception ex) {
            // TODO
        }
        return searcher;
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException {
        return acquireSearcher(source, scope, Function.identity());
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope, Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        DatafusionSearcherSupplier releasable = null;
        try {
            DatafusionSearcherSupplier searcherSupplier = releasable = (DatafusionSearcherSupplier) acquireSearcherSupplier(wrapper, scope);
            DatafusionSearcher searcher = searcherSupplier.acquireSearcher(source);
            releasable = null;
            return new DatafusionSearcher(
                source,
                searcher.getReader(),
                () -> Releasables.close(searcher, searcherSupplier)
            );
        } finally {
            Releasables.close(releasable);
        }
    }

    @Override
    public DatafusionReaderManager getReferenceManager(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public CatalogSnapshotAwareRefreshListener getRefreshListener(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope) {
        return false;
    }

    @Override
    public Map<String, Object[]> execute(DatafusionContext context) {
        Map<String, Object[]> finalRes = new HashMap<>();
        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getTokioRuntimePointer());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getTokioRuntimePointer() , allocator);

            // We can have some collectors passed like this which can collect the results and convert to InternalAggregation
            // Is the possible? need to check

            SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                @Override
                public void collect(RecordBatchStream value) {
                    VectorSchemaRoot root = value.getVectorSchemaRoot();
                    for (Field field : root.getSchema().getFields()) {
                        String filedName = field.getName();
                        FieldVector fieldVector = root.getVector(filedName);
                        Object[] fieldValues = new Object[fieldVector.getValueCount()];
                        for (int i = 0; i < fieldVector.getValueCount(); i++) {
                            fieldValues[i] = fieldVector.getObject(i);
                        }
                        finalRes.put(filedName, fieldValues);
                    }
                }
            };

            while (stream.loadNextBatch().join()) {
                collector.collect(stream);
            }

            logger.info("Final Results:");
            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
        }
        return finalRes;
    }


    // TODO: make this Map<docIds, BytesRef>?
    public List<BytesReference> executeFetchPhase(DatafusionContext context) throws IOException {
//        List<Long> rowIds = context.getDFResults() // TODO: get row_ids, projections from dfContext which was returned in query phase
        List<Long> rowIds = new ArrayList<>();
        List<String> projectionFields = new ArrayList<>();

        DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
        long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getTokioRuntimePointer()); // update to handle fetchPhase query
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getTokioRuntimePointer() , allocator);
        return generateByteRefs(context, stream);
    }

    public List<BytesReference> generateByteRefs(DatafusionContext context, RecordBatchStream recordBatchStream) throws IOException {
        MapperService mapperService = context.mapperService();
        List<BytesReference> byteRefs = new ArrayList<>();
        while(recordBatchStream.loadNextBatch().join()) {
            VectorSchemaRoot vectorSchemaRoot = recordBatchStream.getVectorSchemaRoot();
            List<FieldVector> fieldVectorList = vectorSchemaRoot.getFieldVectors();
            for(int i=0; i<vectorSchemaRoot.getRowCount(); i++) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

                try {
                    for (FieldVector valueVectors : fieldVectorList) {
                        System.out.println("Getting mapper");
                        ObjectMapper mapper = mapperService.getObjectMapper(valueVectors.getName());
                        System.out.println(mapper.simpleName());
                        DerivedFieldGenerator derivedFieldGenerator = mapper.derivedFieldGenerator();
//                            derivedFieldGenerator.convert(fieldVector.getObject(i));
                        derivedFieldGenerator.generate(builder, List.of(valueVectors.getObject(i)));
                    }
                } catch (Exception e) {
                    throw new OpenSearchException("Failed to derive source for doc id [" + i + "]", e);
                } finally {
                    builder.endObject();
                }
                System.out.println("Object: ");
                System.out.println(builder.toString());
                byteRefs.add(BytesReference.bytes(builder));
            }
        }
        return byteRefs;
    }
//
//    class ParquetValuesGenerator() {
//        ObjectMapper objectMapper;
//        ParquetValuesGenerator(ObjectMapper objectMapper, VectorSchemaRoot vectorSchemaRoot) {
//            this.objectMapper = objectMapper;
//        }
//
//        public List<BytesReference> generate(RecordBatchStream recordBatchStream) throws IOException {
//            List<BytesReference> byteRefs = new ArrayList<>();
//            while(recordBatchStream.loadNextBatch().join()) {
//                VectorSchemaRoot vectorSchemaRoot = recordBatchStream.getVectorSchemaRoot();
//                List<FieldVector> fieldVectorList = vectorSchemaRoot.getFieldVectors();
//                for(int i=0; i<vectorSchemaRoot.getRowCount(); i++) {
//                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
//
//                    try {
//                        for (Mapper mapper : objectMapper) {
//                            FieldVector fieldVector = vectorSchemaRoot.getVector(mapper.simpleName());
//                            DerivedFieldGenerator derivedFieldGenerator = mapper.derivedFieldGenerator();
////                            derivedFieldGenerator.convert(fieldVector.getObject(i));
//                            derivedFieldGenerator.generate(builder, List.of(fieldVector.getObject(i)));
//                        }
//                    } catch (Exception e) {
//                        throw new OpenSearchException("Failed to derive source for doc id [" + i + "]", e);
//                    } finally {
//                        builder.endObject();
//                    }
//                    byteRefs.add(BytesReference.bytes(builder));
//                }
//
//            }
//            return byteRefs;
//        }
//    }
}
