/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link QueryPlanExecutor} default implementation.
 * <p>
 * Acquires a composite reader, selects a {@link SearchBackEndPlugin}, and
 * delegates query execution to it. The plugin provides both reader management
 * and query execution — no separate analytics SPI needed.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, SearchBackEndPlugin<?>> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;

    /**
     * Constructs a DefaultPlanExecutor.
     *
     * @param plugins list of search backend plugins (unified storage + query)
     * @param indicesService service for accessing index shards
     * @param clusterService service for accessing cluster state
     */
    public DefaultPlanExecutor(
        List<SearchBackEndPlugin<?>> plugins,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        this.backEnds = new LinkedHashMap<>();
        for (SearchBackEndPlugin<?> plugin : plugins) {
            this.backEnds.put(plugin.name(), plugin);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        String tableName = extractTableName(logicalFragment);
        SearchBackEndPlugin<?> plugin = selectBackEnd();
        if (plugin == null) {
            return new ArrayList<>();
        }

        IndexShard shard = resolveShard(tableName);
        DataFormatAwareEngine dataFormatAwareEngine = shard.getCompositeEngine();
        if (dataFormatAwareEngine == null) {
            throw new IllegalStateException("No CompositeEngine on shard [" + shard.shardId() + "]");
        }

        SearchShardTask task = null; // TODO: init task
        List<Object[]> rows = new ArrayList<>();
        try (DataFormatAwareEngine.DataFormatAwareReader reader = dataFormatAwareEngine.acquireReader()) {
            ExecutionContext ctx = new ExecutionContext(tableName, task, reader);
            try (SearchExecEngine<ExecutionContext, EngineResultStream> engine = createExecEngineFromPlugin(plugin, ctx)) {
                logger.info("[DefaultPlanExecutor] Executing via [{}]", plugin.name());
                try (EngineResultStream resultStream = engine.execute(ctx)) {
                    Iterator<EngineResultBatch> batchIterator = resultStream.iterator();
                    while (batchIterator.hasNext()) {
                        EngineResultBatch batch = batchIterator.next();
                        List<String> fieldNames = batch.getFieldNames();
                        for (int row = 0; row < batch.getRowCount(); row++) {
                            Object[] rowValues = new Object[fieldNames.size()];
                            for (int col = 0; col < fieldNames.size(); col++) {
                                rowValues[col] = batch.getFieldValue(fieldNames.get(col), row);
                            }
                            rows.add(rowValues);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Execution failed for [" + plugin.name() + "]", e);
        }
        return rows;
    }

    /**
     * Creates a full search execution engine from the plugin.
     * If the plugin implements {@link SearchExecEngineProvider}, delegates to it.
     * Otherwise falls back to creating a basic searcher via {@link SearchBackEndPlugin#createSearcher}.
     */
    @SuppressWarnings("unchecked")
    private static <R> SearchExecEngine<ExecutionContext, EngineResultStream> createExecEngineFromPlugin(
        SearchBackEndPlugin<R> plugin,
        ExecutionContext ctx
    ) {
        if (plugin instanceof SearchExecEngineProvider) {
            return ((SearchExecEngineProvider) plugin).createSearchExecEngine(ctx);
        }
        throw new UnsupportedOperationException(
            "Backend [" + plugin.name() + "] does not implement SearchExecEngineProvider"
        );
    }

    static String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan fragment");
    }

    private IndexShard resolveShard(String indexName) {
        IndexService indexService = indicesService.indexService(clusterService.state().metadata().index(indexName).getIndex());
        if (indexService == null) throw new IllegalStateException("Index [" + indexName + "] not on this node");
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) throw new IllegalStateException("No shards for [" + indexName + "]");
        return indexService.getShardOrNull(shardIds.iterator().next());
    }

    private SearchBackEndPlugin<?> selectBackEnd() {
        if (backEnds.isEmpty()) {
            logger.warn("No back-end plugins registered — queries will return empty results");
            return null;
        }
        // TODO: select based on data format available in the catalog snapshot
        return backEnds.values().iterator().next();
    }
}
