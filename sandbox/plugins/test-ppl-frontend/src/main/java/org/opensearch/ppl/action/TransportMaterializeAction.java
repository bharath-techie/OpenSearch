/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.sink.StreamingIndexSink;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;

/**
 * Executes a PPL query on the analytics engine and streams its result into a target index.
 *
 * <p>Flow: plan PPL text → {@link DefaultPlanExecutor#executeStreaming} with a
 * {@link StreamingIndexSink} as the terminal sink (Arrow batches bulk-written as they
 * arrive, bounded in-flight, released immediately) → on query success,
 * {@link StreamingIndexSink#finish} awaits the tail flush and all in-flight bulks →
 * respond with write stats. On query failure the sink is aborted and buffered docs dropped.
 *
 * <p>This is the execution seam that scheduled materialization jobs (rollup/transform
 * evolution, async materialized-view refresh in index-management) call into by action name.
 *
 * <p><b>Cross-plugin dispatch.</b> Declared over {@link ActionRequest} rather than
 * {@link MaterializeRequest}: orchestrating plugins invoke this action with their own
 * wire-compatible request class, and local (same-JVM) dispatch passes that object through
 * by reference — no serialization happens, so a typed signature would throw
 * {@code ClassCastException}. Foreign requests are converted via an explicit
 * serialization round-trip, which is exactly the wire contract.
 */
public class TransportMaterializeAction extends HandledTransportAction<ActionRequest, MaterializeResponse> {

    private static final Logger logger = LogManager.getLogger(TransportMaterializeAction.class);

    private final UnifiedQueryService unifiedQueryService;
    private final DefaultPlanExecutor planExecutor;
    private final Client client;
    private final ThreadPool threadPool;

    @Inject
    public TransportMaterializeAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContextProvider contextProvider,
        DefaultPlanExecutor planExecutor,
        Client client,
        ThreadPool threadPool
    ) {
        super(MaterializeAction.NAME, transportService, actionFilters, MaterializeRequest::new);
        this.unifiedQueryService = new UnifiedQueryService(planExecutor, contextProvider);
        this.planExecutor = planExecutor;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ActionRequest rawRequest, ActionListener<MaterializeResponse> listener) {
        final MaterializeRequest request;
        try {
            request = asMaterializeRequest(rawRequest);
        } catch (Exception e) {
            listener.onFailure(
                new IllegalArgumentException("request is not wire-compatible with " + MaterializeRequest.class.getName(), e)
            );
            return;
        }
        final long startNanos = System.nanoTime();
        // Fork planning off the transport thread; execution itself is asynchronous.
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            final StreamingIndexSink sink;
            try {
                UnifiedQueryService.PlannedQuery planned = unifiedQueryService.plan(request.getPplQuery());
                sink = new StreamingIndexSink(client, request.getTargetIndex(), request.getKeyColumns());
                planExecutor.executeStreaming(
                    planned.plan(),
                    planned.queryCtx(),
                    sink,
                    request.isEmitStates(),
                    ActionListener.wrap(ignored ->
                // Query terminal reached: every batch has been fed. Await the sink's
                // tail flush + in-flight bulks, then report write stats.
                sink.finish(ActionListener.wrap(stats -> {
                    long tookMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    listener.onResponse(new MaterializeResponse(stats.rowsReceived(), stats.docsIndexed(), stats.bulksSent(), tookMillis));
                }, listener::onFailure)), e -> {
                    logger.warn("[materialize] query execution failed for target [{}]", request.getTargetIndex(), e);
                    sink.abort();
                    listener.onFailure(e);
                })
                );
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Accepts the local {@link MaterializeRequest} directly; any other class (an
     * orchestrator's wire-compatible duplicate) is converted through a stream round-trip —
     * the same bytes it would produce over the wire. Package-private for tests.
     */
    static MaterializeRequest asMaterializeRequest(ActionRequest raw) throws IOException {
        if (raw instanceof MaterializeRequest typed) {
            return typed;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            raw.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new MaterializeRequest(in);
            }
        }
    }
}
