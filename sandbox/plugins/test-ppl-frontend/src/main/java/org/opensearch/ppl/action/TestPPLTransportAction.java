/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.MVReadTarget;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.rewrite.MVCatalog;
import org.opensearch.ppl.rewrite.MVQueryRewriter;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;

/**
 * Transport action that coordinates PPL query execution.
 *
 * <p>Receives {@link EngineContextProvider} and {@link QueryPlanExecutor} from the analytics-engine
 * plugin via Guice injection (enabled by {@code extendedPlugins = ['analytics-engine']}).
 *
 * <p>Execution is forked to the {@link ThreadPool.Names#SEARCH} thread pool to avoid
 * blocking the transport thread (DefaultPlanExecutor uses a blocking future internally).
 *
 * <p>Declared over {@link ActionRequest} so orchestrating plugins (e.g. index-management
 * materialized-view change detection) can invoke it by action name with their own
 * wire-compatible request class; local dispatch passes objects by reference, so foreign
 * classes are converted through a stream round-trip.
 */
public class TestPPLTransportAction extends HandledTransportAction<ActionRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(TestPPLTransportAction.class);

    private final UnifiedQueryService unifiedQueryService;
    private final ThreadPool threadPool;
    private final MVCatalog mvCatalog;
    private final Client client;
    private final ClusterService clusterService;
    private volatile boolean rewriteEnabled;

    @Inject
    public TestPPLTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContextProvider contextProvider,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService
    ) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, PPLRequest::new);
        this.unifiedQueryService = new UnifiedQueryService(executor, contextProvider);
        this.threadPool = threadPool;
        this.mvCatalog = new MVCatalog(client);
        this.client = client;
        this.clusterService = clusterService;
        this.rewriteEnabled = TestPPLPlugin.MV_REWRITE_ENABLED.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TestPPLPlugin.MV_REWRITE_ENABLED, v -> rewriteEnabled = v);
    }

    /** Test-only constructor that accepts a pre-built {@link UnifiedQueryService}. */
    public TestPPLTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        UnifiedQueryService unifiedQueryService,
        ThreadPool threadPool
    ) {
        super(UnifiedPPLExecuteAction.NAME, transportService, actionFilters, PPLRequest::new);
        this.unifiedQueryService = unifiedQueryService;
        this.threadPool = threadPool;
        this.mvCatalog = null;
        this.client = null;
        this.clusterService = null;
        this.rewriteEnabled = false;
    }

    @Override
    protected void doExecute(Task task, ActionRequest rawRequest, ActionListener<PPLResponse> listener) {
        final PPLRequest request;
        try {
            request = asPPLRequest(rawRequest);
        } catch (Exception e) {
            listener.onFailure(new IllegalArgumentException("request is not wire-compatible with " + PPLRequest.class.getName(), e));
            return;
        }
        // Fork to SEARCH thread pool — DefaultPlanExecutor.execute() blocks on a future
        // internally, which is forbidden on the transport thread.
        // TODO: update UnifiedQueryService to consume a listener that DefaultPlanExecutor does to avoid threadpool fork
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            try {
                String pplText = request.getPplText();
                // Transparent MV read: an exact-definition match (freshness-gated via the
                // ledger) is answered from the view's stored partial aggregate states.
                // The match happens here; the plan-level rewrite happens in the engine
                // (MVStateReadRewriter), driven by the view's index.parquet.mv.spec.
                MVReadTarget mvReadTarget = null;
                if (rewriteEnabled && mvCatalog != null && request.isExplain() == false) {
                    MVQueryRewriter.ViewDef matched = MVQueryRewriter.match(pplText, mvCatalog.eligibleViews());
                    if (matched != null) {
                        String spec = viewSpec(matched.view());
                        if (spec != null && spec.isEmpty() == false) {
                            mvReadTarget = new MVReadTarget(matched.view(), spec);
                            logger.info("[MV-READ] matched view [{}] for query", matched.view());
                        }
                    }
                }
                PPLResponse response = request.isExplain()
                    ? unifiedQueryService.executeWithProfile(pplText)
                    : unifiedQueryService.execute(pplText, mvReadTarget);
                listener.onResponse(response);
            } catch (Exception e) {
                logger.error("[UNIFIED_PPL] execution failed", e);
                listener.onFailure(e);
            }
        });
    }

    /** The view's {@code index.parquet.mv.spec}, from live index metadata; null when absent. */
    private String viewSpec(String viewIndex) {
        try {
            org.opensearch.cluster.metadata.IndexMetadata index = clusterService.state().metadata().index(viewIndex);
            return index == null ? null : index.getSettings().get("index.parquet.mv.spec");
        } catch (Exception e) {
            logger.debug("[MV-READ] spec lookup for [{}] failed", viewIndex, e);
            return null;
        }
    }

    /** Local requests pass through; wire-compatible duplicates convert via stream round-trip. */
    static PPLRequest asPPLRequest(ActionRequest raw) throws IOException {
        if (raw instanceof PPLRequest typed) {
            return typed;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            raw.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new PPLRequest(in);
            }
        }
    }
}
