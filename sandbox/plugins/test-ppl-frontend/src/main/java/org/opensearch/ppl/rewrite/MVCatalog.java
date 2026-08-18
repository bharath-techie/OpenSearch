/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.rewrite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Freshness-gated catalog of materialized views for transparent rewrite, read from the
 * index-management config index and cached briefly (rewrite sits on the query path; the
 * catalog changes on job CRUD / refresh cadence, not per query).
 *
 * <p>A view is eligible only when its ledger records a successful last refresh — the
 * staleness bound is then the job's refresh interval (plus detection lag). Failed or
 * never-refreshed views are invisible to the rewriter.
 */
public final class MVCatalog {

    private static final Logger logger = LogManager.getLogger(MVCatalog.class);

    static final String CONFIG_INDEX = ".opendistro-ism-config";
    private static final long CACHE_TTL_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final int MAX_VIEWS = 200;

    private final Client client;
    private final AtomicReference<List<MVQueryRewriter.ViewDef>> cached = new AtomicReference<>(List.of());
    private final AtomicLong cachedAt = new AtomicLong(0);

    public MVCatalog(Client client) {
        this.client = client;
    }

    /** Current eligible views; empty on any failure (rewrite silently disabled). */
    public List<MVQueryRewriter.ViewDef> eligibleViews() {
        long now = System.currentTimeMillis();
        if (now - cachedAt.get() < CACHE_TTL_MILLIS) {
            return cached.get();
        }
        try {
            SearchRequest request = new SearchRequest(CONFIG_INDEX).source(
                new SearchSourceBuilder().size(MAX_VIEWS)
                    .trackTotalHits(false)
                    .query(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("mv_job")))
            );
            SearchResponse response = client.search(request).actionGet(TimeUnit.SECONDS.toMillis(5));
            List<Map<String, Object>> docs = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                Object mvJob = hit.getSourceAsMap().get("mv_job");
                if (mvJob instanceof Map<?, ?> job && isFresh(job)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> typed = (Map<String, Object>) job;
                    docs.add(typed);
                }
            }
            List<MVQueryRewriter.ViewDef> views = MVQueryRewriter.toViewDefs(docs);
            cached.set(views);
            cachedAt.set(now);
            return views;
        } catch (Exception e) {
            logger.debug("MV catalog fetch failed; rewrite disabled for this window", e);
            cachedAt.set(now); // back off for a TTL rather than hammering a broken index
            cached.set(List.of());
            return List.of();
        }
    }

    private static boolean isFresh(Map<?, ?> job) {
        Object enabled = job.get("enabled");
        if (Boolean.FALSE.equals(enabled)) {
            return false;
        }
        Object lastRefresh = job.get("last_refresh");
        return lastRefresh instanceof Map<?, ?> lr && "success".equals(lr.get("status"));
    }
}
