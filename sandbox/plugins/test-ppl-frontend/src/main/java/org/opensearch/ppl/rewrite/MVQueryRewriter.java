/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.rewrite;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Transparent materialized-view matching (first cut: exact definition match).
 *
 * <p>An incoming PPL query whose normalized text equals a fresh view's definition is
 * answered from the view's stored partial aggregate states: this class only performs the
 * <em>match</em>; the actual rewrite happens in the analytics engine
 * ({@code MVStateReadRewriter}), which replaces the planned query's PARTIAL aggregate
 * subtree with a scan of the view's state columns — driven by the view's
 * {@code index.parquet.mv.spec}, never by parsing PPL text.
 *
 * <p>Freshness gating is the caller's responsibility ({@link MVCatalog} only offers views
 * whose last refresh succeeded); the staleness bound is the view's refresh interval.
 */
public final class MVQueryRewriter {

    /** A registered view eligible for matching. */
    public record ViewDef(String view, String query, boolean partitioned) {
    }

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private MVQueryRewriter() {}

    /**
     * Returns the first fresh view whose definition matches {@code pplText} exactly
     * (whitespace-normalized), or null.
     */
    public static ViewDef match(String pplText, List<ViewDef> views) {
        String normalizedQuery = normalize(pplText);
        for (ViewDef def : views) {
            if (normalizedQuery.equals(normalize(def.query()))) {
                return def;
            }
        }
        return null;
    }

    /** Whitespace-collapsed, trimmed, case-preserving normalization for definition equality. */
    static String normalize(String query) {
        return WHITESPACE.matcher(query.trim()).replaceAll(" ");
    }

    /** Extracts eligible views from raw mv_job documents (already freshness-filtered). */
    public static List<ViewDef> toViewDefs(List<Map<String, Object>> mvJobDocs) {
        List<ViewDef> defs = new ArrayList<>(mvJobDocs.size());
        for (Map<String, Object> doc : mvJobDocs) {
            Object view = doc.get("view");
            Object query = doc.get("query");
            if (view instanceof String v && query instanceof String q) {
                defs.add(new ViewDef(v, q, doc.get("partition_by") != null));
            }
        }
        return defs;
    }
}
