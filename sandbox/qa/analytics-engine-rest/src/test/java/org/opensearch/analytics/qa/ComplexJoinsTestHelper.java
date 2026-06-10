/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Helper for Complex Joins testing dataset configuration (multi-index).
 *
 * <p>The {@code complex_joins} dataset ships its OWN {@code bulk_*.json} for every index
 * it joins, with join-key values that line up across indices (e.g. {@code performance_metrics.host}
 * = {@code pod-0..N} matching {@code kubernetes_logs.kubernetes.pod_name}; {@code event_processor.cluster_name}
 * = {@code cluster-a/b/c} matching {@code kubernetes_logs.cluster.name}). Every index a query
 * references must be listed here so it is provisioned from this consistent fixture — do NOT
 * re-provision these indices from the shared single-index datasets, which carry different
 * join-key values and would break the cross-index joins (Q4/Q8/Q9).
 */
public final class ComplexJoinsTestHelper {

    private ComplexJoinsTestHelper() {
        // utility class
    }

    public static final Dataset DATASET = new Dataset(
        "complex_joins",
        "security_logs",
        "app_monitor",
        "kubernetes_logs",
        "monitor_tracking",
        "performance_metrics",
        "voice_verification",
        "event_processor",
        "tax_withholding"
    );
}
