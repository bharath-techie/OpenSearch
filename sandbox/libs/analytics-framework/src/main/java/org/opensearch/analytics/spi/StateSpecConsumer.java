/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Implemented by terminal sinks that receive materialized-view state batches.
 *
 * <p>When a reduce stage is prepared in state-emitting mode, the backend derives a
 * describe document from the prepared plan — the merge spec ({@code key_columns} +
 * {@code aggs} with function names and input types) and the state-column schema —
 * and hands it to the downstream sink through this interface <em>before</em> any
 * batch flows. The sink uses it to provision the target index (state-column
 * mappings, {@code index.parquet.mv.spec}) so the layout on disk can never drift
 * from what the plan writes.
 *
 * @opensearch.internal
 */
public interface StateSpecConsumer {

    /**
     * Delivers the state-emission describe JSON derived from the prepared reduce plan.
     * Called during stage setup, strictly before the first batch is fed.
     */
    void onStateSpec(String describeJson);
}
