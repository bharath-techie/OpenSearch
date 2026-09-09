/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FastPathHintsInstructionNode;
import org.opensearch.analytics.spi.FragmentInstructionHandler;

/**
 * Pass-through handler for the {@code FAST_PATH_HINTS} instruction. The hints were already stamped
 * onto the {@code ShardScanExecutionContext} by {@code AnalyticsSearchService} (mirrors how the
 * partial-aggregate flag is stamped), so this handler has nothing to configure — it just returns the
 * backend context unchanged. The shard-scan handler reads the hints from the context, not from here.
 */
public class FastPathHintsInstructionHandler implements FragmentInstructionHandler<FastPathHintsInstructionNode> {

    @Override
    public BackendExecutionContext apply(
        FastPathHintsInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        return backendContext;
    }
}
