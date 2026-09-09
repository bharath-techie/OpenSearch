/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.FastPathHintSpec;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ShardScanInstructionHandler#route} — the single decision that folds QTF row-ids,
 * delete filtering (#22910), and a planner fast-path shape into LISTING vs INDEXED. No FFM here;
 * the decision is a pure function of {@code requestsRowIds} + the context (deletions + hints, both
 * stamped by {@code AnalyticsSearchService}).
 */
public class ShardScanInstructionHandlerRouteTests extends OpenSearchTestCase {

    private static ShardScanExecutionContext context(boolean hasDeletedDocs, FastPathHintSpec hints) {
        ShardScanExecutionContext context = mock(ShardScanExecutionContext.class);
        when(context.hasDeletedDocs()).thenReturn(hasDeletedDocs);
        when(context.getFastPathHints()).thenReturn(hints);
        return context;
    }

    private static FastPathHintSpec countOnly() {
        return FastPathHintSpec.countOnly(true, FastPathHintSpec.RangeUnit.MILLIS, 1000L, 2000L);
    }

    public void testPlainScanRoutesListing() {
        assertEquals(
            ShardScanInstructionHandler.ScanRoute.LISTING,
            ShardScanInstructionHandler.route(context(false, FastPathHintSpec.NONE), false)
        );
    }

    public void testRowIdsRoutesIndexed() {
        assertEquals(
            ShardScanInstructionHandler.ScanRoute.INDEXED,
            ShardScanInstructionHandler.route(context(false, FastPathHintSpec.NONE), true)
        );
    }

    public void testDeletesRouteIndexed() {
        assertEquals(
            ShardScanInstructionHandler.ScanRoute.INDEXED,
            ShardScanInstructionHandler.route(context(true, FastPathHintSpec.NONE), false)
        );
    }

    public void testCountOnlyShapeRoutesIndexed() {
        assertEquals(ShardScanInstructionHandler.ScanRoute.INDEXED, ShardScanInstructionHandler.route(context(false, countOnly()), false));
    }

    /** A range-only hint with no recognised shape does NOT by itself force the indexed path. */
    public void testRangeOnlyHintDoesNotRouteIndexed() {
        FastPathHintSpec rangeOnly = FastPathHintSpec.rangeOnly(true, FastPathHintSpec.RangeUnit.MILLIS, 1000L, 2000L);
        assertEquals(ShardScanInstructionHandler.ScanRoute.LISTING, ShardScanInstructionHandler.route(context(false, rangeOnly), false));
    }
}
