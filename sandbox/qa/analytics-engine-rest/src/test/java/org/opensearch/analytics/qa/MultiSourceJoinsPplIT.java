/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;


/**
 * Complex Redesigned (multi-index) PPL integration test. Runs PPL queries against complex_redesigned data.
 */
public class MultiSourceJoinsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return MultiSourceJoinsTestHelper.DATASET;
    }

    public void testMultiSourceJoinsPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Remaining 1-shard failure is a value mismatch, not a delegation crash: Q2 has a tie on
     * the sort key (dedup ordering of part_mid affects the unique_users count). The dedup +
     * delegated-WHERE crash (Q4) is fixed by the scan-adjacent filter picker.
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(2);
    }
}
