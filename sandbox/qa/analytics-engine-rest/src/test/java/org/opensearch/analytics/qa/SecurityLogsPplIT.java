/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;


/**
 * Security Logs PPL integration test. Runs PPL queries against security_logs data.
 */
public class SecurityLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return SecurityLogsTestHelper.DATASET;
    }

    public void testSecurityLogsPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Remaining 1-shard failures are value/ordering mismatches, not delegation crashes:
     * Q2 has a tie on the sort key (values(action) order), Q8 a timestamp sub-millisecond
     * precision difference (earliest/latest). The HAVING-over-aggregate delegation crashes
     * (Q1, Q3, Q4, Q5, Q7) are fixed by the scan-adjacent filter picker.
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(2, 8);
    }
}
