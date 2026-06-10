/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;


/**
 * Complex Joins PPL integration test (multi-index). Tests join operations across multiple indexes.
 *
 * <p>All indices are provisioned from the {@code complex_joins} dataset (see
 * {@link ComplexJoinsTestHelper}), which ships self-consistent fixtures whose join keys line up
 * across indices. We deliberately do NOT re-provision these indices from the shared single-index
 * datasets: that {@code DELETE}+recreate would replace the consistent join-key values with the
 * shared datasets' unrelated values, breaking the cross-index joins (Q4/Q8/Q9 → zero/incorrect
 * matches). This was the actual cause of the prior cross-table failures — a test-data provisioning
 * clobber, not an engine bug.
 */
public class ComplexJoinsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return ComplexJoinsTestHelper.DATASET;
    }

    public void testComplexJoinsPplQueries() throws Exception {
        runPplQueries();
    }
}
