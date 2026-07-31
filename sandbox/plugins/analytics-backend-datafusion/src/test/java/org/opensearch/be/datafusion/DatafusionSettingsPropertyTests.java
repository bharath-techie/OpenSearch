/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class DatafusionSettingsPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    private ClusterSettings createClusterSettings() {
        Set<Setting<?>> settingsSet = new HashSet<>(DatafusionSettings.ALL_SETTINGS);
        settingsSet.add(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING);
        settingsSet.add(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE);
        return new ClusterSettings(Settings.EMPTY, settingsSet);
    }

    public void testSnapshotUpdateConsistencyProperty() {
        for (int i = 0; i < ITERATIONS; i++) {
            DatafusionSettings datafusionSettings = new DatafusionSettings(Settings.EMPTY);
            ClusterSettings clusterSettings = createClusterSettings();
            datafusionSettings.registerListeners(clusterSettings);

            WireConfigSnapshot before = datafusionSettings.getSnapshot();

            int settingIndex = randomIntBetween(0, 3);
            Settings newSettings;

            switch (settingIndex) {
                case 0: // batch_size
                    int newBatchSize = randomIntBetween(1, 1_000_000);
                    newSettings = Settings.builder().put("datafusion.batch_size", newBatchSize).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterBatch = datafusionSettings.getSnapshot();
                    assertEquals(newBatchSize, afterBatch.batchSize());
                    assertEquals(before.targetPartitions(), afterBatch.targetPartitions());
                    assertEquals(before.listingTablePushdownFilters(), afterBatch.listingTablePushdownFilters());
                    assertEquals(before.indexedPushdownFilters(), afterBatch.indexedPushdownFilters());
                    break;

                case 1: // listing_table.pushdown_filters
                    boolean newPushdown = before.listingTablePushdownFilters() == false;
                    newSettings = Settings.builder().put("datafusion.listing_table.pushdown_filters", newPushdown).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterPushdown = datafusionSettings.getSnapshot();
                    assertEquals(newPushdown, afterPushdown.listingTablePushdownFilters());
                    assertEquals(before.batchSize(), afterPushdown.batchSize());
                    assertEquals(before.targetPartitions(), afterPushdown.targetPartitions());
                    assertEquals(before.indexedPushdownFilters(), afterPushdown.indexedPushdownFilters());
                    break;

                case 2: // max_slice_count
                    int newSliceCount = randomIntBetween(1, 32);
                    newSettings = Settings.builder().put("search.concurrent.max_slice_count", newSliceCount).build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterSlice = datafusionSettings.getSnapshot();
                    assertEquals(Math.min(newSliceCount, Runtime.getRuntime().availableProcessors()), afterSlice.targetPartitions());
                    assertEquals(before.batchSize(), afterSlice.batchSize());
                    assertEquals(before.listingTablePushdownFilters(), afterSlice.listingTablePushdownFilters());
                    assertEquals(before.indexedPushdownFilters(), afterSlice.indexedPushdownFilters());
                    break;

                case 3: // concurrent_search_mode
                    newSettings = Settings.builder().put("search.concurrent_segment_search.mode", "none").build();
                    clusterSettings.applySettings(newSettings);
                    WireConfigSnapshot afterMode = datafusionSettings.getSnapshot();
                    assertEquals(1, afterMode.targetPartitions());
                    assertEquals(before.batchSize(), afterMode.batchSize());
                    assertEquals(before.listingTablePushdownFilters(), afterMode.listingTablePushdownFilters());
                    assertEquals(before.indexedPushdownFilters(), afterMode.indexedPushdownFilters());
                    break;

                default:
                    fail("Unexpected setting index: " + settingIndex);
            }
        }
    }

    public void testSequentialUpdatesAccumulateCorrectly() {
        for (int i = 0; i < ITERATIONS; i++) {
            DatafusionSettings datafusionSettings = new DatafusionSettings(Settings.EMPTY);
            ClusterSettings clusterSettings = createClusterSettings();
            datafusionSettings.registerListeners(clusterSettings);

            int newBatchSize = randomIntBetween(1, 1_000_000);
            boolean newIndexedPushdown = randomBoolean();

            clusterSettings.applySettings(
                Settings.builder()
                    .put("datafusion.batch_size", newBatchSize)
                    .put("datafusion.indexed.pushdown_filters", newIndexedPushdown)
                    .build()
            );

            WireConfigSnapshot finalSnapshot = datafusionSettings.getSnapshot();

            assertEquals(newBatchSize, finalSnapshot.batchSize());
            assertEquals(newIndexedPushdown, finalSnapshot.indexedPushdownFilters());
            // Untouched settings keep their defaults.
            assertEquals(false, finalSnapshot.listingTablePushdownFilters());
        }
    }
}
