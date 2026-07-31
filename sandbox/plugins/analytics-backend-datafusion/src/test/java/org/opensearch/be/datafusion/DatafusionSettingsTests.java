/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

public class DatafusionSettingsTests extends OpenSearchTestCase {

    private static final int DEFAULT_PARALLELISM = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors() / 2, 4));

    public void testBatchSizeSettingDefinition() {
        assertEquals("datafusion.batch_size", DatafusionSettings.BATCH_SIZE.getKey());
        assertEquals(Integer.valueOf(8192), DatafusionSettings.BATCH_SIZE.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.BATCH_SIZE.isDynamic());
        assertTrue(DatafusionSettings.BATCH_SIZE.hasNodeScope());
    }

    public void testListingTablePushdownFiltersSettingDefinition() {
        assertEquals("datafusion.listing_table.pushdown_filters", DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.getKey());
        assertEquals(Boolean.FALSE, DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.isDynamic());
        assertTrue(DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS.hasNodeScope());
    }

    public void testIndexedPushdownFiltersSettingDefinition() {
        assertEquals("datafusion.indexed.pushdown_filters", DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.getKey());
        assertEquals(Boolean.TRUE, DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.get(Settings.EMPTY));
        assertTrue(DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.isDynamic());
        assertTrue(DatafusionSettings.INDEXED_PUSHDOWN_FILTERS.hasNodeScope());
    }

    public void testAllSettingsContainsAllExpectedSettings() {
        assertEquals(32, DatafusionSettings.ALL_SETTINGS.size());
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DataFusionPlugin.DATAFUSION_REDUCE_TARGET_PARTITIONS));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DataFusionPlugin.DATAFUSION_MEMORY_GUARD_SPILL_EXEMPT_CAP));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.BATCH_SIZE));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.LISTING_TABLE_PUSHDOWN_FILTERS));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_PUSHDOWN_FILTERS));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_FORCE_STRATEGY));
        assertTrue(DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.INDEXED_DECODE_TIME_REFINEMENT));
    }

    public void testForceStrategyMapsToWireCodes() {
        assertEquals(-1, DatafusionSettings.forceStrategyToWire("none"));
        assertEquals(0, DatafusionSettings.forceStrategyToWire("row_selection"));
        assertEquals(1, DatafusionSettings.forceStrategyToWire("boolean_mask"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.forceStrategyToWire("bogus"));
        assertTrue(e.getMessage().contains("must be one of [none, row_selection, boolean_mask]"));
    }

    public void testGranularitySettingDefinitions() {
        assertEquals(Integer.valueOf(1024), DatafusionSettings.INDEXED_MIN_SKIP_RUN_DEFAULT.get(Settings.EMPTY));
        assertEquals(0.03, DatafusionSettings.INDEXED_MIN_SKIP_RUN_SELECTIVITY_THRESHOLD.get(Settings.EMPTY), 0.0);
        assertEquals("none", DatafusionSettings.INDEXED_FORCE_STRATEGY.get(Settings.EMPTY));
        // Decode-time refinement costs a second decode pass, so it is off by default.
        assertEquals(Boolean.FALSE, DatafusionSettings.INDEXED_DECODE_TIME_REFINEMENT.get(Settings.EMPTY));
    }

    public void testDefaultSnapshotValuesMatchDefaults() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);
        WireConfigSnapshot snapshot = ds.getSnapshot();

        assertEquals(8192, snapshot.batchSize());
        assertEquals(false, snapshot.listingTablePushdownFilters());
        assertEquals(DEFAULT_PARALLELISM, snapshot.targetPartitions());
        assertTrue(snapshot.indexedPushdownFilters());
        assertEquals(1024, snapshot.minSkipRunDefault());
        assertEquals(0.03, snapshot.minSkipRunSelectivityThreshold(), 0.0);
        assertEquals(WireConfigSnapshot.FORCE_STRATEGY_NONE, snapshot.forceStrategy());
        assertFalse(snapshot.indexedDecodeTimeRefinement());
    }

    public void testTargetPartitionsPassthroughWhenNonZero() {
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 8)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);

        assertEquals(Math.min(8, Runtime.getRuntime().availableProcessors()), ds.getSnapshot().targetPartitions());
    }

    public void testTargetPartitionsFallbackWhenZero() {
        DatafusionSettings ds = new DatafusionSettings(Settings.EMPTY);

        assertEquals(DEFAULT_PARALLELISM, ds.getSnapshot().targetPartitions());
    }

    public void testTargetPartitionsForcedToOneWhenModeNone() {
        Settings settings = Settings.builder()
            .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "none")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), 16)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);

        assertEquals(1, ds.getSnapshot().targetPartitions());
    }

    public void testTargetPartitionsCappedAtAvailableProcessors() {
        int processors = Runtime.getRuntime().availableProcessors();
        Settings settings = Settings.builder()
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), processors + 10)
            .build();
        DatafusionSettings ds = new DatafusionSettings(settings);

        assertEquals(processors, ds.getSnapshot().targetPartitions());
    }

    public void testBatchSizeZeroIsRejected() {
        Settings settings = Settings.builder().put("datafusion.batch_size", 0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.BATCH_SIZE.get(settings));
    }

}
