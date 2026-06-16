/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for the parquet caches: the {@code CustomCacheManager}-owned metadata
 * (footer) cache and column-statistics cache, plus the two process-global scoped
 * page-index caches — the predicate-driven {@code column_index_cache} (keyed per
 * {@code (file, col, rg)} cell) and the projection-driven {@code offset_index_cache}
 * (keyed per {@code (file, col)} cell).
 *
 * <p>Each sub-group reports five fields. Disabled caches surface as all-zero
 * groups (in particular {@code size_limit_bytes == 0}); the JSON shape stays
 * symmetric so clients can render the response uniformly.
 */
public class CacheStats implements Writeable, ToXContentFragment {

    private final CacheGroupStats metadataCache;
    private final CacheGroupStats statisticsCache;
    private final CacheGroupStats columnIndexCache;
    private final CacheGroupStats offsetIndexCache;

    /**
     * Construct from individual sub-group stats.
     *
     * @param metadataCache     metadata cache counters (must not be null)
     * @param statisticsCache   statistics cache counters (must not be null)
     * @param columnIndexCache  scoped ColumnIndex cache counters (must not be null)
     * @param offsetIndexCache  scoped OffsetIndex cache counters (must not be null)
     */
    public CacheStats(
        CacheGroupStats metadataCache,
        CacheGroupStats statisticsCache,
        CacheGroupStats columnIndexCache,
        CacheGroupStats offsetIndexCache
    ) {
        this.metadataCache = Objects.requireNonNull(metadataCache);
        this.statisticsCache = Objects.requireNonNull(statisticsCache);
        this.columnIndexCache = Objects.requireNonNull(columnIndexCache);
        this.offsetIndexCache = Objects.requireNonNull(offsetIndexCache);
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public CacheStats(StreamInput in) throws IOException {
        this.metadataCache = new CacheGroupStats(in);
        this.statisticsCache = new CacheGroupStats(in);
        this.columnIndexCache = new CacheGroupStats(in);
        this.offsetIndexCache = new CacheGroupStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        metadataCache.writeTo(out);
        statisticsCache.writeTo(out);
        columnIndexCache.writeTo(out);
        offsetIndexCache.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("cache_stats");
        builder.startObject("metadata_cache");
        metadataCache.toXContent(builder);
        builder.endObject();
        builder.startObject("statistics_cache");
        statisticsCache.toXContent(builder);
        builder.endObject();
        builder.startObject("column_index_cache");
        columnIndexCache.toXContent(builder);
        builder.endObject();
        builder.startObject("offset_index_cache");
        offsetIndexCache.toXContent(builder);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /** Returns the metadata cache counters. */
    public CacheGroupStats getMetadataCache() {
        return metadataCache;
    }

    /** Returns the statistics cache counters. */
    public CacheGroupStats getStatisticsCache() {
        return statisticsCache;
    }

    /** Returns the scoped ColumnIndex cache counters. */
    public CacheGroupStats getColumnIndexCache() {
        return columnIndexCache;
    }

    /** Returns the scoped OffsetIndex cache counters. */
    public CacheGroupStats getOffsetIndexCache() {
        return offsetIndexCache;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheStats that = (CacheStats) o;
        return Objects.equals(metadataCache, that.metadataCache)
            && Objects.equals(statisticsCache, that.statisticsCache)
            && Objects.equals(columnIndexCache, that.columnIndexCache)
            && Objects.equals(offsetIndexCache, that.offsetIndexCache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataCache, statisticsCache, columnIndexCache, offsetIndexCache);
    }
}
