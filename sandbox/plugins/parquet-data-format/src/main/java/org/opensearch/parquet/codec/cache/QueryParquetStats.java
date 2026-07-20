/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.cache;

import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Query-scoped accumulator that sums per-column {@link CacheStats} across every segment touched by
 * a single search, so the Parquet read-path cost can be summarized in one log line per query.
 *
 * <p>One instance is created per search (by {@code ParquetDocValuesDirectoryReader}) and shared by
 * all the per-segment {@code ParquetColumnReader}s that search opens. Each column reader
 * {@link #register(CacheStats) registers} its {@link CacheStats} when it is opened; the values are
 * summed <b>live</b> at {@link #summary()} time. Registering at open (rather than merging at close)
 * means the roll-up does not depend on reader-close ordering — by the time the per-query summary is
 * produced (end of search), every registered reader has finished collecting, so the live sums are
 * final. The registry is a {@link ConcurrentLinkedQueue} so concurrent segment slices can register
 * safely, and each reader mutates only its own {@link CacheStats} during collection.
 */
public final class QueryParquetStats {

    private final ConcurrentLinkedQueue<CacheStats> registered = new ConcurrentLinkedQueue<>();

    // Liquid-cache event counters are process-wide and monotonic. We snapshot them at query start
    // (only when the stats summary is enabled — the snapshot itself is an FFM crossing) and report
    // the delta in the summary. Meaningful for the single-query-at-a-time benchmark case; under
    // concurrent queries the delta is an over-count (other queries' liquid events land in the window),
    // which is acceptable for a diagnostic. -1 baseline means "not captured" → liquid line suppressed.
    private long liquidHitsBase = -1;
    private long liquidMissesBase = -1;
    private long liquidPutsBase = -1;

    /**
     * Records the liquid counter baseline at query start so {@link #summary()} can report per-query
     * deltas. Call only when the summary will actually be emitted (this reads native counters over
     * FFM); skipping it leaves the liquid line out of the summary at zero cost.
     */
    public void captureLiquidBaseline(long hits, long misses, long puts) {
        this.liquidHitsBase = hits;
        this.liquidMissesBase = misses;
        this.liquidPutsBase = puts;
    }

    /** Registers a column reader's stats; its counters are summed live when {@link #summary()} runs. */
    public void register(CacheStats s) {
        if (s != null) {
            registered.add(s);
        }
    }

    /** True when nothing was recorded (used to suppress an empty summary). */
    public boolean isEmpty() {
        return registered.isEmpty();
    }

    /**
     * A single-line, human-readable per-query summary. The {@code liquid*Now} arguments are the
     * current process-wide liquid counters (read by the caller over FFM); the reported liquid line
     * is their delta from the baseline captured at query start. Pass any value when no baseline was
     * captured — the liquid line is suppressed unless a baseline exists.
     */
    public String summary(long liquidHitsNow, long liquidMissesNow, long liquidPutsNow) {
        long columns = 0;
        long jumpTableLookups = 0, allNullSkips = 0;
        long pageDecodes = 0, slowValueReads = 0, slowRepeatedReads = 0;
        for (CacheStats s : registered) {
            columns++;
            jumpTableLookups += s.pageIndexLookups();
            allNullSkips += s.allNullPageSkips();
            pageDecodes += s.pageDecodes();
            slowValueReads += s.slowValueReads();
            slowRepeatedReads += s.slowRepeatedReads();
        }

        // Liquid line: per-query deltas from the baseline captured at query start. Suppressed when
        // no baseline was taken (stats disabled path). hits = pages served from liquid, decoded =
        // liquid misses that fell through to a Parquet decode, puts = pages inserted into liquid.
        String liquidLine = "";
        if (liquidHitsBase >= 0) {
            long lh = liquidHitsNow - liquidHitsBase;
            long lm = liquidMissesNow - liquidMissesBase;
            long lp = liquidPutsNow - liquidPutsBase;
            long lget = lh + lm;
            double lHitRate = lget == 0 ? 0.0 : (double) lh / lget * 100.0;
            liquidLine = String.format(
                Locale.ROOT,
                " | liquid: hits=%d decoded=%d puts=%d (liquidHitRate=%.2f%%)",
                lh,
                lm,
                lp,
                lHitRate
            );
        }
        return String.format(
            Locale.ROOT,
            "segments/columns=%d | L3 jumpTableLookups=%d | L4 allNullSkips=%d | "
                + "FFM: pageDecodes=%d slowValueReads=%d slowRepeatedReads=%d%s",
            columns,
            jumpTableLookups,
            allNullSkips,
            pageDecodes,
            slowValueReads,
            slowRepeatedReads,
            liquidLine
        );
    }
}
