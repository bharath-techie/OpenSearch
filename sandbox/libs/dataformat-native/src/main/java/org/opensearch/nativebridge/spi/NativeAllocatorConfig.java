/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Dynamic jemalloc tuning via FFM.
 * <p>
 * Provides methods to adjust jemalloc's {@code background_thread}, {@code dirty_decay_ms} and
 * {@code muzzy_decay_ms} at runtime for all arenas. These are called by plugin-level cluster
 * settings listeners.
 * <p>
 * Note: {@code lg_tcache_max} is NOT dynamically tunable by jemalloc (init-time only).
 */
public final class NativeAllocatorConfig {

    private static final Logger logger = LogManager.getLogger(NativeAllocatorConfig.class);

    private static final MethodHandle SET_BACKGROUND_THREAD;
    private static final MethodHandle SET_DIRTY;
    private static final MethodHandle SET_MUZZY;
    private static final MethodHandle SET_OVERSIZE_THRESHOLD;

    static {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        FunctionDescriptor desc = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        SET_BACKGROUND_THREAD = linker.downcallHandle(lookup.find("native_jemalloc_set_background_thread").orElseThrow(), desc);
        SET_DIRTY = linker.downcallHandle(lookup.find("native_jemalloc_set_dirty_decay_ms").orElseThrow(), desc);
        SET_MUZZY = linker.downcallHandle(lookup.find("native_jemalloc_set_muzzy_decay_ms").orElseThrow(), desc);
        SET_OVERSIZE_THRESHOLD = linker
            .downcallHandle(lookup.find("native_jemalloc_set_oversize_threshold").orElseThrow(), desc);
    }

    private NativeAllocatorConfig() {}

    /**
     * Enables or disables jemalloc's internal background purge threads. No restart required.
     * <p>
     * When disabled (jemalloc's default) decay purging runs inline on whichever thread allocates,
     * which for the analytics path means a DataFusion worker mid-query. Enabling moves that work
     * to jemalloc's own threads, as recommended by jemalloc's TUNING.md for throughput-oriented
     * workloads.
     *
     * @param enabled whether jemalloc may run background purge threads
     */
    public static void setBackgroundThread(boolean enabled) {
        try {
            long rc = (long) SET_BACKGROUND_THREAD.invokeExact(enabled ? 1L : 0L);
            NativeLibraryLoader.checkResult(rc);
            logger.info("jemalloc background_thread updated to {}", enabled);
        } catch (Throwable t) {
            logger.warn("Error setting jemalloc background_thread", t);
        }
    }

    /**
     * Sets dirty_decay_ms for all jemalloc arenas. No restart required.
     *
     * @param ms decay time in milliseconds (-1 to disable decay)
     */
    public static void setDirtyDecayMs(long ms) {
        applyDecay(SET_DIRTY, "dirty_decay_ms", ms);
    }

    /**
     * Sets muzzy_decay_ms for all jemalloc arenas. No restart required.
     *
     * @param ms decay time in milliseconds (-1 to disable decay)
     */
    public static void setMuzzyDecayMs(long ms) {
        applyDecay(SET_MUZZY, "muzzy_decay_ms", ms);
    }

    /**
     * Sets jemalloc's oversize_threshold on all arenas. No restart required.
     * <p>
     * Freed allocations at or above this size are purged back to the OS immediately, bypassing
     * decay — the next query then re-faults and the kernel re-zeroes every page. DataFusion frees
     * its aggregation hash tables and sort buffers as single large allocations, so this threshold
     * must clear their doubling-growth sizes (measured up to ~512 MB on ClickBench URL group-bys)
     * or those queries pay ~100 ms+ of page-fault overhead each. 0 disables oversize routing.
     *
     * @param bytes threshold in bytes ({@code >= 0}; 0 disables the eager-purge routing)
     */
    public static void setOversizeThreshold(long bytes) {
        try {
            long rc = (long) SET_OVERSIZE_THRESHOLD.invokeExact(bytes);
            NativeLibraryLoader.checkResult(rc);
            logger.info("jemalloc oversize_threshold updated to {} bytes", bytes);
        } catch (Throwable t) {
            logger.warn("Error setting jemalloc oversize_threshold", t);
        }
    }

    private static void applyDecay(MethodHandle handle, String name, long ms) {
        try {
            long rc = (long) handle.invokeExact(ms);
            NativeLibraryLoader.checkResult(rc);
            logger.info("jemalloc {} updated to {}", name, ms);
        } catch (Throwable t) {
            logger.warn("Error setting jemalloc " + name, t);
        }
    }
}
