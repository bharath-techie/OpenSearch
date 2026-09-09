/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.spi.FastPathHintSpec;
import org.opensearch.common.annotation.ExperimentalApi;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Planner fast-path hints — the fixed-layout, versioned wire contract the Java
 * planner ships once per shard fragment to the native indexed executor.
 *
 * <p>The planner decides the plan shape exactly once (see the fast-path hint
 * extractor) and serializes it here; Rust reads it back
 * ({@code fast_path_hints::FastPathHints::from_ffm_ptr}) and makes exactly one
 * per-row-group decision from footer stats + these hints + {@code has_deletes}.
 * No plan-shape sniffing remains in Rust.
 *
 * <p>Layout is little-endian, 8-byte aligned, and pinned by a byte-layout test
 * on BOTH sides ({@link #BYTE_SIZE} must equal the Rust
 * {@code FASTPATHHINTS_BYTE_SIZE}).
 *
 * <pre>
 * Offset  Size  Field                     Notes
 * ──────  ────  ────────────────────────  ─────────────────────────────────────
 * 0       1     version (=1)
 * 1       1     shape                     0 NONE, 1 COUNT_ONLY, 2 TOPK*, 3 HISTOGRAM*
 * 2       1     flags                     bit0 range_on_leading_sort_field,
 *                                         bit1 range_conjunct_strippable,
 *                                         bit2 topk_keep_last*, bit3 topk_path_safe*
 * 3       1     range_unit                0 none, 1 seconds, 2 millis, 3 micros, 4 nanos
 * 4       4     topk_budget*              reserved (0)
 * 8       8     range_lower_inclusive     absent = Long.MIN_VALUE
 * 16      8     range_upper_inclusive     absent = Long.MAX_VALUE
 * 24      1     histogram_bucket_op*      reserved (0)
 * 25      7     (pad)
 * 32      8     histogram_bucket_operand* reserved (0)
 * ──────  ────
 * Total: 40 bytes                         (* = populated by PR3)
 * </pre>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record FastPathHints(int version, int shape, int flags, int rangeUnit, int topkBudget, long rangeLowerInclusive,
    long rangeUpperInclusive, int histogramBucketOp, long histogramBucketOperand) {

    /** Total byte size of the wire struct. Must match Rust {@code FASTPATHHINTS_BYTE_SIZE}. */
    public static final long BYTE_SIZE = 40;

    /** Current wire format version. */
    public static final int VERSION = 1;

    // ── shape discriminants ───────────────────────────────────────────
    public static final int SHAPE_NONE = 0;
    public static final int SHAPE_COUNT_ONLY = 1;
    /** Reserved (PR3). */
    public static final int SHAPE_TOPK = 2;
    /** Reserved (PR3). */
    public static final int SHAPE_HISTOGRAM = 3;

    // ── range_unit discriminants ──────────────────────────────────────
    public static final int UNIT_NONE = 0;
    public static final int UNIT_SECONDS = 1;
    public static final int UNIT_MILLIS = 2;
    public static final int UNIT_MICROS = 3;
    public static final int UNIT_NANOS = 4;

    // ── flag bits ─────────────────────────────────────────────────────
    public static final int FLAG_RANGE_ON_LEADING_SORT_FIELD = 1 << 0;
    public static final int FLAG_RANGE_CONJUNCT_STRIPPABLE = 1 << 1;
    public static final int FLAG_TOPK_KEEP_LAST = 1 << 2;
    public static final int FLAG_TOPK_PATH_SAFE = 1 << 3;

    // ── histogram bucket-op discriminants (mirror FastPathHintSpec.BucketOp ordinals) ──
    public static final int BUCKET_OP_NONE = 0;
    public static final int BUCKET_OP_DIV = 1;
    public static final int BUCKET_OP_ADD = 2;
    public static final int BUCKET_OP_SUB = 3;
    public static final int BUCKET_OP_MUL = 4;
    /** {@code bucket = floor(col / operand) * operand} — PPL span composite (numeric {@code (col/N)*N} / time {@code date_trunc}). */
    public static final int BUCKET_OP_FLOOR_TO_MULTIPLE = 5;

    /**
     * The all-NONE hint every non-fast-path fragment ships: shape NONE, no flags,
     * no range. Rust classifies each row group as {@code Full}. Reserved fields are
     * zero (populated by PR3).
     */
    public static final FastPathHints NONE = new FastPathHints(VERSION, SHAPE_NONE, 0, UNIT_NONE, 0, 0L, 0L, 0, 0L);

    /**
     * Maps the planner's framework-level {@link FastPathHintSpec} to this fixed-layout wire struct.
     * The COUNT_ONLY range fields, the TOPK budget/flags, and the HISTOGRAM bucket op/operand all
     * land in their reserved slots. Returns {@link #NONE} for a null spec so callers can pass through
     * unconditionally.
     */
    public static FastPathHints fromSpec(FastPathHintSpec spec) {
        if (spec == null) {
            return NONE;
        }
        int shape = switch (spec.shape()) {
            case NONE -> SHAPE_NONE;
            case COUNT_ONLY -> SHAPE_COUNT_ONLY;
            case TOPK -> SHAPE_TOPK;
            case HISTOGRAM -> SHAPE_HISTOGRAM;
        };
        int flags = (spec.rangeOnLeadingSortField() ? FLAG_RANGE_ON_LEADING_SORT_FIELD : 0) | (spec.rangeConjunctStrippable()
            ? FLAG_RANGE_CONJUNCT_STRIPPABLE
            : 0) | (spec.topKKeepLast() ? FLAG_TOPK_KEEP_LAST : 0) | (spec.topKPathSafe() ? FLAG_TOPK_PATH_SAFE : 0);
        // RangeUnit and BucketOp ordinals mirror the wire discriminants (defined in lockstep on both sides).
        int rangeUnit = spec.rangeUnit().ordinal();
        int bucketOp = spec.histogramBucketOp().ordinal();
        return new FastPathHints(
            VERSION,
            shape,
            flags,
            rangeUnit,
            spec.topKBudget(),
            spec.rangeLowerInclusive(),
            spec.rangeUpperInclusive(),
            bucketOp,
            spec.histogramBucketOperand()
        );
    }

    /**
     * Writes this hint into a {@link MemorySegment} matching the Rust
     * {@code #[repr(C)]} layout. The segment must be at least {@link #BYTE_SIZE}
     * bytes and allocated from a confined {@code Arena} scoped to the query lifetime.
     *
     * @param segment the target memory segment (at least {@link #BYTE_SIZE} bytes)
     */
    public void writeTo(MemorySegment segment) {
        segment.set(ValueLayout.JAVA_BYTE, 0, (byte) version);
        segment.set(ValueLayout.JAVA_BYTE, 1, (byte) shape);
        segment.set(ValueLayout.JAVA_BYTE, 2, (byte) flags);
        segment.set(ValueLayout.JAVA_BYTE, 3, (byte) rangeUnit);
        segment.set(ValueLayout.JAVA_INT, 4, topkBudget);
        segment.set(ValueLayout.JAVA_LONG, 8, rangeLowerInclusive);
        segment.set(ValueLayout.JAVA_LONG, 16, rangeUpperInclusive);
        segment.set(ValueLayout.JAVA_BYTE, 24, (byte) histogramBucketOp);
        // offsets 25..31 are padding — left as the segment's initial zero.
        segment.set(ValueLayout.JAVA_LONG, 32, histogramBucketOperand);
    }
}
