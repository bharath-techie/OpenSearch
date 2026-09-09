/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Backend-agnostic fast-path hints the planner computes ONCE per shard fragment and ships to the
 * data node alongside {@code treeShape}/{@code delegatedPredicateCount}/{@code requestsRowIds} on
 * the shard-scan instruction node. It is the framework-level carrier for the decision the
 * {@code FastPathHintExtractor} makes; the DataFusion backend maps it to its fixed-layout FFM wire
 * struct ({@code org.opensearch.be.datafusion.nativelib.FastPathHints}) at session-context build
 * time. Kept here because the planner (analytics-engine) cannot depend on the DataFusion plugin.
 *
 * <p>All fields fail closed: {@link #NONE} (shape {@link Shape#NONE}, no flags, no range) is what
 * every non-fast-path fragment ships, and the data node then classifies every row group as full.
 *
 * @param shape                   the recognised fragment shape
 * @param rangeOnLeadingSortField true when exactly one top-level MUST range conjunct targets the
 *                                leading index-sort field (literal bounds, no column-side CAST)
 * @param rangeConjunctStrippable true when that range conjunct can be stripped from the residual +
 *                                pushdown because the condition is a pure conjunction (Fix 10)
 * @param rangeUnit               the DECLARED mapping unit of the sort field (date =&gt; millis,
 *                                date_nanos =&gt; nanos); {@link RangeUnit#NONE} for non-temporal
 * @param rangeLowerInclusive     inclusive lower bound ({@link Long#MIN_VALUE} = unbounded below)
 * @param rangeUpperInclusive     inclusive upper bound ({@link Long#MAX_VALUE} = unbounded above)
 * @param topKBudget              {@code offset + fetch} rows to keep per WITHIN row group ({@code >0}
 *                                only for {@link Shape#TOPK}); {@code 0} otherwise
 * @param topKKeepLast            true when the query sort direction differs from the index sort order,
 *                                so the newest N rows are the LAST N of each index-sorted row group
 * @param topKPathSafe            true when the Sort→Scan spine is truncation-safe (identity
 *                                Project/Filter/Limit/SubqueryAlias only)
 * @param histogramBucketOp       the monotonic integer bucket operator on the sort field
 *                                ({@link Shape#HISTOGRAM} only); {@link BucketOp#NONE} otherwise
 * @param histogramBucketOperand  the positive integer operand of {@code histogramBucketOp}
 *
 * @opensearch.internal
 */
public record FastPathHintSpec(Shape shape, boolean rangeOnLeadingSortField, boolean rangeConjunctStrippable, RangeUnit rangeUnit,
    long rangeLowerInclusive, long rangeUpperInclusive, int topKBudget, boolean topKKeepLast, boolean topKPathSafe,
    BucketOp histogramBucketOp, long histogramBucketOperand) implements Writeable {

    /** Recognised fragment shape. Ordinals are wire-stable; append only. */
    public enum Shape {
        /** No fast path — the data node classifies every row group as full. */
        NONE,
        /** Aggregate with a single {@code count(*)}/{@code count(literal)}, no group keys. */
        COUNT_ONLY,
        /** {@code sort <leading> | head N} — keep only N candidates per WITHIN row group. */
        TOPK,
        /** Aggregate grouped by monotonic integer arithmetic on the leading sort field + count. */
        HISTOGRAM
    }

    /** Declared unit of the sort field's range bounds. Ordinals mirror the FFM wire discriminants. */
    public enum RangeUnit {
        NONE,
        SECONDS,
        MILLIS,
        MICROS,
        NANOS
    }

    /** Monotonic integer bucket operator. Ordinals are wire-stable and mirror the FFM discriminants. */
    public enum BucketOp {
        /** No bucket op (identity: {@code bucket = col}). */
        NONE,
        /** {@code bucket = col / operand} (floor toward -inf on the native side). */
        DIV,
        /** {@code bucket = col + operand}. */
        ADD,
        /** {@code bucket = col - operand}. */
        SUB,
        /** {@code bucket = col * operand}. */
        MUL,
        /**
         * {@code bucket = floor(col / operand) * operand}, {@code operand > 0}. The composite bucket
         * PPL {@code span()} actually lowers to: numeric span → {@code (col/N)*N} (FLOOR-wrapped for
         * non-integer), time span (interval 1) → {@code date_trunc(second|minute|hour, col)}. The
         * operand is expressed in the sort field's declared {@link RangeUnit} (the same unit the range
         * bounds use); the data node coerces it to the physical unit exactly like the range.
         */
        FLOOR_TO_MULTIPLE
    }

    /** The all-NONE hint every non-fast-path fragment ships. */
    public static final FastPathHintSpec NONE = new FastPathHintSpec(
        Shape.NONE,
        false,
        false,
        RangeUnit.NONE,
        Long.MIN_VALUE,
        Long.MAX_VALUE,
        0,
        false,
        false,
        BucketOp.NONE,
        0L
    );

    public FastPathHintSpec(StreamInput in) throws IOException {
        this(
            in.readEnum(Shape.class),
            in.readBoolean(),
            in.readBoolean(),
            in.readEnum(RangeUnit.class),
            in.readLong(),
            in.readLong(),
            in.readVInt(),
            in.readBoolean(),
            in.readBoolean(),
            in.readEnum(BucketOp.class),
            in.readLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(shape);
        out.writeBoolean(rangeOnLeadingSortField);
        out.writeBoolean(rangeConjunctStrippable);
        out.writeEnum(rangeUnit);
        out.writeLong(rangeLowerInclusive);
        out.writeLong(rangeUpperInclusive);
        out.writeVInt(topKBudget);
        out.writeBoolean(topKKeepLast);
        out.writeBoolean(topKPathSafe);
        out.writeEnum(histogramBucketOp);
        out.writeLong(histogramBucketOperand);
    }

    /**
     * A range-only hint: shape {@link Shape#NONE} but the leading-sort range is present (and possibly
     * strippable). The data node can still apply the timestamp-strip fast path even without a
     * recognised aggregate/sort shape.
     */
    public static FastPathHintSpec rangeOnly(boolean strippable, RangeUnit unit, long lower, long upper) {
        return new FastPathHintSpec(Shape.NONE, true, strippable, unit, lower, upper, 0, false, false, BucketOp.NONE, 0L);
    }

    /** A COUNT_ONLY hint carrying the leading-sort range. */
    public static FastPathHintSpec countOnly(boolean strippable, RangeUnit unit, long lower, long upper) {
        return new FastPathHintSpec(Shape.COUNT_ONLY, true, strippable, unit, lower, upper, 0, false, false, BucketOp.NONE, 0L);
    }

    /** A TOPK hint: leading-sort range + per-RG candidate budget and truncation direction. */
    public static FastPathHintSpec topK(
        int budget,
        boolean keepLast,
        boolean pathSafe,
        boolean strippable,
        RangeUnit unit,
        long lower,
        long upper
    ) {
        return new FastPathHintSpec(Shape.TOPK, true, strippable, unit, lower, upper, budget, keepLast, pathSafe, BucketOp.NONE, 0L);
    }

    /** A HISTOGRAM hint: leading-sort range + the monotonic integer bucket op/operand. */
    public static FastPathHintSpec histogram(BucketOp op, long operand, boolean strippable, RangeUnit unit, long lower, long upper) {
        return new FastPathHintSpec(Shape.HISTOGRAM, true, strippable, unit, lower, upper, 0, false, false, op, operand);
    }

    /** True when this hint carries no fast-path signal (shape NONE and no range flag). */
    public boolean isNone() {
        return shape == Shape.NONE && rangeOnLeadingSortField == false;
    }
}
