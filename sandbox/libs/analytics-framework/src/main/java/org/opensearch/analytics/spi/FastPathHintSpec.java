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
 * Reserved shapes ({@code TOPK}/{@code HISTOGRAM}) are introduced in PR3.
 *
 * @param shape                   the recognised fragment shape ({@link Shape#COUNT_ONLY} today)
 * @param rangeOnLeadingSortField true when exactly one top-level MUST range conjunct targets the
 *                                leading index-sort field (literal bounds, no column-side CAST)
 * @param rangeConjunctStrippable true when that range conjunct can be stripped from the residual +
 *                                pushdown because the condition is a pure conjunction (Fix 10)
 * @param rangeUnit               the DECLARED mapping unit of the sort field (date =&gt; millis,
 *                                date_nanos =&gt; nanos); {@link RangeUnit#NONE} for non-temporal
 * @param rangeLowerInclusive     inclusive lower bound ({@link Long#MIN_VALUE} = unbounded below)
 * @param rangeUpperInclusive     inclusive upper bound ({@link Long#MAX_VALUE} = unbounded above)
 *
 * @opensearch.internal
 */
public record FastPathHintSpec(Shape shape, boolean rangeOnLeadingSortField, boolean rangeConjunctStrippable, RangeUnit rangeUnit,
    long rangeLowerInclusive, long rangeUpperInclusive) implements Writeable {

    /** Recognised fragment shape. Ordinals are wire-stable; append only. */
    public enum Shape {
        /** No fast path — the data node classifies every row group as full. */
        NONE,
        /** Aggregate with a single {@code count(*)}/{@code count(literal)}, no group keys. */
        COUNT_ONLY
    }

    /** Declared unit of the sort field's range bounds. Ordinals mirror the FFM wire discriminants. */
    public enum RangeUnit {
        NONE,
        SECONDS,
        MILLIS,
        MICROS,
        NANOS
    }

    /** The all-NONE hint every non-fast-path fragment ships. */
    public static final FastPathHintSpec NONE = new FastPathHintSpec(
        Shape.NONE,
        false,
        false,
        RangeUnit.NONE,
        Long.MIN_VALUE,
        Long.MAX_VALUE
    );

    public FastPathHintSpec(StreamInput in) throws IOException {
        this(in.readEnum(Shape.class), in.readBoolean(), in.readBoolean(), in.readEnum(RangeUnit.class), in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(shape);
        out.writeBoolean(rangeOnLeadingSortField);
        out.writeBoolean(rangeConjunctStrippable);
        out.writeEnum(rangeUnit);
        out.writeLong(rangeLowerInclusive);
        out.writeLong(rangeUpperInclusive);
    }

    /** True when this hint carries no fast-path signal (shape NONE and no range flag). */
    public boolean isNone() {
        return shape == Shape.NONE && rangeOnLeadingSortField == false;
    }
}
