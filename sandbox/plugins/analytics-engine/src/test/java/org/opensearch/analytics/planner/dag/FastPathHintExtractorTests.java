/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.spi.FastPathHintSpec;

import java.util.List;

/**
 * Unit tests for {@link FastPathHintExtractor}. Fragments are assembled directly from Calcite rel
 * nodes (generic {@code Aggregate}/{@code Filter}, which the OpenSearch subclasses extend), so the
 * rules are exercised without running the whole planner.
 */
public class FastPathHintExtractorTests extends BasePlannerRulesTests {

    private static final String BACKEND = "datafusion";

    /** Scan with a leading sort field {@code ts} (BIGINT) and a {@code host} (VARCHAR). */
    private RelNode logsScan() {
        return stubScan(mockTable("logs", new String[] { "ts", "host" }, new SqlTypeName[] { SqlTypeName.BIGINT, SqlTypeName.VARCHAR }));
    }

    private RexNode tsAtLeast(RelNode input, long value) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(input.getRowType().getFieldList().get(0).getType(), 0),
            rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
    }

    private RexNode hostEquals(RelNode input, String value) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(input.getRowType().getFieldList().get(1).getType(), 1),
            rexBuilder.makeLiteral(value)
        );
    }

    private RelNode countOver(RelNode input) {
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        return LogicalAggregate.create(input, List.of(), ImmutableBitSet.of(), null, List.of(count));
    }

    public void testCountOnlyHappyPath() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RelNode agg = countOver(filter);

        FastPathHintSpec hints = FastPathHintExtractor.extract(agg, "ts", "date", BACKEND);

        assertEquals(FastPathHintSpec.Shape.COUNT_ONLY, hints.shape());
        assertTrue(hints.rangeOnLeadingSortField());
        assertTrue(hints.rangeConjunctStrippable());
        assertEquals(FastPathHintSpec.RangeUnit.MILLIS, hints.rangeUnit());
        assertEquals(1000L, hints.rangeLowerInclusive());
        assertEquals(Long.MAX_VALUE, hints.rangeUpperInclusive());
    }

    public void testNoneForGroupKeys() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        AggregateCall count = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            filter,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        // group by host — a grouped aggregate is not a whole-fragment count.
        RelNode agg = LogicalAggregate.create(filter, List.of(), ImmutableBitSet.of(1), null, List.of(count));

        FastPathHintSpec hints = FastPathHintExtractor.extract(agg, "ts", "date", BACKEND);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
    }

    public void testNoneForRangeUnderOr() {
        RelNode scan = logsScan();
        RexNode or = rexBuilder.makeCall(SqlStdOperatorTable.OR, tsAtLeast(scan, 1000L), hostEquals(scan, "x"));
        RelNode filter = LogicalFilter.create(scan, or);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
        assertFalse("a range under a top-level OR must not be reported on the leading sort field", hints.rangeOnLeadingSortField());
    }

    public void testNoneForCastOnColumn() {
        RelNode scan = logsScan();
        RelDataType tsType = scan.getRowType().getFieldList().get(0).getType();
        // CAST(ts AS TIMESTAMP) > 1000 — the column side is a function, not a bare ref.
        RexNode castCol = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), rexBuilder.makeInputRef(tsType, 0));
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            castCol,
            rexBuilder.makeLiteral(1000L, typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        );
        RelNode filter = LogicalFilter.create(scan, cond);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND);
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
        assertFalse(hints.rangeOnLeadingSortField());
    }

    /** A pure AND with a native residual: the range is strippable, but the shape is not count-only. */
    public void testStrippableWithNativeResidual() {
        RelNode scan = logsScan();
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsAtLeast(scan, 1000L), hostEquals(scan, "foo"));
        RelNode filter = LogicalFilter.create(scan, and);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND);
        assertTrue(hints.rangeOnLeadingSortField());
        assertTrue("a pure conjunction is strippable", hints.rangeConjunctStrippable());
        assertEquals("a native residual predicate rules out count-only", FastPathHintSpec.Shape.NONE, hints.shape());
    }

    /** An OR sibling makes the residual non-pure-AND, so the range conjunct is not strippable. */
    public void testNotStrippableUnderOrSibling() {
        RelNode scan = logsScan();
        RexNode orSibling = rexBuilder.makeCall(SqlStdOperatorTable.OR, hostEquals(scan, "a"), hostEquals(scan, "b"));
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsAtLeast(scan, 1000L), orSibling);
        RelNode filter = LogicalFilter.create(scan, and);

        FastPathHintSpec hints = FastPathHintExtractor.extract(filter, "ts", "date", BACKEND);
        assertTrue("the leading-sort range is still a top-level MUST conjunct", hints.rangeOnLeadingSortField());
        assertFalse("an OR sibling makes the residual non-pure-AND", hints.rangeConjunctStrippable());
        assertEquals(FastPathHintSpec.Shape.NONE, hints.shape());
    }

    public void testDateVersusDateNanosUnit() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        RelNode agg = countOver(filter);

        assertEquals(FastPathHintSpec.RangeUnit.MILLIS, FastPathHintExtractor.extract(agg, "ts", "date", BACKEND).rangeUnit());
        assertEquals(FastPathHintSpec.RangeUnit.NANOS, FastPathHintExtractor.extract(agg, "ts", "date_nanos", BACKEND).rangeUnit());
    }

    /** A delegated (Lucene) leaf alongside the sort-range is allowed by count-only. */
    public void testCountOnlyAllowsDelegatedLeaf() {
        RelNode scan = logsScan();
        RexNode hostEq = hostEquals(scan, "foo");
        AnnotatedPredicate delegated = new AnnotatedPredicate(hostEq.getType(), hostEq, List.of("lucene"), 0);
        RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, tsAtLeast(scan, 1000L), delegated);
        RelNode filter = LogicalFilter.create(scan, and);
        RelNode agg = countOver(filter);

        FastPathHintSpec hints = FastPathHintExtractor.extract(agg, "ts", "date", BACKEND);
        assertEquals(FastPathHintSpec.Shape.COUNT_ONLY, hints.shape());
    }

    public void testNullLeadingSortFieldFailsClosed() {
        RelNode scan = logsScan();
        RelNode filter = LogicalFilter.create(scan, tsAtLeast(scan, 1000L));
        assertEquals(FastPathHintSpec.NONE, FastPathHintExtractor.extract(filter, null, "date", BACKEND));
    }
}
