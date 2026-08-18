/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.MVReadTarget;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Plan-shape tests for the materialized-view read rewrite: the user query's PARTIAL
 * aggregate subtree (aggregation over the raw source) is replaced by a scan of the
 * view's state columns; the FINAL half and everything above stay untouched.
 */
public class MVStateReadRewriterTests extends PlanShapeTestBase {

    private static final String VIEW = "logs_view";

    /** sum(size) grouped by status — `stats sum(size) as total by status`. */
    private RelNode groupedSum(RelOptTable sourceTable) {
        RelNode scan = stubScan(sourceTable);
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total"
        );
        return makeAggregate(scan, ImmutableBitSet.of(0), sum);
    }

    /** View table: [status, total-state] where the state was mapping-widened to BIGINT. */
    private RelOptTable viewTable() {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("status", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        builder.add("sum_input_0_total___st_0", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true));
        builder.add("__mv_partition", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true));
        RelDataType rowType = builder.build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(VIEW));
        when(table.getRowType()).thenReturn(rowType);
        return table;
    }

    private static final String SPEC = """
        {"key_columns":["status","__mv_partition"],
         "aggs":[{"output":"sum_input_0_total_","fn":"sum","input_types":["Int64"]}],
         "state_columns":[
           {"name":"status","type":"Int32"},
           {"name":"__mv_partition","type":"Timestamp(ms)"},
           {"name":"sum_input_0_total___st_0","type":"Int64"}]}""";

    /** Context knowing both indices, with mappings covering the view's state columns. */
    private PlannerContext viewAwareContext() {
        java.util.Map<String, java.util.Map<String, Object>> fields = java.util.Map.of(
            "status",
            java.util.Map.of("type", "integer"),
            "size",
            java.util.Map.of("type", "integer"),
            "sum_input_0_total___st_0",
            java.util.Map.of("type", "long"),
            "__mv_partition",
            java.util.Map.of("type", "date")
        );
        return buildContextPerIndex("parquet", java.util.Map.of("test_index", 1, VIEW, 1), fields, java.util.List.of(DATAFUSION, LUCENE));
    }

    public void testRewriteReplacesPartialWithViewStateScan() {
        PlannerContext context = viewAwareContext();

        // Source table resolves the view through its RelOptSchema — the same catalog hop
        // the production path takes.
        RelOptTable sourceTable = mockTable("test_index", "status", "size");
        RelOptSchema relOptSchema = mock(RelOptSchema.class);
        RelOptTable view = viewTable();
        when(sourceTable.getRelOptSchema()).thenReturn(relOptSchema);
        when(relOptSchema.getTableForMember(List.of(VIEW))).thenReturn(view);

        RelNode planned = runPlanner(groupedSum(sourceTable), context);
        RelNode rewritten = MVStateReadRewriter.rewrite(planned, new MVReadTarget(VIEW, SPEC), context);

        String plan = RelOptUtil.toString(rewritten);
        assertTrue("FINAL aggregate must remain:\n" + plan, plan.contains("mode=[FINAL]"));
        assertTrue("view scan must replace the source:\n" + plan, plan.contains(VIEW));
        assertFalse("PARTIAL over the source must be gone:\n" + plan, plan.contains("mode=[PARTIAL]"));
        assertFalse("source scan must be gone:\n" + plan, plan.contains("test_index"));

        // The projection feeding the FINAL must reference exactly [key, state column] of
        // the view scan — never the partition column (states fold across partitions).
        org.opensearch.analytics.planner.rel.OpenSearchProject project = findProject(rewritten);
        assertNotNull("state projection expected:\n" + plan, project);
        assertEquals("keys + one state column", 2, project.getProjects().size());
        java.util.Set<Integer> referenced = new java.util.TreeSet<>();
        for (org.apache.calcite.rex.RexNode expr : project.getProjects()) {
            expr.accept(new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(org.apache.calcite.rex.RexInputRef inputRef) {
                    referenced.add(inputRef.getIndex());
                    return null;
                }
            });
        }
        RelNode scan = project.getInput();
        java.util.List<String> scanFields = scan.getRowType().getFieldNames();
        assertTrue("key column referenced", referenced.contains(scanFields.indexOf("status")));
        assertTrue("state column referenced", referenced.contains(scanFields.indexOf("sum_input_0_total___st_0")));
        assertFalse("partition column must not be referenced", referenced.contains(scanFields.indexOf("__mv_partition")));
    }

    private static org.opensearch.analytics.planner.rel.OpenSearchProject findProject(RelNode node) {
        if (node instanceof org.opensearch.analytics.planner.rel.OpenSearchProject p) {
            return p;
        }
        for (RelNode input : node.getInputs()) {
            org.opensearch.analytics.planner.rel.OpenSearchProject found = findProject(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    public void testCallMismatchAbandonsRewrite() {
        PlannerContext context = viewAwareContext();
        RelOptTable sourceTable = mockTable("test_index", "status", "size");
        RelOptSchema relOptSchema = mock(RelOptSchema.class);
        RelOptTable view = viewTable();
        when(sourceTable.getRelOptSchema()).thenReturn(relOptSchema);
        when(relOptSchema.getTableForMember(List.of(VIEW))).thenReturn(view);

        // Spec claims a MIN call; the plan carries SUM — drift must abandon the rewrite.
        String driftedSpec = SPEC.replace("\"fn\":\"sum\"", "\"fn\":\"min\"");
        RelNode planned = runPlanner(groupedSum(sourceTable), context);
        RelNode result = MVStateReadRewriter.rewrite(planned, new MVReadTarget(VIEW, driftedSpec), context);

        String plan = RelOptUtil.toString(result);
        assertTrue("query must fall back to the source:\n" + plan, plan.contains("test_index"));
        assertFalse("view must not be scanned on mismatch:\n" + plan, plan.contains(VIEW));
    }

    public void testMissingStateColumnAbandonsRewrite() {
        PlannerContext context = viewAwareContext();
        RelOptTable sourceTable = mockTable("test_index", "status", "size");
        RelOptSchema relOptSchema = mock(RelOptSchema.class);
        // View lacks the state column entirely.
        RelOptTable emptyView = mockTable(VIEW, "status");
        when(sourceTable.getRelOptSchema()).thenReturn(relOptSchema);
        when(relOptSchema.getTableForMember(List.of(VIEW))).thenReturn(emptyView);

        RelNode planned = runPlanner(groupedSum(sourceTable), context);
        RelNode result = MVStateReadRewriter.rewrite(planned, new MVReadTarget(VIEW, SPEC), context);
        assertTrue(RelOptUtil.toString(result).contains("test_index"));
    }
}
