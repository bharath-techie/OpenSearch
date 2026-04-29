/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import com.google.protobuf.ByteString;
import io.substrait.proto.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builds substrait plan protobuf bytes directly for perf tests.
 * Bypasses Calcite and the substrait Java model — constructs proto
 * messages using the generated builders.
 */
public class SubstraitPlanBuilder {

    private final List<String> columnNames;
    private final List<Type> columnTypes;

    public SubstraitPlanBuilder(List<String> columnNames, List<Type> columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    /**
     * Build: {@code SELECT * FROM tableName WHERE index_filter(field, value) AND predColumn op predValue}
     */
    public byte[] buildAndCollectorPredicate(
        String tableName,
        String field,
        String value,
        String predColumn,
        String predOp,
        int predValue
    ) {
        // Extension: declare index_filter as function ref 0
        var extUri = SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1)
            .setUri("/functions_opensearch.yaml")
            .build();
        var extDecl = SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setExtensionUriReference(1)
                    .setFunctionAnchor(0)
                    .setName("index_filter")
            )
            .build();
        // Declare comparison function (e.g. "equal" for eq)
        String compFuncName = switch (predOp) {
            case "eq" -> "equal";
            case "lt" -> "lt";
            case "gt" -> "gt";
            case "lte" -> "lte";
            case "gte" -> "gte";
            default -> throw new IllegalArgumentException("unknown op: " + predOp);
        };
        var compDecl = SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setExtensionUriReference(1)
                    .setFunctionAnchor(1)
                    .setName(compFuncName)
            )
            .build();
        // AND function
        var andDecl = SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                    .setExtensionUriReference(1)
                    .setFunctionAnchor(2)
                    .setName("and")
            )
            .build();

        // Schema: NamedStruct
        var namedStruct = NamedStruct.newBuilder()
            .addAllNames(columnNames)
            .setStruct(Type.Struct.newBuilder().addAllTypes(columnTypes));

        // NamedScan (Read relation)
        var readRel = ReadRel.newBuilder()
            .setBaseSchema(namedStruct)
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName))
            .build();

        // index_filter(query_bytes) expression
        byte[] queryBytes = (field + "\0" + value).getBytes(StandardCharsets.UTF_8);
        var indexFilterExpr = Expression.newBuilder()
            .setScalarFunction(
                Expression.ScalarFunction.newBuilder()
                    .setFunctionReference(0) // index_filter
                    .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                    .addArguments(
                        FunctionArgument.newBuilder()
                            .setValue(
                                Expression.newBuilder()
                                    .setLiteral(
                                        Expression.Literal.newBuilder()
                                            .setBinary(ByteString.copyFrom(queryBytes))
                                    )
                            )
                    )
            )
            .build();

        // predColumn op predValue expression
        int predColIdx = columnNames.indexOf(predColumn);
        if (predColIdx < 0) throw new IllegalArgumentException("column not found: " + predColumn);

        var colRef = Expression.newBuilder()
            .setSelection(
                Expression.FieldReference.newBuilder()
                    .setDirectReference(
                        Expression.ReferenceSegment.newBuilder()
                            .setStructField(
                                Expression.ReferenceSegment.StructField.newBuilder()
                                    .setField(predColIdx)
                            )
                    )
                    .setRootReference(Expression.FieldReference.RootReference.newBuilder())
            )
            .build();
        var litVal = Expression.newBuilder()
            .setLiteral(Expression.Literal.newBuilder().setI32(predValue))
            .build();
        var compExpr = Expression.newBuilder()
            .setScalarFunction(
                Expression.ScalarFunction.newBuilder()
                    .setFunctionReference(1) // comparison
                    .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                    .addArguments(FunctionArgument.newBuilder().setValue(colRef))
                    .addArguments(FunctionArgument.newBuilder().setValue(litVal))
            )
            .build();

        // AND(index_filter, comparison)
        var andExpr = Expression.newBuilder()
            .setScalarFunction(
                Expression.ScalarFunction.newBuilder()
                    .setFunctionReference(2) // and
                    .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                    .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr))
                    .addArguments(FunctionArgument.newBuilder().setValue(compExpr))
            )
            .build();

        // Filter(Read, AND(...))
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(andExpr)
            .build();

        // Plan root
        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setFilter(filterRel))
                .addAllNames(columnNames)
            )
            .build();

        var plan = Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(extDecl)
            .addExtensions(compDecl)
            .addExtensions(andDecl)
            .addRelations(root)
            .build();

        return plan.toByteArray();
    }

    /**
     * Build: {@code SELECT * FROM tableName WHERE index_filter(field, value)}
     * Collector only, no parquet-native predicate.
     */
    public byte[] buildCollectorOnly(String tableName, String field, String value) {
        var extUri = SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1).setUri("/functions_opensearch.yaml").build();
        var extDecl = SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                .setExtensionUriReference(1).setFunctionAnchor(0).setName("index_filter")).build();

        var namedStruct = NamedStruct.newBuilder()
            .addAllNames(columnNames)
            .setStruct(Type.Struct.newBuilder().addAllTypes(columnTypes));

        var readRel = ReadRel.newBuilder()
            .setBaseSchema(namedStruct)
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName)).build();

        byte[] queryBytes = (field + "\0" + value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var indexFilterExpr = Expression.newBuilder()
            .setScalarFunction(Expression.ScalarFunction.newBuilder()
                .setFunctionReference(0)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder()
                    .setValue(Expression.newBuilder()
                        .setLiteral(Expression.Literal.newBuilder()
                            .setBinary(ByteString.copyFrom(queryBytes)))))).build();

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(indexFilterExpr).build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setFilter(filterRel))
                .addAllNames(columnNames)).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri).addExtensions(extDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT groupCol, COUNT(*) FROM table WHERE index_filter(field,value) AND predCol op predVal GROUP BY groupCol}
     */
    public byte[] buildAndCollectorPredicateGroupBy(
        String tableName, String field, String value,
        String predColumn, String predOp, int predValue,
        String groupColumn
    ) {
        // Reuse the filter plan, then wrap in AggregateRel
        var extUri = SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1).setUri("/functions_opensearch.yaml").build();
        var indexFilterDecl = extDecl(0, "index_filter");
        var compDecl = extDecl(1, compFuncName(predOp));
        var andDecl = extDecl(2, "and");
        var countDecl = extDecl(3, "count");

        var namedStruct = NamedStruct.newBuilder()
            .addAllNames(columnNames)
            .setStruct(Type.Struct.newBuilder().addAllTypes(columnTypes));
        var readRel = ReadRel.newBuilder()
            .setBaseSchema(namedStruct)
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName)).build();

        // Filter: AND(index_filter, comparison)
        byte[] queryBytes = (field + "\0" + value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(andExpr(
                indexFilterExpr(queryBytes),
                compExpr(predColumn, predValue)
            )).build();

        // Aggregate: GROUP BY groupColumn, COUNT(*)
        int groupColIdx = columnNames.indexOf(groupColumn);
        var grouping = AggregateRel.Grouping.newBuilder()
            .addGroupingExpressions(fieldRef(groupColIdx));
        var countMeasure = AggregateRel.Measure.newBuilder()
            .setMeasure(AggregateFunction.newBuilder()
                .setFunctionReference(3)
                .setOutputType(Type.newBuilder().setI64(Type.I64.newBuilder()))
                .setInvocation(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL)
            );
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(grouping)
            .addMeasures(countMeasure)
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames(groupColumn).addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(compDecl)
            .addExtensions(andDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT groupCol, COUNT(*) FROM table WHERE index_filter(field,value) GROUP BY groupCol}
     */
    public byte[] buildCollectorOnlyGroupBy(
        String tableName, String field, String value, String groupColumn
    ) {
        var extUri = SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1).setUri("/functions_opensearch.yaml").build();
        var indexFilterDecl = extDecl(0, "index_filter");
        var countDecl = extDecl(1, "count");

        var namedStruct = NamedStruct.newBuilder()
            .addAllNames(columnNames)
            .setStruct(Type.Struct.newBuilder().addAllTypes(columnTypes));
        var readRel = ReadRel.newBuilder()
            .setBaseSchema(namedStruct)
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName)).build();

        byte[] queryBytes = (field + "\0" + value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(indexFilterExpr(queryBytes)).build();

        int groupColIdx = columnNames.indexOf(groupColumn);
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder()
                .addGroupingExpressions(fieldRef(groupColIdx)))
            .addMeasures(AggregateRel.Measure.newBuilder()
                .setMeasure(AggregateFunction.newBuilder()
                    .setFunctionReference(1)
                    .setOutputType(Type.newBuilder().setI64(Type.I64.newBuilder()))
                    .setInvocation(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL)))
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames(groupColumn).addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    // ── Shared expression builders ────────────────────────────────

    private SimpleExtensionDeclaration extDecl(int anchor, String name) {
        return SimpleExtensionDeclaration.newBuilder()
            .setExtensionFunction(SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                .setExtensionUriReference(1).setFunctionAnchor(anchor).setName(name)).build();
    }

    private String compFuncName(String op) {
        return switch (op) {
            case "eq" -> "equal"; case "lt" -> "lt"; case "gt" -> "gt";
            case "lte" -> "lte"; case "gte" -> "gte";
            default -> throw new IllegalArgumentException("unknown op: " + op);
        };
    }

    private Expression indexFilterExpr(byte[] queryBytes) {
        return Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(0)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder()
                    .setValue(Expression.newBuilder().setLiteral(
                        Expression.Literal.newBuilder().setBinary(ByteString.copyFrom(queryBytes))
                    )))).build();
    }

    private Expression compExpr(String col, int value) {
        int idx = columnNames.indexOf(col);
        return Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(1)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(fieldRef(idx)))
                .addArguments(FunctionArgument.newBuilder().setValue(
                    Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI32(value))
                ))).build();
    }

    private Expression andExpr(Expression left, Expression right) {
        return Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(2)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(left))
                .addArguments(FunctionArgument.newBuilder().setValue(right))
        ).build();
    }

    private Expression fieldRef(int idx) {
        return Expression.newBuilder().setSelection(
            Expression.FieldReference.newBuilder()
                .setDirectReference(Expression.ReferenceSegment.newBuilder()
                    .setStructField(Expression.ReferenceSegment.StructField.newBuilder().setField(idx)))
                .setRootReference(Expression.FieldReference.RootReference.newBuilder())
        ).build();
    }

    // ── Helpers for building column types ──────────────────────────

    public static Type i32Type() {
        return Type.newBuilder().setI32(Type.I32.newBuilder()).build();
    }

    public static Type i64Type() {
        return Type.newBuilder().setI64(Type.I64.newBuilder()).build();
    }

    public static Type fp32Type() {
        return Type.newBuilder().setFp32(Type.FP32.newBuilder()).build();
    }

    public static Type binaryType() {
        return Type.newBuilder().setBinary(Type.Binary.newBuilder()).build();
    }
}
