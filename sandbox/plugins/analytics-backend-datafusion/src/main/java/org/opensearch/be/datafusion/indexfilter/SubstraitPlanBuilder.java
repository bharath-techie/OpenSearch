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
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(grouping)
            .addMeasures(countMeasure(3))
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
            .addMeasures(countMeasure(1))
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

    /**
     * Build: {@code SELECT COUNT(*) FROM table WHERE index_filter(field,value) AND predCol op predVal}
     */
    public byte[] buildAndCollectorPredicateCount(
        String tableName, String field, String value,
        String predColumn, String predOp, int predValue
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var compDecl = extDecl(1, compFuncName(predOp));
        var andDecl = extDecl(2, "and");
        var countDecl = extDecl(3, "count");

        var readRel = namedRead(tableName);
        byte[] queryBytes = (field + "\0" + value).getBytes(StandardCharsets.UTF_8);
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(andExpr(indexFilterExpr(queryBytes), compExpr(predColumn, predValue)))
            .build();

        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder()) // empty grouping = no GROUP BY
            .addMeasures(countMeasure(3))
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(compDecl)
            .addExtensions(andDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM table WHERE index_filter(field,value)}
     */
    public byte[] buildCollectorOnlyCount(String tableName, String field, String value) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var countDecl = extDecl(1, "count");

        var readRel = namedRead(tableName);
        byte[] queryBytes = (field + "\0" + value).getBytes(StandardCharsets.UTF_8);
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(indexFilterExpr(queryBytes)).build();

        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(1))
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri).addExtensions(indexFilterDecl)
            .addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build a deep/wide tree:
     * {@code (c1 OR c2) AND ((c3 AND p1) OR (c4 AND p2)) AND (c5 OR (c6 AND p3))}
     *
     * 6 collectors, 3 numeric predicates, AND/OR nested to depth 4.
     * Exercises prefetch scheduling when many Lucene calls must complete
     * per RG, mixed with cheap page-pruner predicates.
     *
     * Arrays are 6-long for collectors and 3-long for predicates — caller
     * provides real field/value pairs.
     */
    public byte[] buildDeepTreeCount(
        String tableName,
        String[] cFields, String[] cVals,        // 6
        String[] pCols, String[] pOps, int[] pVals // 3
    ) {
        if (cFields.length != 6 || cVals.length != 6) {
            throw new IllegalArgumentException("expected 6 collectors");
        }
        if (pCols.length != 3 || pOps.length != 3 || pVals.length != 3) {
            throw new IllegalArgumentException("expected 3 predicates");
        }
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var compDecl = extDecl(1, compFuncName(pOps[0])); // assume same op for simplicity
        var andDecl = extDecl(2, "and");
        var orDecl = extDecl(3, "or");
        var countDecl = extDecl(4, "count");

        var readRel = namedRead(tableName);
        Expression[] c = new Expression[6];
        for (int i = 0; i < 6; i++) {
            byte[] qb = (cFields[i] + "\0" + cVals[i]).getBytes(StandardCharsets.UTF_8);
            c[i] = indexFilterExpr(qb);
        }
        Expression[] p = new Expression[3];
        for (int i = 0; i < 3; i++) {
            p[i] = compExpr(pCols[i], pVals[i]);
        }

        var or1 = orExpr(c[0], c[1]);
        var group2 = orExpr(andExpr(c[2], p[0]), andExpr(c[3], p[1]));
        var group3 = orExpr(c[4], andExpr(c[5], p[2]));
        var filterExpr = andExpr(andExpr(or1, group2), group3);

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filterExpr).build();
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(4)).build();
        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(compDecl)
            .addExtensions(andDecl).addExtensions(orDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build an extra-wide / deep tree:
     * {@code ((c1 AND c2) OR (c3 AND p1)) AND ((c4 OR c5) AND (c6 OR p2)) AND (c7 AND p3)}
     *
     * Alt shape for variance — depth 5, 7 collectors, 3 predicates.
     */
    public byte[] buildExtraDeepTreeCount(
        String tableName,
        String[] cFields, String[] cVals,        // 7
        String[] pCols, String[] pOps, int[] pVals // 3
    ) {
        if (cFields.length != 7 || cVals.length != 7) {
            throw new IllegalArgumentException("expected 7 collectors");
        }
        if (pCols.length != 3 || pOps.length != 3 || pVals.length != 3) {
            throw new IllegalArgumentException("expected 3 predicates");
        }
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var compDecl = extDecl(1, compFuncName(pOps[0]));
        var andDecl = extDecl(2, "and");
        var orDecl = extDecl(3, "or");
        var countDecl = extDecl(4, "count");

        var readRel = namedRead(tableName);
        Expression[] c = new Expression[7];
        for (int i = 0; i < 7; i++) {
            byte[] qb = (cFields[i] + "\0" + cVals[i]).getBytes(StandardCharsets.UTF_8);
            c[i] = indexFilterExpr(qb);
        }
        Expression[] p = new Expression[3];
        for (int i = 0; i < 3; i++) {
            p[i] = compExpr(pCols[i], pVals[i]);
        }

        var g1 = orExpr(andExpr(c[0], c[1]), andExpr(c[2], p[0]));
        var g2 = andExpr(orExpr(c[3], c[4]), orExpr(c[5], p[1]));
        var g3 = andExpr(c[6], p[2]);
        var filterExpr = andExpr(andExpr(g1, g2), g3);

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filterExpr).build();
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(4)).build();
        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(compDecl)
            .addExtensions(andDecl).addExtensions(orDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM t WHERE index_filter(f1,v1) AND index_filter(f2,v2) AND index_filter(f3,v3) AND predCol op predVal}
     * Three collectors + one numeric predicate. Tree path. Mixes Lucene
     * work with a cheap residual.
     */
    public byte[] buildThreeCollectorAndPredicateCount(
        String tableName,
        String f1, String v1,
        String f2, String v2,
        String f3, String v3,
        String predCol, String predOp, int predVal
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var compDecl = extDecl(1, compFuncName(predOp));
        var andDecl = extDecl(2, "and");
        var countDecl = extDecl(3, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (f1 + "\0" + v1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (f2 + "\0" + v2).getBytes(StandardCharsets.UTF_8);
        byte[] q3 = (f3 + "\0" + v3).getBytes(StandardCharsets.UTF_8);
        var filterExpr = andExpr(
            andExpr(
                andExpr(indexFilterExpr(q1), indexFilterExpr(q2)),
                indexFilterExpr(q3)),
            compExpr(predCol, predVal));

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filterExpr).build();
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(3)).build();
        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(compDecl)
            .addExtensions(andDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM t WHERE (index_filter(f1,v1) AND predCol op predVal1) OR (index_filter(f2,v2) AND predCol op predVal2)}
     * Top-level OR of two AND-groups, each with collector + numeric
     * predicate. Tests the tree path when OR saturation doesn't happen
     * (different AND-group filters select different subsets).
     */
    public byte[] buildTwoGroupOrCount(
        String tableName,
        String f1, String v1, String pred1Col, String pred1Op, int pred1Val,
        String f2, String v2, String pred2Col, String pred2Op, int pred2Val
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var compDecl = extDecl(1, compFuncName(pred1Op));
        var andDecl = extDecl(2, "and");
        var orDecl = extDecl(3, "or");
        var countDecl = extDecl(4, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (f1 + "\0" + v1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (f2 + "\0" + v2).getBytes(StandardCharsets.UTF_8);

        var group1 = andExpr(indexFilterExpr(q1), compExpr(pred1Col, pred1Val));
        var group2 = andExpr(indexFilterExpr(q2), compExpr(pred2Col, pred2Val));
        var filterExpr = Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3) // or
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(group1))
                .addArguments(FunctionArgument.newBuilder().setValue(group2))
        ).build();

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filterExpr).build();
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(4)).build();
        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(compDecl)
            .addExtensions(andDecl).addExtensions(orDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM table WHERE index_filter(f1,v1) AND index_filter(f2,v2) AND index_filter(f3,v3)}
     * Three collectors AND'd → Tree classification path. Stresses prefetch
     * pipelining: three Lucene calls per RG, should all be issued in parallel
     * if possible.
     */
    public byte[] buildThreeCollectorAndCount(
        String tableName,
        String f1, String v1,
        String f2, String v2,
        String f3, String v3
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var andDecl = extDecl(2, "and");
        var countDecl = extDecl(3, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (f1 + "\0" + v1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (f2 + "\0" + v2).getBytes(StandardCharsets.UTF_8);
        byte[] q3 = (f3 + "\0" + v3).getBytes(StandardCharsets.UTF_8);
        // AND(AND(c1, c2), c3) — substrait AND is binary, left-associate.
        var filterExpr = andExpr(
            andExpr(indexFilterExpr(q1), indexFilterExpr(q2)),
            indexFilterExpr(q3));
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filterExpr)
            .build();

        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(3))
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(andDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM table WHERE index_filter(f1,v1) OR index_filter(f2,v2) OR index_filter(f3,v3)}
     * Three collectors OR'd → Tree classification with OR saturation paths.
     * Tests whether OR-recovery materialises all bitmaps (Q12-style).
     */
    public byte[] buildThreeCollectorOrCount(
        String tableName,
        String f1, String v1,
        String f2, String v2,
        String f3, String v3
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var orDecl = extDecl(3, "or");
        var countDecl = extDecl(4, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (f1 + "\0" + v1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (f2 + "\0" + v2).getBytes(StandardCharsets.UTF_8);
        byte[] q3 = (f3 + "\0" + v3).getBytes(StandardCharsets.UTF_8);
        // OR(OR(c1, c2), c3)
        var or12 = Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q1)))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q2)))
        ).build();
        var or123 = Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(or12))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q3)))
        ).build();

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(or123).build();

        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(4)).build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(orDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM t WHERE (index_filter(f1,v1) OR index_filter(f2,v2)) AND (index_filter(f3,v3) OR index_filter(f4,v4))}
     * Two OR-groups AND'd, each group has two collectors → 4 collectors total,
     * deep-tree prefetch stress (Q12 on steroids).
     */
    public byte[] buildFourCollectorTreeCount(
        String tableName,
        String f1, String v1,
        String f2, String v2,
        String f3, String v3,
        String f4, String v4
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var andDecl = extDecl(2, "and");
        var orDecl = extDecl(3, "or");
        var countDecl = extDecl(4, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (f1 + "\0" + v1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (f2 + "\0" + v2).getBytes(StandardCharsets.UTF_8);
        byte[] q3 = (f3 + "\0" + v3).getBytes(StandardCharsets.UTF_8);
        byte[] q4 = (f4 + "\0" + v4).getBytes(StandardCharsets.UTF_8);
        var or12 = Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q1)))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q2)))
        ).build();
        var or34 = Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3)
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q3)))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q4)))
        ).build();
        var filterExpr = andExpr(or12, or34);

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filterExpr).build();

        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(4)).build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(andDecl)
            .addExtensions(orDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM t WHERE index_filter(f1,v1) AND index_filter(f2,v2) AND index_filter(f3,v3) AND index_filter(f4,v4)}
     * Four collectors AND'd → Tree classification path. Stresses prefetch
     * pipelining when many Lucene calls per RG must complete before decode.
     */
    public byte[] buildFourCollectorAndCount(
        String tableName,
        String f1, String v1,
        String f2, String v2,
        String f3, String v3,
        String f4, String v4
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var andDecl = extDecl(2, "and");
        var countDecl = extDecl(3, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (f1 + "\0" + v1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (f2 + "\0" + v2).getBytes(StandardCharsets.UTF_8);
        byte[] q3 = (f3 + "\0" + v3).getBytes(StandardCharsets.UTF_8);
        byte[] q4 = (f4 + "\0" + v4).getBytes(StandardCharsets.UTF_8);
        var filterExpr = andExpr(
            andExpr(
                andExpr(indexFilterExpr(q1), indexFilterExpr(q2)),
                indexFilterExpr(q3)),
            indexFilterExpr(q4));

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filterExpr).build();
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(3)).build();
        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(andDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM table WHERE index_filter(field1,val1) AND index_filter(field2,val2)}
     * Two collectors → Tree classification path.
     */
    public byte[] buildTwoCollectorAndCount(
        String tableName, String field1, String value1, String field2, String value2
    ) {
        var extUri = extUri();
        var indexFilterDecl = extDecl(0, "index_filter");
        var andDecl = extDecl(2, "and");
        var countDecl = extDecl(3, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (field1 + "\0" + value1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (field2 + "\0" + value2).getBytes(StandardCharsets.UTF_8);
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(andExpr(indexFilterExpr(q1), indexFilterExpr(q2)))
            .build();

        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(3))
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(andDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT COUNT(*) FROM t WHERE (index_filter(f1,v1) OR pred1Col op pred1Val) AND (index_filter(f2,v2) OR pred2Col op pred2Val)}
     * Mixed collector+predicate in OR branches → BitmapTree classification.
     */
    public byte[] buildMixedOrCount(
        String tableName,
        String f1, String v1, String pred1Col, String pred1Op, int pred1Val,
        String f2, String v2, String pred2Col, String pred2Op, int pred2Val
    ) {
        var extUri = extUri();
        // 0=index_filter, 1=comp (equal), 2=and, 3=or, 4=count
        var indexFilterDecl = extDecl(0, "index_filter");
        var compDecl = extDecl(1, compFuncName(pred1Op));
        var andDecl = extDecl(2, "and");
        var orDecl = extDecl(3, "or");
        var countDecl = extDecl(4, "count");

        var readRel = namedRead(tableName);
        byte[] q1 = (f1 + "\0" + v1).getBytes(StandardCharsets.UTF_8);
        byte[] q2 = (f2 + "\0" + v2).getBytes(StandardCharsets.UTF_8);

        // OR(index_filter(f1,v1), pred1Col op pred1Val)
        var or1 = Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3) // or
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q1)))
                .addArguments(FunctionArgument.newBuilder().setValue(compExpr(pred1Col, pred1Val)))
        ).build();
        // OR(index_filter(f2,v2), pred2Col op pred2Val)
        var or2 = Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3) // or
                .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()))
                .addArguments(FunctionArgument.newBuilder().setValue(indexFilterExpr(q2)))
                .addArguments(FunctionArgument.newBuilder().setValue(compExpr(pred2Col, pred2Val)))
        ).build();
        // AND(or1, or2)
        var filter = andExpr(or1, or2);

        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(filter).build();
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder())
            .addMeasures(countMeasure(4)).build();
        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames("cnt")).build();

        return Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(indexFilterDecl).addExtensions(compDecl)
            .addExtensions(andDecl).addExtensions(orDecl).addExtensions(countDecl)
            .addRelations(root).build().toByteArray();
    }

    // ── Shared expression builders ────────────────────────────────

    private SimpleExtensionURI extUri() {
        return SimpleExtensionURI.newBuilder()
            .setExtensionUriAnchor(1).setUri("/functions_opensearch.yaml").build();
    }

    private ReadRel namedRead(String tableName) {
        var namedStruct = NamedStruct.newBuilder()
            .addAllNames(columnNames)
            .setStruct(Type.Struct.newBuilder().addAllTypes(columnTypes));
        return ReadRel.newBuilder()
            .setBaseSchema(namedStruct)
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName)).build();
    }

    private AggregateRel.Measure countMeasure(int funcRef) {
        return AggregateRel.Measure.newBuilder()
            .setMeasure(AggregateFunction.newBuilder()
                .setFunctionReference(funcRef)
                .setInvocation(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL)
                .addArguments(FunctionArgument.newBuilder()
                    .setValue(Expression.newBuilder()
                        .setLiteral(Expression.Literal.newBuilder().setI64(1)))))
            .build();
    }

    private AggregateRel.Measure sumMeasure(int funcRef, int colIdx) {
        return AggregateRel.Measure.newBuilder()
            .setMeasure(AggregateFunction.newBuilder()
                .setFunctionReference(funcRef)
                .setInvocation(AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL)
                .addArguments(FunctionArgument.newBuilder().setValue(fieldRef(colIdx))))
            .build();
    }

    /**
     * Build: {@code SELECT groupCol, COUNT(*), SUM(sumCol) FROM t WHERE index_filter(f,v) AND predCol op predVal GROUP BY groupCol}
     */
    public byte[] buildCollectorPredicateGroupByCountSum(
        String tableName, String field, String value,
        String predColumn, String predOp, int predValue,
        String groupColumn, String sumColumn
    ) {
        var extUri = extUri();
        // 0=index_filter, 1=comp, 2=and, 3=count, 4=sum
        var readRel = namedRead(tableName);
        byte[] queryBytes = (field + "\0" + value).getBytes(StandardCharsets.UTF_8);
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(andExpr(indexFilterExpr(queryBytes), compExpr(predColumn, predValue)))
            .build();

        int groupColIdx = columnNames.indexOf(groupColumn);
        int sumColIdx = columnNames.indexOf(sumColumn);
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder().addGroupingExpressions(fieldRef(groupColIdx)))
            .addMeasures(countMeasure(3))
            .addMeasures(sumMeasure(4, sumColIdx))
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames(groupColumn).addNames("cnt").addNames("total")).build();

        return io.substrait.proto.Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(extDecl(0, "index_filter"))
            .addExtensions(extDecl(1, compFuncName(predOp)))
            .addExtensions(extDecl(2, "and"))
            .addExtensions(extDecl(3, "count"))
            .addExtensions(extDecl(4, "sum"))
            .addRelations(root).build().toByteArray();
    }

    /**
     * Build: {@code SELECT groupCol, SUM(sumCol) FROM t WHERE index_filter(f,v) GROUP BY groupCol}
     */
    public byte[] buildCollectorOnlyGroupBySum(
        String tableName, String field, String value,
        String groupColumn, String sumColumn
    ) {
        var extUri = extUri();
        var readRel = namedRead(tableName);
        byte[] queryBytes = (field + "\0" + value).getBytes(StandardCharsets.UTF_8);
        var filterRel = FilterRel.newBuilder()
            .setInput(Rel.newBuilder().setRead(readRel))
            .setCondition(indexFilterExpr(queryBytes)).build();

        int groupColIdx = columnNames.indexOf(groupColumn);
        int sumColIdx = columnNames.indexOf(sumColumn);
        var aggRel = AggregateRel.newBuilder()
            .setInput(Rel.newBuilder().setFilter(filterRel))
            .addGroupings(AggregateRel.Grouping.newBuilder().addGroupingExpressions(fieldRef(groupColIdx)))
            .addMeasures(sumMeasure(1, sumColIdx))
            .build();

        var root = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setAggregate(aggRel))
                .addNames(groupColumn).addNames("total")).build();

        return io.substrait.proto.Plan.newBuilder()
            .addExtensionUris(extUri)
            .addExtensions(extDecl(0, "index_filter"))
            .addExtensions(extDecl(1, "sum"))
            .addRelations(root).build().toByteArray();
    }

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

    private Expression orExpr(Expression left, Expression right) {
        return Expression.newBuilder().setScalarFunction(
            Expression.ScalarFunction.newBuilder()
                .setFunctionReference(3)
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
