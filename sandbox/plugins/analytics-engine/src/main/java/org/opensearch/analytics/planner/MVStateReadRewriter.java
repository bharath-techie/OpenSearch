/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.MVReadTarget;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Answers a query from a materialized view's stored partial aggregate states.
 *
 * <p>The front-end matches the query text to a fresh view's definition and attaches an
 * {@link MVReadTarget} (view index + its {@code index.parquet.mv.spec}). This rewriter
 * then performs one surgical, post-Volcano plan transform: the shard-side
 * <b>PARTIAL aggregate subtree</b> — the aggregation over the raw source, plus everything
 * below it — is replaced by a scan of the view's state columns, projected and cast to the
 * PARTIAL's exact output row type. Nothing above changes: the FINAL aggregate folds the
 * stored states positionally exactly as it folds shard-produced partial states (the same
 * merge path every distributed query uses), and the definition's finalize projections
 * (avg's divide, stddev's sqrt — pre-decomposed by Calcite's reduce rule) reapply
 * unchanged.
 *
 * <p>Because the spec was derived from the definition's own prepared plan and this query
 * replans the identical definition through the identical pipeline, the FINAL calls align
 * ordinally with {@code spec.aggs}; each alignment is still verified (function kind per
 * call, group keys against {@code key_columns}) and any mismatch abandons the rewrite —
 * the query then runs against the source, which is always correct.
 *
 * <p>The view's {@code __mv_partition} span column is intentionally <em>not</em> scanned:
 * states are associative, so the FINAL fold collapses across partitions by simply
 * grouping on the user keys only.
 *
 * @opensearch.internal
 */
public final class MVStateReadRewriter {

    private static final Logger logger = LogManager.getLogger(MVStateReadRewriter.class);

    private static final String PARTITION_COLUMN = "__mv_partition";
    private static final String STATE_SUFFIX = "__st_0";

    private MVStateReadRewriter() {}

    /**
     * Attempts the rewrite; returns the original plan untouched when any precondition
     * fails (shape mismatch, spec drift, unresolvable view table).
     */
    public static RelNode rewrite(RelNode plan, MVReadTarget target, PlannerContext context) {
        try {
            RelNode rewritten = doRewrite(plan, target, context);
            if (rewritten != null) {
                logger.info("[MV-READ] answering from view [{}] state columns", target.viewIndex());
                return rewritten;
            }
        } catch (Exception e) {
            logger.warn("[MV-READ] rewrite for view [{}] failed; answering from source", target.viewIndex(), e);
        }
        return plan;
    }

    @SuppressWarnings("unchecked")
    private static RelNode doRewrite(RelNode plan, MVReadTarget target, PlannerContext context) {
        Map<String, Object> spec = XContentHelper.convertToMap(new BytesArray(target.specJson()), false, MediaTypeRegistry.JSON).v2();
        List<String> keyColumns = (List<String>) spec.get("key_columns");
        List<Map<String, Object>> specAggs = (List<Map<String, Object>>) spec.get("aggs");
        if (keyColumns == null || specAggs == null || specAggs.isEmpty()) {
            return null;
        }
        List<String> userKeys = keyColumns.stream().filter(k -> PARTITION_COLUMN.equals(k) == false).toList();

        // The FINAL/PARTIAL pair must exist; single-shard plans carry a SINGLE aggregate,
        // so force the same split state emission forces (identical construction).
        RelNode split = OpenSearchAggregateSplitRule.forceStateEmissionSplit(plan, context);
        Located located = locate(split);
        if (located == null) {
            return null;
        }
        OpenSearchAggregate finalAgg = located.finalAgg;
        OpenSearchAggregate partial = located.partial;

        // Alignment checks — spec and plan derive from the same definition + pipeline,
        // but verify rather than trust.
        if (finalAgg.getAggCallList().size() != specAggs.size()) {
            logger.debug("[MV-READ] call count mismatch: plan={}, spec={}", finalAgg.getAggCallList().size(), specAggs.size());
            return null;
        }
        int groupCount = finalAgg.getGroupSet().cardinality();
        if (groupCount != userKeys.size()) {
            logger.debug("[MV-READ] group key count mismatch: plan={}, spec={}", groupCount, userKeys.size());
            return null;
        }
        for (int i = 0; i < specAggs.size(); i++) {
            String planFn = finalAgg.getAggCallList().get(i).getAggregation().getName();
            String specFn = String.valueOf(specAggs.get(i).get("fn"));
            if (planFn.equalsIgnoreCase(specFn) == false && matchesKnownForm(planFn, specFn) == false) {
                logger.debug("[MV-READ] call {} function mismatch: plan={}, spec={}", i, planFn, specFn);
                return null;
            }
        }

        // Resolve the view table through the same catalog the query was planned against.
        RelNode sourceScan = findScan(partial);
        if (sourceScan == null || sourceScan.getTable() == null) {
            return null;
        }
        List<String> qualified = new ArrayList<>(sourceScan.getTable().getQualifiedName());
        qualified.set(qualified.size() - 1, target.viewIndex());
        RelOptTable resolved = sourceScan.getTable().getRelOptSchema() == null
            ? null
            : sourceScan.getTable().getRelOptSchema().getTableForMember(qualified);
        if (resolved == null) {
            logger.debug("[MV-READ] view table [{}] not resolvable", target.viewIndex());
            return null;
        }
        // Fragments carry bare index names on the wire (the shard session resolves the
        // last path element); catalog readers return catalog-rooted names — normalize.
        RelOptTable viewTable = new BareNameTable(resolved, target.viewIndex());

        // Scan column order = the PARTIAL's positional output contract: fronted group
        // keys, then one state column per call (spec aggs are single-state by
        // construction: Calcite pre-decomposes multi-state functions, engine-native
        // functions use one opaque column).
        List<String> scanColumns = new ArrayList<>(groupCount + specAggs.size());
        RelDataType viewRowType = viewTable.getRowType();
        RelDataType partialRowType = partial.getRowType();
        for (int i = 0; i < groupCount; i++) {
            // PARTIAL fronts the group keys; their output names are the source column names.
            scanColumns.add(partialRowType.getFieldList().get(i).getName());
        }
        for (Map<String, Object> agg : specAggs) {
            scanColumns.add(agg.get("output") + STATE_SUFFIX);
        }
        for (String column : scanColumns) {
            if (viewRowType.getField(column, false, false) == null) {
                logger.debug("[MV-READ] view [{}] lacks column [{}]", target.viewIndex(), column);
                return null;
            }
        }

        // Build: scan(view) → project [keys…, states…] cast to the PARTIAL's row type.
        IndexResolution resolution = IndexResolution.resolve(target.viewIndex(), context.getClusterState());
        List<IndexMetadata> indexMetadata = resolution.concreteIndices();
        FieldStorageResolver storageResolver = context.getCapabilityRegistry().resolveFieldStorage(indexMetadata);
        List<String> viewFieldNames = viewRowType.getFieldList().stream().map(RelDataTypeField::getName).toList();
        List<FieldStorageInfo> fieldStorage = storageResolver.resolve(viewFieldNames);
        int shardCount = indexMetadata.stream().mapToInt(IndexMetadata::getNumberOfShards).sum();

        OpenSearchTableScan viewScan = OpenSearchTableScan.create(
            partial.getCluster(),
            viewTable,
            ((OpenSearchTableScan) sourceScan).getViableBackends(),
            fieldStorage,
            Math.max(shardCount, 1),
            context.getDistributionTraitDef()
        );

        RexBuilder rexBuilder = partial.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = partial.getCluster().getTypeFactory();
        List<org.opensearch.analytics.spi.AggregateFunction.IntermediateField> intermediateFields = finalAgg.getIntermediateFields();
        List<RexNode> exprs = new ArrayList<>(scanColumns.size());
        RelDataTypeFactory.Builder projectType = typeFactory.builder();
        for (int i = 0; i < scanColumns.size(); i++) {
            RelDataTypeField viewField = viewRowType.getField(scanColumns.get(i), false, false);
            RelDataTypeField partialField = partialRowType.getFieldList().get(i);
            // The wire type each call's state crosses the exchange as: engine-native
            // calls (HLL sketches) re-type to their intermediate form (the same
            // resolution overrideExchangeType applies to the FINAL's stage input);
            // everything else uses the PARTIAL's declared output type.
            RelDataType targetType = partialField.getType();
            int callIdx = i - groupCount;
            if (callIdx >= 0 && intermediateFields != null && callIdx < intermediateFields.size()) {
                org.opensearch.analytics.spi.AggregateFunction.IntermediateField field = intermediateFields.get(callIdx);
                if (field != null) {
                    targetType = field.typeResolver().resolve(List.of(viewField.getType()), typeFactory);
                }
            }
            RexNode ref = rexBuilder.makeInputRef(viewField.getType(), viewField.getIndex());
            // Mapping layers widen state types (Int16 min-state stored as long); cast back
            // to the wire type so the FINAL consumes exactly what it expects.
            RexNode expr = viewField.getType().getSqlTypeName() == targetType.getSqlTypeName() ? ref : rexBuilder.makeCast(targetType, ref);
            exprs.add(expr);
            projectType.add(partialField.getName(), expr.getType());
        }
        org.opensearch.analytics.planner.rel.OpenSearchProject stateProject = new org.opensearch.analytics.planner.rel.OpenSearchProject(
            partial.getCluster(),
            viewScan.getTraitSet(),
            viewScan,
            exprs,
            projectType.build(),
            ((OpenSearchTableScan) sourceScan).getViableBackends()
        );

        // Swap the PARTIAL subtree under the exchange; everything above is untouched.
        RelNode newReducer = located.reducer.copy(located.reducer.getTraitSet(), List.of(stateProject));
        return replaceChild(split, located.reducer, newReducer);
    }

    /** FINAL aggregate → exchange reducer → PARTIAL aggregate, through single-input wrappers. */
    private record Located(OpenSearchAggregate finalAgg, OpenSearchExchangeReducer reducer, OpenSearchAggregate partial) {
    }

    private static Located locate(RelNode node) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) {
            RelNode child = agg.getInput();
            if (child instanceof OpenSearchExchangeReducer reducer) {
                RelNode grandchild = reducer.getInput();
                if (grandchild instanceof OpenSearchAggregate p && p.getMode() == AggregateMode.PARTIAL) {
                    return new Located(agg, reducer, p);
                }
            }
            return null;
        }
        for (RelNode input : node.getInputs()) {
            Located found = locate(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static RelNode findScan(RelNode node) {
        if (node instanceof OpenSearchTableScan) {
            return node;
        }
        for (RelNode input : node.getInputs()) {
            RelNode found = findScan(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static RelNode replaceChild(RelNode node, RelNode oldChild, RelNode newChild) {
        if (node == oldChild) {
            return newChild;
        }
        List<RelNode> inputs = node.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            RelNode rewritten = replaceChild(inputs.get(i), oldChild, newChild);
            if (rewritten != inputs.get(i)) {
                List<RelNode> newInputs = new ArrayList<>(inputs);
                newInputs.set(i, rewritten);
                return node.copy(node.getTraitSet(), newInputs);
            }
        }
        return node;
    }

    /**
     * Calcite aggregate names and DataFusion registry names differ for known merge
     * forms: COUNT's merge is SUM (the reduce fragment declares SUM over the partial
     * count, and the spec records the fragment's call); APPROX_COUNT_DISTINCT lowers to
     * DataFusion's {@code approx_distinct}.
     */
    private static boolean matchesKnownForm(String planFn, String specFn) {
        if ("COUNT".equalsIgnoreCase(planFn) && "sum".equalsIgnoreCase(specFn)) {
            return true;
        }
        return "APPROX_COUNT_DISTINCT".equalsIgnoreCase(planFn) && "approx_distinct".equalsIgnoreCase(specFn);
    }

    /**
     * Delegating {@link RelOptTable} that reports a single-element qualified name.
     * Catalog readers fully qualify resolved tables ({@code [opensearch, view]}), but
     * fragments carry bare index names on the wire — shard sessions resolve the last
     * path element only, and a catalog prefix fails schema resolution at the reduce.
     */
    private record BareNameTable(RelOptTable delegate, String name) implements RelOptTable {
        @Override
        public List<String> getQualifiedName() {
            return List.of(name);
        }

        @Override
        public double getRowCount() {
            return delegate.getRowCount();
        }

        @Override
        public RelDataType getRowType() {
            return delegate.getRowType();
        }

        @Override
        public org.apache.calcite.plan.RelOptSchema getRelOptSchema() {
            return delegate.getRelOptSchema();
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            return delegate.toRel(context);
        }

        @Override
        public List<org.apache.calcite.rel.RelCollation> getCollationList() {
            return delegate.getCollationList();
        }

        @Override
        public org.apache.calcite.rel.RelDistribution getDistribution() {
            return delegate.getDistribution();
        }

        @Override
        public boolean isKey(org.apache.calcite.util.ImmutableBitSet columns) {
            return delegate.isKey(columns);
        }

        @Override
        public List<org.apache.calcite.rel.RelReferentialConstraint> getReferentialConstraints() {
            return delegate.getReferentialConstraints();
        }

        @Override
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return delegate.getExpression(clazz);
        }

        @Override
        public RelOptTable extend(List<org.apache.calcite.rel.type.RelDataTypeField> extendedFields) {
            return delegate.extend(extendedFields);
        }

        @Override
        public List<org.apache.calcite.schema.ColumnStrategy> getColumnStrategies() {
            return delegate.getColumnStrategies();
        }

        @Override
        public <C> C unwrap(Class<C> clazz) {
            if (clazz.isInstance(this)) {
                return clazz.cast(this);
            }
            return delegate.unwrap(clazz);
        }

        @Override
        public List<org.apache.calcite.util.ImmutableBitSet> getKeys() {
            return delegate.getKeys();
        }
    }
}
