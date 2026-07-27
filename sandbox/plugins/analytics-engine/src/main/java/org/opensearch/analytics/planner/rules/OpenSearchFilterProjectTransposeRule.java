/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Pushes filters below projects without duplicating computed expressions used by non-null guards.
 *
 * <p>PPL adds {@code IS NOT NULL} predicates for non-nullable aggregation buckets. When a bucket is
 * a computed alias, Calcite's stock transpose rule expands the alias into the filter while retaining
 * the project, causing expensive expressions such as {@code REGEXP_REPLACE} to run twice per row.
 * This rule keeps those guards above the project and still pushes independent conjuncts down.
 *
 * <p>Projects containing non-deterministic expressions are excluded entirely because evaluating
 * those expressions again in a pushed filter would change query semantics.
 */
public final class OpenSearchFilterProjectTransposeRule extends FilterProjectTransposeRule {

    public static final OpenSearchFilterProjectTransposeRule INSTANCE = new OpenSearchFilterProjectTransposeRule();
    private static final Set<String> NULL_PROPAGATING_FUNCTIONS = Set.of("ABS", "DATE_FORMAT", "EXTRACT", "REGEXP_REPLACE");

    private OpenSearchFilterProjectTransposeRule() {
        super(
            FilterProjectTransposeRule.Config.DEFAULT.withOperandFor(
                Filter.class,
                filter -> true,
                Project.class,
                project -> project.getProjects().stream().allMatch(RexUtil::isDeterministic)
            )
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Project project = call.rel(1);
        List<RexNode> retained = new ArrayList<>();
        List<RexNode> pushable = new ArrayList<>();
        List<RexNode> derivedInputGuards = new ArrayList<>();
        boolean rewroteNullGuard = false;

        for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
            RexNode computedExpression = computedAliasNullGuardExpression(conjunct, project);
            if (computedExpression == null) {
                pushable.add(conjunct);
            } else if (deriveInputNullGuards(computedExpression, derivedInputGuards, filter)) {
                // A strict function is non-null exactly when its nullable inputs are non-null.
                rewroteNullGuard = true;
            } else {
                retained.add(conjunct);
            }
        }

        if (rewroteNullGuard == false && retained.isEmpty() && derivedInputGuards.isEmpty()) {
            super.onMatch(call);
            return;
        }

        List<RexNode> inputConditions = new ArrayList<>(derivedInputGuards);
        if (pushable.isEmpty() == false) {
            RexNode pushedCondition = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), pushable);
            pushedCondition = RelOptUtil.pushPastProjectUnlessBloat(pushedCondition, project, config.bloat());
            if (pushedCondition == null) {
                return;
            }
            inputConditions.addAll(RelOptUtil.conjunctions(pushedCondition));
        }

        RelNode projectInput = project.getInput();
        RelNode input = projectInput;
        if (inputConditions.isEmpty() == false) {
            RelTraitSet pushedFilterTraits = filter.getTraitSet()
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE,
                    () -> Collections.singletonList(projectInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE))
                )
                .replaceIfs(
                    RelDistributionTraitDef.INSTANCE,
                    () -> Collections.singletonList(projectInput.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE))
                );
            RexNode pushedCondition = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), inputConditions);
            pushedCondition = RexUtil.removeNullabilityCast(call.builder().getTypeFactory(), pushedCondition);
            input = filter.copy(pushedFilterTraits, projectInput, pushedCondition);
        }

        RelNode newProject = project.copy(project.getTraitSet(), input, project.getProjects(), project.getRowType());
        if (retained.isEmpty()) {
            call.transformTo(newProject);
        } else {
            RexNode retainedCondition = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), retained);
            call.transformTo(filter.copy(filter.getTraitSet(), newProject, retainedCondition));
        }
    }

    private static RexNode computedAliasNullGuardExpression(RexNode conjunct, Project project) {
        if ((conjunct instanceof RexCall) == false || conjunct.isA(SqlKind.IS_NOT_NULL) == false) {
            return null;
        }
        RexNode operand = ((RexCall) conjunct).getOperands().getFirst();
        if ((operand instanceof RexInputRef inputRef) == false) {
            return null;
        }
        RexNode projectExpression = project.getProjects().get(((RexInputRef) operand).getIndex());
        return projectExpression instanceof RexInputRef ? null : projectExpression;
    }

    /**
     * Rewrite {@code f(x) IS NOT NULL} into guards on {@code f}'s nullable inputs, when {@code f}
     * returns null only if an input is null. Returns false (keep the guard as-is) if that cannot be
     * established.
     *
     * <p>Nullness propagation is decided by Calcite's {@link Strong} analysis rather than an operator
     * name list: {@link Strong.Policy#ANY} means "null when any operand is null", which is exactly
     * the property that makes {@code f(x) IS NOT NULL} equivalent to {@code x IS NOT NULL}. That
     * covers arithmetic ({@code x - 1}), comparisons and casts. Named functions land on
     * {@code Policy.AS_IS} (Calcite makes no claim), so the strict ones we rely on are still listed
     * explicitly in {@link #NULL_PROPAGATING_FUNCTIONS}.
     */
    private static boolean deriveInputNullGuards(RexNode expression, List<RexNode> guards, Filter filter) {
        if ((expression instanceof RexCall) == false) {
            return false;
        }
        RexCall call = (RexCall) expression;
        boolean nullPropagating = Strong.policy(call.getOperator()) == Strong.Policy.ANY
            || NULL_PROPAGATING_FUNCTIONS.contains(call.getOperator().getName().toUpperCase(Locale.ROOT));
        if (nullPropagating == false) {
            return false;
        }

        List<RexNode> derived = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            if (deriveOperandNullGuards(operand, derived, filter) == false) {
                return false;
            }
        }
        guards.addAll(derived);
        return true;
    }

    private static boolean deriveOperandNullGuards(RexNode operand, List<RexNode> guards, Filter filter) {
        if (operand instanceof RexInputRef) {
            if (operand.getType().isNullable()) {
                guards.add(filter.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
            }
            return true;
        }
        if (operand instanceof RexLiteral literal) {
            return literal.isNull() == false;
        }
        if (operand instanceof RexCall call && call.isA(SqlKind.CAST)) {
            return deriveOperandNullGuards(call.getOperands().getFirst(), guards, filter);
        }
        // Nested null-propagating call (e.g. CAST(x) - 1 inside another expression): recurse so the
        // guard collapses to the leaf column rather than being abandoned.
        if (operand instanceof RexCall nested && Strong.policy(nested.getOperator()) == Strong.Policy.ANY) {
            for (RexNode inner : nested.getOperands()) {
                if (deriveOperandNullGuards(inner, guards, filter) == false) {
                    return false;
                }
            }
            return true;
        }
        return operand.getType().isNullable() == false;
    }
}
