/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
Converts a DataFusion logical filter expression into a `BoolNode` tree.

After Substrait is decoded into a DataFusion `LogicalPlan`, the filter
expression is a tree of `Expr` nodes. This module walks that tree and
classifies each node:

- `AND` / `OR` / `NOT` → `BoolNode::And` / `Or` / `Not`
- `ScalarFunction` named `COLLECTOR_FUNCTION_NAME` → `BoolNode::Collector`
    The function carries `provider_id` and `collector_idx` as integer literal args.
- Any other comparison (`=`, `>`, `<`, etc.) → `BoolNode::Predicate`
    The predicate details (column, op, value) are stored in a side vec
    and the tree references them by index.

This replaces the Java-side tree construction + serialization + Rust-side
deserialization with a single Rust-side extraction from the Substrait plan.
**/

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};

use super::bool_tree::{BoolNode, ResolvedPredicate};

/// The Substrait extension function name that marks a collector leaf.
/// The planner embeds serialized Lucene query builders as arguments to
/// this function in the Rex/Substrait expression tree.
pub const COLLECTOR_FUNCTION_NAME: &str = "index_filter";

/// Result of converting a filter expression to a BoolNode tree.
pub struct ExtractionResult {
    pub tree: BoolNode,
    pub predicates: Vec<ResolvedPredicate>,
}

/// Extract a `BoolNode` tree from a DataFusion filter `Expr`.
///
/// Walks the expression tree, classifying nodes into Collector leaves
/// (recognized by function name) and Predicate leaves (standard comparisons).
pub fn expr_to_bool_tree(expr: &Expr) -> Result<ExtractionResult, String> {
    let mut predicates = Vec::new();
    let tree = convert_expr(expr, &mut predicates)?;
    Ok(ExtractionResult { tree, predicates })
}

fn convert_expr(expr: &Expr, predicates: &mut Vec<ResolvedPredicate>) -> Result<BoolNode, String> {
    match expr {
        Expr::BinaryExpr(binary) => {
            match binary.op {
                Operator::And => {
                    let left = convert_expr(&binary.left, predicates)?;
                    let right = convert_expr(&binary.right, predicates)?;
                    Ok(BoolNode::And(vec![left, right]))
                }
                Operator::Or => {
                    let left = convert_expr(&binary.left, predicates)?;
                    let right = convert_expr(&binary.right, predicates)?;
                    Ok(BoolNode::Or(vec![left, right]))
                }
                // Comparison operators → PredicateLeaf
                Operator::Eq | Operator::NotEq | Operator::Lt
                | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                    convert_comparison(&binary.left, binary.op, &binary.right, predicates)
                }
                _ => Err(format!("unsupported binary operator: {:?}", binary.op)),
            }
        }
        Expr::Not(inner) => {
            let child = convert_expr(inner, predicates)?;
            Ok(BoolNode::Not(Box::new(child)))
        }
        Expr::ScalarFunction(func) => {
            if func.name() == COLLECTOR_FUNCTION_NAME {
                convert_collector_function(&func.args)
            } else {
                Err(format!("unsupported scalar function: {}", func.name()))
            }
        }
        _ => Err(format!("unsupported expression type: {:?}", expr)),
    }
}

/// Convert a comparison expression to a PredicateLeaf.
/// Expects one side to be a Column and the other a Literal.
fn convert_comparison(
    left: &Expr,
    op: Operator,
    right: &Expr,
    predicates: &mut Vec<ResolvedPredicate>,
) -> Result<BoolNode, String> {
    let (column, value) = match (left, right) {
        (Expr::Column(col), Expr::Literal(val, _)) => (col.name().to_string(), val.clone()),
        (Expr::Literal(val, _), Expr::Column(col)) => (col.name().to_string(), val.clone()),
        _ => return Err(format!("comparison must be column op literal, got: {:?} {:?} {:?}", left, op, right)),
    };
    let pred_id = predicates.len() as u16;
    predicates.push(ResolvedPredicate { column, op, value });
    Ok(BoolNode::Predicate { predicate_id: pred_id })
}

/// Convert a collector function call to a CollectorLeaf.
/// Expected args: index_filter(provider_id, collector_idx)
fn convert_collector_function(args: &[Expr]) -> Result<BoolNode, String> {
    if args.len() < 2 {
        return Err(format!(
            "index_filter expects 2 args (provider_id, collector_idx), got {}",
            args.len()
        ));
    }
    let provider_id = extract_int_literal(&args[0], "provider_id")? as u16;
    let collector_idx = extract_int_literal(&args[1], "collector_idx")? as usize;
    Ok(BoolNode::Collector { provider_id, collector_idx })
}

fn extract_int_literal(expr: &Expr, name: &str) -> Result<i64, String> {
    match expr {
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v as i64),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(*v),
        Expr::Literal(ScalarValue::Int16(Some(v)), _) => Ok(*v as i64),
        Expr::Literal(ScalarValue::Int8(Some(v)), _) => Ok(*v as i64),
        _ => Err(format!("{} must be an integer literal, got: {:?}", name, expr)),
    }
}

/// Extract the filter expression from a DataFusion LogicalPlan.
/// Walks down through Projection nodes to find the Filter node.
pub fn extract_filter_expr(plan: &datafusion::logical_expr::LogicalPlan) -> Option<Expr> {
    use datafusion::logical_expr::LogicalPlan;
    match plan {
        LogicalPlan::Filter(filter) => Some(filter.predicate.clone()),
        _ => {
            for input in plan.inputs() {
                if let Some(expr) = extract_filter_expr(input) {
                    return Some(expr);
                }
            }
            None
        }
    }
}

/// Create a DataFusion ScalarUDF for `index_filter(provider_id, collector_idx)`.
/// This UDF is a no-op at execution time — it exists only so that SQL containing
/// `index_filter(0, 0)` can be parsed and converted to Substrait. The Rust-side
/// tree extractor recognizes it and converts it to a `BoolNode::Collector`.
pub fn create_index_filter_udf() -> datafusion::logical_expr::ScalarUDF {
    use datafusion::logical_expr::ScalarUDF;
    ScalarUDF::new_from_impl(IndexFilterUdfImpl)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IndexFilterUdfImpl;

impl datafusion::logical_expr::ScalarUDFImpl for IndexFilterUdfImpl {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { COLLECTOR_FUNCTION_NAME }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        static SIG: std::sync::OnceLock<datafusion::logical_expr::Signature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| {
            datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            )
        })
    }
    fn return_type(&self, _: &[datafusion::arrow::datatypes::DataType]) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Boolean)
    }
    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> datafusion::common::Result<datafusion::physical_plan::ColumnarValue> {
        // At execution time, return all-true (the tree evaluator handles the real logic)
        let len = args.args.first()
            .map(|a| match a { datafusion::physical_plan::ColumnarValue::Array(a) => a.len(), _ => 1 })
            .unwrap_or(1);
        Ok(datafusion::physical_plan::ColumnarValue::Array(
            std::sync::Arc::new(datafusion::arrow::array::BooleanArray::from(vec![true; len]))
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit, Operator};

    #[test]
    fn test_simple_predicate() {
        let expr = col("price").gt(lit(100i32));
        let result = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(result.tree, BoolNode::Predicate { predicate_id: 0 }));
        assert_eq!(result.predicates.len(), 1);
        assert_eq!(result.predicates[0].column, "price");
        assert_eq!(result.predicates[0].op, Operator::Gt);
    }

    #[test]
    fn test_and_of_predicates() {
        let expr = col("price").gt(lit(100i32)).and(col("qty").lt(lit(50i32)));
        let result = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(result.tree, BoolNode::And(_)));
        assert_eq!(result.predicates.len(), 2);
    }

    #[test]
    fn test_not_predicate() {
        let expr = Expr::Not(Box::new(col("active").eq(lit(true))));
        let result = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(result.tree, BoolNode::Not(_)));
    }

    #[test]
    fn test_collector_function() {
        use datafusion::logical_expr::ScalarUDF;
        use std::sync::Arc;

        // Create a mock UDF named "index_filter"
        let udf = ScalarUDF::new_from_impl(MockIndexFilterUdf);
        let expr = Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(udf),
                vec![lit(0i32), lit(1i32)],
            )
        );
        let result = expr_to_bool_tree(&expr).unwrap();
        match &result.tree {
            BoolNode::Collector { provider_id, collector_idx } => {
                assert_eq!(*provider_id, 0);
                assert_eq!(*collector_idx, 1);
            }
            _ => panic!("expected Collector"),
        }
    }

    #[test]
    fn test_mixed_tree() {
        // AND(collector(0,0), OR(price > 100, qty < 50))
        use datafusion::logical_expr::ScalarUDF;
        use std::sync::Arc;

        let udf = ScalarUDF::new_from_impl(MockIndexFilterUdf);
        let collector = Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(udf),
                vec![lit(0i32), lit(0i32)],
            )
        );
        let pred_or = col("price").gt(lit(100i32)).or(col("qty").lt(lit(50i32)));
        let expr = Expr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(collector),
            Operator::And,
            Box::new(pred_or),
        ));
        let result = expr_to_bool_tree(&expr).unwrap();
        assert!(matches!(result.tree, BoolNode::And(_)));
        assert_eq!(result.predicates.len(), 2);
        if let BoolNode::And(children) = &result.tree {
            assert!(matches!(children[0], BoolNode::Collector { .. }));
            assert!(matches!(children[1], BoolNode::Or(_)));
        }
    }

    // Minimal UDF impl for testing
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockIndexFilterUdf;

    impl datafusion::logical_expr::ScalarUDFImpl for MockIndexFilterUdf {
        fn as_any(&self) -> &dyn std::any::Any { self }
        fn name(&self) -> &str { COLLECTOR_FUNCTION_NAME }
        fn signature(&self) -> &datafusion::logical_expr::Signature {
            static SIG: std::sync::OnceLock<datafusion::logical_expr::Signature> = std::sync::OnceLock::new();
            SIG.get_or_init(|| {
                datafusion::logical_expr::Signature::variadic_any(
                    datafusion::logical_expr::Volatility::Immutable,
                )
            })
        }
        fn return_type(&self, _: &[datafusion::arrow::datatypes::DataType]) -> datafusion::common::Result<datafusion::arrow::datatypes::DataType> {
            Ok(datafusion::arrow::datatypes::DataType::Boolean)
        }
        fn invoke_with_args(&self, _args: datafusion::logical_expr::ScalarFunctionArgs) -> datafusion::common::Result<datafusion::physical_plan::ColumnarValue> {
            unimplemented!("mock only")
        }
    }
}
