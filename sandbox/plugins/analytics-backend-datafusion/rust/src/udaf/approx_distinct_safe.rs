/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Safe `approx_distinct` override — fixes https://github.com/apache/datafusion/pull/21064
//! by routing Utf8View through consistent &str hashing.
//!
//! TODO: Remove once fixed upstream (likely DataFusion v55/v56).
//! TODO: Evaluate if DF's UDAF extension points (e.g. AccumulatorArgs overrides or
//!       custom PhysicalOptimizerRule to inject CastExec) can avoid same-name overrides.

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{downcast_value, Result};
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(datafusion::logical_expr::AggregateUDF::from(
        SafeApproxDistinct::new(),
    ));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SafeApproxDistinct {
    signature: Signature,
}

impl SafeApproxDistinct {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SafeApproxDistinct {
    fn name(&self) -> &str {
        "approx_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        approx_distinct_udaf().inner().return_type(args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        approx_distinct_udaf().inner().state_fields(args)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.expr_fields[0].data_type();
        if matches!(data_type, DataType::Utf8View) {
            // Wrap DF's Utf8 accumulator, feed it materialized &str from StringViewArray
            let utf8_args = AccumulatorArgs {
                return_field: acc_args.return_field,
                schema: acc_args.schema,
                ignore_nulls: acc_args.ignore_nulls,
                order_bys: acc_args.order_bys,
                name: acc_args.name,
                is_distinct: acc_args.is_distinct,
                exprs: acc_args.exprs,
                expr_fields: &[Arc::new(Field::new("x", DataType::Utf8, true))],
                is_reversed: acc_args.is_reversed,
            };
            let inner = approx_distinct_udaf().inner().accumulator(utf8_args)?;
            Ok(Box::new(Utf8ViewToUtf8Accumulator { inner }))
        } else {
            approx_distinct_udaf().inner().accumulator(acc_args)
        }
    }

    fn documentation(&self) -> Option<&datafusion::logical_expr::Documentation> {
        None
    }
}

/// Wraps DF's Utf8 HLL accumulator, converting Utf8View batches to StringArray on the fly.
#[derive(Debug)]
struct Utf8ViewToUtf8Accumulator {
    inner: Box<dyn Accumulator>,
}

impl Accumulator for Utf8ViewToUtf8Accumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let view_array: &StringViewArray = downcast_value!(values[0], StringViewArray);
        // Materialize as StringArray — consistent hashing via StringHLLAccumulator
        let string_array: StringArray = view_array.iter().collect();
        self.inner
            .update_batch(&[Arc::new(string_array) as ArrayRef])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}


#[cfg(test)]
mod merge_compat_tests {
    use super::*;
    use datafusion::execution::FunctionRegistry;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    /// The materialized-view merge-compatibility convention: a UDAF that SHADOWS a
    /// built-in registry name must expose the built-in's exact state layout, because
    /// the parquet PartialReduce merge resolves functions from a PLAIN registry by the
    /// spec's `fn` name and folds the stored state columns with whatever it resolves.
    /// If this test fails, views built with the custom function would be folded with an
    /// incompatible accumulator — fix the state_fields delegation, never the test.
    #[test]
    fn shadowing_udaf_state_fields_match_builtin() {
        let plain = SessionContext::new();
        let builtin = plain.state().udaf("approx_distinct").expect("built-in exists");

        for data_type in [
            arrow_schema::DataType::Int64,
            arrow_schema::DataType::Utf8,
            arrow_schema::DataType::Binary,
        ] {
            let input = Arc::new(arrow_schema::Field::new("x", data_type.clone(), true));
            let return_field = Arc::new(arrow_schema::Field::new(
                "out",
                arrow_schema::DataType::UInt64,
                true,
            ));
            let args = StateFieldsArgs {
                name: "dc",
                input_fields: std::slice::from_ref(&input),
                return_field: Arc::clone(&return_field),
                ordering_fields: &[],
                is_distinct: false,
            };
            let custom = SafeApproxDistinct::new().state_fields(args).expect("custom state fields");
            let args2 = StateFieldsArgs {
                name: "dc",
                input_fields: std::slice::from_ref(&input),
                return_field,
                ordering_fields: &[],
                is_distinct: false,
            };
            let expected = builtin.inner().state_fields(args2).expect("builtin state fields");
            assert_eq!(
                custom.iter().map(|f| (f.name().clone(), f.data_type().clone())).collect::<Vec<_>>(),
                expected.iter().map(|f| (f.name().clone(), f.data_type().clone())).collect::<Vec<_>>(),
                "state layout must match the built-in for input {data_type:?}"
            );
        }
    }
}
