//! Custom User-Defined Aggregate Functions (UDAFs) for DeQL.
//!
//! Provides `LAST(expr)` and `FIRST(expr)` aggregates for computing current
//! state and historical snapshots of aggregates in STATE AS queries.
//!
//! Ported from `deql-cli/deql-dereg/src/udaf.rs` (IMPL-02, task 3.2).

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::ArrayRef,
        datatypes::{DataType, Field, FieldRef},
    },
    common::ScalarValue,
    error::Result,
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
        function::{AccumulatorArgs, StateFieldsArgs},
    },
};

// ---------------------------------------------------------------------------
// LAST UDAF
// ---------------------------------------------------------------------------

/// UDAF implementation for LAST — returns the last non-null value in
/// accumulation order (event sequence order).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LastUdaf {
    signature: Signature,
}

impl LastUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for LastUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.return_field.data_type().clone();
        Ok(Box::new(LastAccumulator {
            value: ScalarValue::try_from(&data_type)?,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(
            args.return_field
                .as_ref()
                .clone()
                .with_name(format!("{}_last_value", args.name)),
        )])
    }
}

#[derive(Debug)]
struct LastAccumulator {
    value: ScalarValue,
}

impl Accumulator for LastAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.value = ScalarValue::try_from_array(arr, i)?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.value.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.value.size()
    }
}

// ---------------------------------------------------------------------------
// FIRST UDAF
// ---------------------------------------------------------------------------

/// UDAF implementation for FIRST — returns the first non-null value in
/// accumulation order (event sequence order).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FirstUdaf {
    signature: Signature,
}

impl FirstUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for FirstUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "first"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.return_field.data_type().clone();
        Ok(Box::new(FirstAccumulator {
            value: ScalarValue::try_from(&data_type)?,
            has_value: false,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(
                args.return_field
                    .as_ref()
                    .clone()
                    .with_name(format!("{}_first_value", args.name)),
            ),
            Arc::new(Field::new(
                format!("{}_first_has_value", args.name),
                DataType::Boolean,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct FirstAccumulator {
    value: ScalarValue,
    has_value: bool,
}

impl Accumulator for FirstAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.has_value {
            return Ok(());
        }
        let arr = &values[0];
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.value = ScalarValue::try_from_array(arr, i)?;
                self.has_value = true;
                return Ok(());
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.value.clone(),
            ScalarValue::Boolean(Some(self.has_value)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if self.has_value {
            return Ok(());
        }
        // states[0] = values, states[1] = has_value flags
        let values = &states[0];
        let flags = &states[1];
        for i in 0..values.len() {
            if !flags.is_null(i) {
                let flag = ScalarValue::try_from_array(flags, i)?;
                if flag == ScalarValue::Boolean(Some(true)) && !values.is_null(i) {
                    self.value = ScalarValue::try_from_array(values, i)?;
                    self.has_value = true;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.value.size()
    }
}

// ---------------------------------------------------------------------------
// Public constructors (IMPL-02)
// ---------------------------------------------------------------------------

/// Create the `LAST(expr)` aggregate UDF for registration with DataFusion.
pub fn create_last_udaf() -> AggregateUDF {
    AggregateUDF::from(LastUdaf::new())
}

/// Create the `FIRST(expr)` aggregate UDF for registration with DataFusion.
pub fn create_first_udaf() -> AggregateUDF {
    AggregateUDF::from(FirstUdaf::new())
}
