use crate::error::{Result, SparkXError};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::compute::kernels::{boolean, cmp, numeric};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::UInt64(a), Self::UInt64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a.to_bits() == b.to_bits(),
            (Self::Utf8(a), Self::Utf8(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(value) => value.hash(state),
            Self::Int64(value) => value.hash(state),
            Self::UInt64(value) => value.hash(state),
            Self::Float64(value) => value.to_bits().hash(state),
            Self::Utf8(value) => value.hash(state),
        }
    }
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Int64(_) => DataType::Int64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
        }
    }

    pub fn partial_compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Int64(a), Self::Int64(b)) => a.partial_cmp(b),
            (Self::UInt64(a), Self::UInt64(b)) => a.partial_cmp(b),
            (Self::Float64(a), Self::Float64(b)) => a.partial_cmp(b),
            (Self::Utf8(a), Self::Utf8(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(value) => write!(f, "{value}"),
            Self::Int64(value) => write!(f, "{value}"),
            Self::UInt64(value) => write!(f, "{value}"),
            Self::Float64(value) => write!(f, "{value}"),
            Self::Utf8(value) => write!(f, "'{value}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
        };
        write!(f, "{text}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl Display for AggregateFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self).map(|_| ())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(ScalarValue),
    Binary {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    Alias(Box<Expr>, String),
    Aggregate {
        function: AggregateFunction,
        expr: Box<Expr>,
        distinct: bool,
    },
    Wildcard,
}

impl Expr {
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    pub fn literal(value: ScalarValue) -> Self {
        Self::Literal(value)
    }

    pub fn binary(left: Expr, op: Operator, right: Expr) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn alias(self, alias: impl Into<String>) -> Self {
        Self::Alias(Box::new(self), alias.into())
    }

    pub fn name(&self) -> String {
        match self {
            Self::Column(name) => unqualified(name).to_owned(),
            Self::Literal(value) => value.to_string(),
            Self::Binary { .. } => self.to_string(),
            Self::Alias(_, alias) => alias.clone(),
            Self::Aggregate { function, expr, .. } => {
                format!("{}({})", function.to_string().to_uppercase(), expr)
            }
            Self::Wildcard => "*".to_owned(),
        }
    }

    pub fn unalias(&self) -> &Expr {
        match self {
            Self::Alias(expr, _) => expr,
            _ => self,
        }
    }

    pub fn columns(&self) -> BTreeSet<String> {
        let mut columns = BTreeSet::new();
        self.collect_columns(&mut columns);
        columns
    }

    fn collect_columns(&self, columns: &mut BTreeSet<String>) {
        match self {
            Self::Column(name) => {
                columns.insert(unqualified(name).to_owned());
            }
            Self::Binary { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            Self::Alias(expr, _) | Self::Aggregate { expr, .. } => expr.collect_columns(columns),
            Self::Literal(_) | Self::Wildcard => {}
        }
    }

    pub fn contains_aggregate(&self) -> bool {
        match self {
            Self::Aggregate { .. } => true,
            Self::Binary { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::Alias(expr, _) => expr.contains_aggregate(),
            _ => false,
        }
    }

    pub fn data_type(&self, schema: &Schema) -> Result<DataType> {
        match self.unalias() {
            Self::Column(name) => Ok(find_field(schema, name)?.data_type().clone()),
            Self::Literal(value) => Ok(value.data_type()),
            Self::Binary { left, op, right } => {
                let left_type = left.data_type(schema)?;
                let right_type = right.data_type(schema)?;
                match op {
                    Operator::And | Operator::Or
                        if left_type == DataType::Boolean && right_type == DataType::Boolean =>
                    {
                        Ok(DataType::Boolean)
                    }
                    Operator::And | Operator::Or => Err(SparkXError::planning(format!(
                        "{op} requires Boolean operands, got {left_type} and {right_type}"
                    ))),
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                        if left_type == right_type
                            || common_numeric_type(&left_type, &right_type).is_some() =>
                    {
                        Ok(DataType::Boolean)
                    }
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => Err(SparkXError::planning(format!(
                        "cannot compare {left_type} and {right_type}"
                    ))),
                    Operator::Add
                    | Operator::Subtract
                    | Operator::Multiply
                    | Operator::Divide => common_numeric_type(&left_type, &right_type).ok_or_else(
                        || {
                            SparkXError::planning(format!(
                                "{op} requires numeric operands, got {left_type} and {right_type}"
                            ))
                        },
                    ),
                }
            }
            Self::Aggregate { function, expr, .. } => match function {
                AggregateFunction::Count => Ok(DataType::UInt64),
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    let input_type = expr.data_type(schema)?;
                    if is_numeric(&input_type) {
                        Ok(DataType::Float64)
                    } else {
                        Err(SparkXError::planning(format!(
                            "{function} requires a numeric argument, got {input_type}"
                        )))
                    }
                }
                AggregateFunction::Min | AggregateFunction::Max => expr.data_type(schema),
            },
            Self::Wildcard => Err(SparkXError::planning(
                "wildcard must be expanded before type inference",
            )),
            Self::Alias(_, _) => unreachable!(),
        }
    }

    pub fn field(&self, schema: &Schema) -> Result<Field> {
        Ok(Field::new(self.name(), self.data_type(schema)?, true))
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(name) => write!(f, "#{name}"),
            Self::Literal(value) => write!(f, "{value}"),
            Self::Binary { left, op, right } => write!(f, "({left} {op} {right})"),
            Self::Alias(expr, alias) => write!(f, "{expr} AS {alias}"),
            Self::Aggregate {
                function,
                expr,
                distinct,
            } => write!(
                f,
                "{}({}{expr})",
                function.to_string().to_uppercase(),
                if *distinct { "DISTINCT " } else { "" }
            ),
            Self::Wildcard => write!(f, "*"),
        }
    }
}

pub fn unqualified(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

pub fn find_field<'a>(schema: &'a Schema, name: &str) -> Result<&'a Field> {
    let short = unqualified(name);
    schema
        .fields()
        .iter()
        .find(|field| field.name() == name || field.name() == short)
        .map(|field| field.as_ref())
        .ok_or_else(|| SparkXError::planning(format!("column '{name}' does not exist")))
}

pub fn find_column(schema: &Schema, name: &str) -> Result<usize> {
    let short = unqualified(name);
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name || field.name() == short)
        .ok_or_else(|| SparkXError::planning(format!("column '{name}' does not exist")))
}

pub fn evaluate(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expr.unalias() {
        Expr::Column(name) => Ok(batch
            .column(find_column(batch.schema().as_ref(), name)?)
            .clone()),
        Expr::Literal(value) => scalar_array(value, batch.num_rows()),
        Expr::Binary { left, op, right } => {
            let mut left = evaluate(left, batch)?;
            let mut right = evaluate(right, batch)?;
            if left.data_type() != right.data_type() {
                let target =
                    common_numeric_type(left.data_type(), right.data_type()).ok_or_else(|| {
                        SparkXError::execution(format!(
                            "cannot apply {op} to {} and {}",
                            left.data_type(),
                            right.data_type()
                        ))
                    })?;
                left = cast(&left, &target)?;
                right = cast(&right, &target)?;
            }
            let result: ArrayRef = match op {
                Operator::Eq => Arc::new(cmp::eq(left.as_ref(), right.as_ref())?),
                Operator::NotEq => Arc::new(cmp::neq(left.as_ref(), right.as_ref())?),
                Operator::Lt => Arc::new(cmp::lt(left.as_ref(), right.as_ref())?),
                Operator::LtEq => Arc::new(cmp::lt_eq(left.as_ref(), right.as_ref())?),
                Operator::Gt => Arc::new(cmp::gt(left.as_ref(), right.as_ref())?),
                Operator::GtEq => Arc::new(cmp::gt_eq(left.as_ref(), right.as_ref())?),
                Operator::And => Arc::new(boolean::and(as_boolean(&left)?, as_boolean(&right)?)?),
                Operator::Or => Arc::new(boolean::or(as_boolean(&left)?, as_boolean(&right)?)?),
                Operator::Add => numeric::add(left.as_ref(), right.as_ref())?,
                Operator::Subtract => numeric::sub(left.as_ref(), right.as_ref())?,
                Operator::Multiply => numeric::mul(left.as_ref(), right.as_ref())?,
                Operator::Divide => numeric::div(left.as_ref(), right.as_ref())?,
            };
            Ok(result)
        }
        Expr::Aggregate { .. } => Err(SparkXError::execution(
            "aggregate expression cannot be evaluated row-wise",
        )),
        Expr::Wildcard => Err(SparkXError::execution(
            "wildcard must be expanded before execution",
        )),
        Expr::Alias(_, _) => unreachable!(),
    }
}

fn common_numeric_type(left: &DataType, right: &DataType) -> Option<DataType> {
    use DataType::*;
    match (left, right) {
        (Float64, _) | (_, Float64) | (Float32, _) | (_, Float32) => Some(Float64),
        (Int64 | Int32, Int64 | Int32) => Some(Int64),
        (UInt64 | UInt32, UInt64 | UInt32) => Some(UInt64),
        _ => None,
    }
}

fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32
            | DataType::Int64
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

fn as_boolean(array: &ArrayRef) -> Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| SparkXError::execution("boolean expression expected"))
}

pub fn scalar_array(value: &ScalarValue, len: usize) -> Result<ArrayRef> {
    let array: ArrayRef = match value {
        ScalarValue::Null => Arc::new(arrow::array::NullArray::new(len)),
        ScalarValue::Boolean(value) => Arc::new(BooleanArray::from(vec![Some(*value); len])),
        ScalarValue::Int64(value) => Arc::new(Int64Array::from(vec![Some(*value); len])),
        ScalarValue::UInt64(value) => Arc::new(UInt64Array::from(vec![Some(*value); len])),
        ScalarValue::Float64(value) => Arc::new(Float64Array::from(vec![Some(*value); len])),
        ScalarValue::Utf8(value) => Arc::new(StringArray::from(vec![Some(value.as_str()); len])),
    };
    Ok(array)
}

pub fn value_at(array: &dyn Array, row: usize) -> Result<ScalarValue> {
    if array.is_null(row) {
        return Ok(ScalarValue::Null);
    }
    let value = match array.data_type() {
        DataType::Boolean => ScalarValue::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Int32 => ScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int64 => ScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt32 => ScalarValue::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row) as u64,
        ),
        DataType::UInt64 => ScalarValue::UInt64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Float32 => ScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        ),
        DataType::Float64 => ScalarValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Utf8 => ScalarValue::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_owned(),
        ),
        other => {
            return Err(SparkXError::unsupported(format!(
                "scalar extraction for {other}"
            )));
        }
    };
    Ok(value)
}

pub fn scalars_to_array(values: &[ScalarValue], data_type: &DataType) -> Result<ArrayRef> {
    let array: ArrayRef = match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Boolean(value) => Some(*value),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Int64(value) => Some(*value as i32),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Int64(value) => Some(*value),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::UInt32 => Arc::new(UInt32Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::UInt64(value) => Some(*value as u32),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::UInt64(value) => Some(*value),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Float32 => Arc::new(Float32Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Float64(value) => Some(*value as f32),
                    ScalarValue::Int64(value) => Some(*value as f32),
                    ScalarValue::UInt64(value) => Some(*value as f32),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Float64(value) => Some(*value),
                    ScalarValue::Int64(value) => Some(*value as f64),
                    ScalarValue::UInt64(value) => Some(*value as f64),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Utf8(value) => Some(value.as_str()),
                    ScalarValue::Null => None,
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        other => {
            return Err(SparkXError::unsupported(format!(
                "building output arrays for {other}"
            )));
        }
    };
    Ok(array)
}
