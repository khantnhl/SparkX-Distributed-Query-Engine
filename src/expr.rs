use crate::error::{Result, SparkXError};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::compute::kernels::{boolean, cmp, numeric};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
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
        write!(f, "{self:?}")
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
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
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

    pub fn is_null(expr: Expr, negated: bool) -> Self {
        Self::IsNull {
            expr: Box::new(expr),
            negated,
        }
    }

    pub fn cast(expr: Expr, data_type: DataType) -> Self {
        Self::Cast {
            expr: Box::new(expr),
            data_type,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Column(name) => unqualified(name).to_owned(),
            Self::Literal(value) => value.to_string(),
            Self::Binary { .. } | Self::IsNull { .. } | Self::Cast { .. } => self.to_string(),
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
            Self::IsNull { expr, .. }
            | Self::Cast { expr, .. }
            | Self::Alias(expr, _)
            | Self::Aggregate { expr, .. } => expr.collect_columns(columns),
            Self::Literal(_) | Self::Wildcard => {}
        }
    }

    pub fn contains_aggregate(&self) -> bool {
        match self {
            Self::Aggregate { .. } => true,
            Self::Binary { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Self::IsNull { expr, .. } | Self::Cast { expr, .. } | Self::Alias(expr, _) => {
                expr.contains_aggregate()
            }
            _ => false,
        }
    }

    pub fn simplify(&self, schema: &Schema) -> Result<Expr> {
        let simplified = match self {
            Self::Binary { left, op, right } => {
                Self::binary(left.simplify(schema)?, *op, right.simplify(schema)?)
            }
            Self::IsNull { expr, negated } => Self::is_null(expr.simplify(schema)?, *negated),
            Self::Cast { expr, data_type } => Self::cast(expr.simplify(schema)?, data_type.clone()),
            Self::Alias(expr, alias) => {
                return Ok(expr.simplify(schema)?.alias(alias.clone()));
            }
            Self::Aggregate {
                function,
                expr,
                distinct,
            } => {
                return Ok(Self::Aggregate {
                    function: *function,
                    expr: Box::new(expr.simplify(schema)?),
                    distinct: *distinct,
                });
            }
            Self::Wildcard => return Ok(Self::Wildcard),
            Self::Column(_) | Self::Literal(_) => self.clone(),
        };
        simplify_node(simplified, schema)
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
                        if is_boolean_or_null(&left_type) && is_boolean_or_null(&right_type) =>
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
                            || left_type == DataType::Null
                            || right_type == DataType::Null
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
                    Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                        common_numeric_type(&left_type, &right_type).ok_or_else(|| {
                            SparkXError::planning(format!(
                                "{op} requires numeric operands, got {left_type} and {right_type}"
                            ))
                        })
                    }
                }
            }
            Self::IsNull { .. } => Ok(DataType::Boolean),
            Self::Cast { data_type, .. } => Ok(data_type.clone()),
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
            Self::IsNull { expr, negated } => {
                write!(f, "({expr} IS {}NULL)", if *negated { "NOT " } else { "" })
            }
            Self::Cast { expr, data_type } => write!(f, "CAST({expr} AS {data_type})"),
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
    Ok(schema.field(resolve_field_index(schema, name)?))
}

pub fn find_column(schema: &Schema, name: &str) -> Result<usize> {
    resolve_field_index(schema, name)
}

fn resolve_field_index(schema: &Schema, name: &str) -> Result<usize> {
    let exact = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (field.name() == name).then_some(index))
        .collect::<Vec<_>>();
    match exact.as_slice() {
        [index] => return Ok(*index),
        [_, _, ..] => return Err(ambiguous_column(name)),
        [] => {}
    }

    let qualified = name.contains('.');
    let schema_is_qualified = schema
        .fields()
        .iter()
        .any(|field| field.name().contains('.'));
    if qualified && schema_is_qualified {
        return Err(SparkXError::planning(format!(
            "column '{name}' does not exist"
        )));
    }

    let short = unqualified(name);
    let matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (unqualified(field.name()) == short).then_some(index))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(*index),
        [_, _, ..] => Err(ambiguous_column(name)),
        [] => Err(SparkXError::planning(format!(
            "column '{name}' does not exist"
        ))),
    }
}

fn ambiguous_column(name: &str) -> SparkXError {
    SparkXError::planning(format!(
        "column '{name}' is ambiguous; qualify it with a table name or alias"
    ))
}

fn simplify_node(expr: Expr, schema: &Schema) -> Result<Expr> {
    let output_type = expr.data_type(schema)?;
    if expr.columns().is_empty() && !expr.contains_aggregate() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        let value = evaluate(&expr, &batch)?;
        return Ok(typed_literal(
            value_at(value.as_ref(), 0)?,
            value.data_type(),
        ));
    }

    let Expr::Binary { left, op, right } = &expr else {
        return Ok(expr);
    };
    if matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Add
            | Operator::Subtract
            | Operator::Multiply
            | Operator::Divide
    ) && (is_null_expression(left) || is_null_expression(right))
    {
        return Ok(typed_literal(ScalarValue::Null, &output_type));
    }

    match (op, boolean_literal(left), boolean_literal(right)) {
        (Operator::And, Some(true), _) | (Operator::Or, Some(false), _) => {
            Ok(right.as_ref().clone())
        }
        (Operator::And, _, Some(true)) | (Operator::Or, _, Some(false)) => {
            Ok(left.as_ref().clone())
        }
        (Operator::And, Some(false), _)
        | (Operator::And, _, Some(false))
        | (Operator::Or, Some(true), _)
        | (Operator::Or, _, Some(true)) => Ok(Expr::literal(ScalarValue::Boolean(matches!(
            op,
            Operator::Or
        )))),
        _ => Ok(expr),
    }
}

fn typed_literal(value: ScalarValue, data_type: &DataType) -> Expr {
    let value_type = value.data_type();
    let literal = Expr::literal(value);
    if &value_type == data_type {
        literal
    } else {
        Expr::cast(literal, data_type.clone())
    }
}

fn is_null_expression(expr: &Expr) -> bool {
    match expr.unalias() {
        Expr::Literal(ScalarValue::Null) => true,
        Expr::Cast { expr, .. } => is_null_expression(expr),
        _ => false,
    }
}

fn boolean_literal(expr: &Expr) -> Option<bool> {
    match expr.unalias() {
        Expr::Literal(ScalarValue::Boolean(value)) => Some(*value),
        _ => None,
    }
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
            if left.data_type() == &DataType::Null && right.data_type() == &DataType::Null {
                return match op {
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or => {
                        Ok(Arc::new(BooleanArray::from(vec![None; batch.num_rows()])))
                    }
                    Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                        Err(SparkXError::execution(
                            "cannot infer the type of arithmetic on NULL values",
                        ))
                    }
                };
            }
            if left.data_type() != right.data_type() {
                let target = if matches!(
                    op,
                    Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide
                ) {
                    common_numeric_type(left.data_type(), right.data_type())
                } else {
                    match (left.data_type(), right.data_type()) {
                        (DataType::Null, right_type) => Some(right_type.clone()),
                        (left_type, DataType::Null) => Some(left_type.clone()),
                        (left_type, right_type) => common_numeric_type(left_type, right_type),
                    }
                }
                .ok_or_else(|| {
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
                Operator::Eq => Arc::new(cmp::eq(&left.as_ref(), &right.as_ref())?),
                Operator::NotEq => Arc::new(cmp::neq(&left.as_ref(), &right.as_ref())?),
                Operator::Lt => Arc::new(cmp::lt(&left.as_ref(), &right.as_ref())?),
                Operator::LtEq => Arc::new(cmp::lt_eq(&left.as_ref(), &right.as_ref())?),
                Operator::Gt => Arc::new(cmp::gt(&left.as_ref(), &right.as_ref())?),
                Operator::GtEq => Arc::new(cmp::gt_eq(&left.as_ref(), &right.as_ref())?),
                Operator::And => Arc::new(boolean::and_kleene(
                    as_boolean(&left)?,
                    as_boolean(&right)?,
                )?),
                Operator::Or => {
                    Arc::new(boolean::or_kleene(as_boolean(&left)?, as_boolean(&right)?)?)
                }
                Operator::Add => numeric::add(&left.as_ref(), &right.as_ref())?,
                Operator::Subtract => numeric::sub(&left.as_ref(), &right.as_ref())?,
                Operator::Multiply => numeric::mul(&left.as_ref(), &right.as_ref())?,
                Operator::Divide => numeric::div(&left.as_ref(), &right.as_ref())?,
            };
            Ok(result)
        }
        Expr::Cast { expr, data_type } => Ok(cast(&evaluate(expr, batch)?, data_type)?),
        Expr::IsNull { expr, negated } => {
            let value = evaluate(expr, batch)?;
            let result = if *negated {
                boolean::is_not_null(value.as_ref())?
            } else {
                boolean::is_null(value.as_ref())?
            };
            Ok(Arc::new(result))
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
    if left == &Null && is_numeric(right) {
        return normalized_numeric_type(right);
    }
    if right == &Null && is_numeric(left) {
        return normalized_numeric_type(left);
    }
    if !is_numeric(left) || !is_numeric(right) {
        return None;
    }
    match (left, right) {
        (Float64, _) | (_, Float64) | (Float32, _) | (_, Float32) => Some(Float64),
        (Int64 | Int32, Int64 | Int32) => Some(Int64),
        (UInt64 | UInt32, UInt64 | UInt32) => Some(UInt64),
        _ => None,
    }
}

fn normalized_numeric_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Int32 | DataType::Int64 => Some(DataType::Int64),
        DataType::UInt32 | DataType::UInt64 => Some(DataType::UInt64),
        DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
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

fn is_boolean_or_null(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Boolean | DataType::Null)
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
        DataType::Null => ScalarValue::Null,
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
