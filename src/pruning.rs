use crate::expr::{Expr, Operator, ScalarValue, unqualified};
use arrow::datatypes::{DataType, Schema};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use std::cmp::Ordering;

pub(crate) fn row_group_may_match(
    schema: &Schema,
    parquet_columns: &[Option<usize>],
    row_group: &RowGroupMetaData,
    filters: &[Expr],
) -> bool {
    filters
        .iter()
        .all(|filter| expression_may_match(filter, schema, parquet_columns, row_group))
}

fn expression_may_match(
    expression: &Expr,
    schema: &Schema,
    parquet_columns: &[Option<usize>],
    row_group: &RowGroupMetaData,
) -> bool {
    match expression.unalias() {
        Expr::Literal(ScalarValue::Boolean(value)) => *value,
        Expr::Literal(ScalarValue::Null) => false,
        Expr::Binary { left, op, right } => match op {
            Operator::And => {
                expression_may_match(left, schema, parquet_columns, row_group)
                    && expression_may_match(right, schema, parquet_columns, row_group)
            }
            Operator::Or => {
                expression_may_match(left, schema, parquet_columns, row_group)
                    || expression_may_match(right, schema, parquet_columns, row_group)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                comparison_may_match(left, *op, right, schema, parquet_columns, row_group)
            }
            _ => true,
        },
        Expr::IsNull { expr, negated } => {
            let Expr::Column(name) = expr.unalias() else {
                return true;
            };
            let Some((field_index, statistics)) =
                column_statistics(name, schema, parquet_columns, row_group)
            else {
                return true;
            };
            if !schema.field(field_index).is_nullable() {
                return *negated;
            }
            match statistics.null_count_opt() {
                Some(nulls) if *negated => nulls < row_group.num_rows().max(0) as u64,
                Some(nulls) => nulls > 0,
                None => true,
            }
        }
        _ => true,
    }
}

fn comparison_may_match(
    left: &Expr,
    operator: Operator,
    right: &Expr,
    schema: &Schema,
    parquet_columns: &[Option<usize>],
    row_group: &RowGroupMetaData,
) -> bool {
    match (left.unalias(), right.unalias()) {
        (Expr::Column(name), Expr::Literal(value)) => {
            column_literal_may_match(name, operator, value, schema, parquet_columns, row_group)
        }
        (Expr::Literal(value), Expr::Column(name)) => column_literal_may_match(
            name,
            reverse_comparison(operator),
            value,
            schema,
            parquet_columns,
            row_group,
        ),
        _ => true,
    }
}

fn column_literal_may_match(
    name: &str,
    operator: Operator,
    literal: &ScalarValue,
    schema: &Schema,
    parquet_columns: &[Option<usize>],
    row_group: &RowGroupMetaData,
) -> bool {
    if matches!(literal, ScalarValue::Null) {
        return false;
    }
    let Some((field_index, statistics)) =
        column_statistics(name, schema, parquet_columns, row_group)
    else {
        return true;
    };
    let data_type = schema.field(field_index).data_type();
    let ordered_statistics =
        !statistics.is_min_max_deprecated() || statistics.is_min_max_backwards_compatible();
    let min = ordered_statistics
        .then_some(statistics)
        .filter(|statistics| statistics.min_is_exact())
        .and_then(|statistics| statistic_min(statistics, data_type));
    let max = ordered_statistics
        .then_some(statistics)
        .filter(|statistics| statistics.max_is_exact())
        .and_then(|statistics| statistic_max(statistics, data_type));

    match operator {
        Operator::Eq => {
            min.as_ref()
                .is_none_or(|min| literal.partial_compare(min) != Some(Ordering::Less))
                && max
                    .as_ref()
                    .is_none_or(|max| literal.partial_compare(max) != Some(Ordering::Greater))
        }
        Operator::NotEq if !matches!(data_type, DataType::Float32 | DataType::Float64) => {
            !(min.as_ref().is_some_and(|min| min == literal)
                && max.as_ref().is_some_and(|max| max == literal))
        }
        Operator::NotEq => true,
        Operator::Lt => !min.as_ref().is_some_and(|min| {
            matches!(
                min.partial_compare(literal),
                Some(Ordering::Equal | Ordering::Greater)
            )
        }),
        Operator::LtEq => min
            .as_ref()
            .is_none_or(|min| min.partial_compare(literal) != Some(Ordering::Greater)),
        Operator::Gt => !max.as_ref().is_some_and(|max| {
            matches!(
                max.partial_compare(literal),
                Some(Ordering::Equal | Ordering::Less)
            )
        }),
        Operator::GtEq => max
            .as_ref()
            .is_none_or(|max| max.partial_compare(literal) != Some(Ordering::Less)),
        _ => true,
    }
}

fn column_statistics<'a>(
    name: &str,
    schema: &Schema,
    parquet_columns: &[Option<usize>],
    row_group: &'a RowGroupMetaData,
) -> Option<(usize, &'a Statistics)> {
    let name = unqualified(name);
    let field_index = schema
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(name))?;
    let parquet_index = parquet_columns.get(field_index).copied().flatten()?;
    let statistics = row_group.column(parquet_index).statistics()?;
    Some((field_index, statistics))
}

fn statistic_min(statistics: &Statistics, data_type: &DataType) -> Option<ScalarValue> {
    match (statistics, data_type) {
        (Statistics::Boolean(values), DataType::Boolean) => {
            values.min_opt().copied().map(ScalarValue::Boolean)
        }
        (Statistics::Int32(values), DataType::Int32) => values
            .min_opt()
            .copied()
            .map(i64::from)
            .map(ScalarValue::Int64),
        (Statistics::Int64(values), DataType::Int64) => {
            values.min_opt().copied().map(ScalarValue::Int64)
        }
        (Statistics::Float(values), DataType::Float32) => values
            .min_opt()
            .copied()
            .map(f64::from)
            .map(ScalarValue::Float64),
        (Statistics::Double(values), DataType::Float64) => {
            values.min_opt().copied().map(ScalarValue::Float64)
        }
        (Statistics::ByteArray(values), DataType::Utf8) => values
            .min_opt()
            .and_then(|value| value.as_utf8().ok())
            .map(str::to_owned)
            .map(ScalarValue::Utf8),
        _ => None,
    }
}

fn statistic_max(statistics: &Statistics, data_type: &DataType) -> Option<ScalarValue> {
    match (statistics, data_type) {
        (Statistics::Boolean(values), DataType::Boolean) => {
            values.max_opt().copied().map(ScalarValue::Boolean)
        }
        (Statistics::Int32(values), DataType::Int32) => values
            .max_opt()
            .copied()
            .map(i64::from)
            .map(ScalarValue::Int64),
        (Statistics::Int64(values), DataType::Int64) => {
            values.max_opt().copied().map(ScalarValue::Int64)
        }
        (Statistics::Float(values), DataType::Float32) => values
            .max_opt()
            .copied()
            .map(f64::from)
            .map(ScalarValue::Float64),
        (Statistics::Double(values), DataType::Float64) => {
            values.max_opt().copied().map(ScalarValue::Float64)
        }
        (Statistics::ByteArray(values), DataType::Utf8) => values
            .max_opt()
            .and_then(|value| value.as_utf8().ok())
            .map(str::to_owned)
            .map(ScalarValue::Utf8),
        _ => None,
    }
}

fn reverse_comparison(operator: Operator) -> Operator {
    match operator {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}
