//! Versioned Protobuf codec for transportable physical-plan fragments.
//!
//! Storage providers are deliberately not serialized. Scan nodes carry a catalog table name and
//! an explicit schema; decoding resolves the provider in the worker's catalog and validates that
//! its projected fields still match the coordinator's contract.

use crate::catalog::{Catalog, projected_schema};
use crate::error::{Result, SparkXError};
use crate::execution::{OperatorId, PhysicalPlan};
use crate::expr::{AggregateFunction, Expr, Operator, ScalarValue};
use crate::logical::{JoinType, SortExpr};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

pub const PLAN_CODEC_VERSION: u32 = 1;
pub const MAX_PLAN_FRAGMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_PLAN_DEPTH: usize = 128;

pub struct PhysicalPlanCodec;

impl PhysicalPlanCodec {
    pub fn encode(plan: &PhysicalPlan) -> Result<Vec<u8>> {
        let envelope = WirePlanEnvelope {
            version: PLAN_CODEC_VERSION,
            root: Some(encode_plan(plan, 0)?),
        };
        let bytes = envelope.encode_to_vec();
        validate_fragment_size(bytes.len())?;
        Ok(bytes)
    }

    pub fn validate_fragment(bytes: &[u8]) -> Result<()> {
        let root = decode_envelope(bytes)?;
        validate_wire_plan(&root, 0)
    }

    pub fn decode(bytes: &[u8], catalog: &Catalog) -> Result<Arc<PhysicalPlan>> {
        let root = decode_envelope(bytes)?;
        decode_plan(root, catalog, 0)
    }
}

fn decode_envelope(bytes: &[u8]) -> Result<WirePlan> {
    validate_fragment_size(bytes.len())?;
    let envelope = WirePlanEnvelope::decode(bytes)
        .map_err(|error| codec_error(format!("cannot decode Protobuf envelope: {error}")))?;
    if envelope.version != PLAN_CODEC_VERSION {
        return Err(codec_error(format!(
            "unsupported plan codec version {}; expected {PLAN_CODEC_VERSION}",
            envelope.version
        )));
    }
    envelope
        .root
        .ok_or_else(|| codec_error("plan envelope has no root operator"))
}

fn validate_fragment_size(bytes: usize) -> Result<()> {
    if bytes == 0 {
        return Err(codec_error("plan fragment must not be empty"));
    }
    if bytes > MAX_PLAN_FRAGMENT_BYTES {
        return Err(codec_error(format!(
            "plan fragment is {bytes} bytes; maximum is {MAX_PLAN_FRAGMENT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn check_depth(depth: usize) -> Result<()> {
    if depth >= MAX_PLAN_DEPTH {
        return Err(codec_error(format!(
            "plan exceeds maximum nesting depth {MAX_PLAN_DEPTH}"
        )));
    }
    Ok(())
}

fn encode_plan(plan: &PhysicalPlan, depth: usize) -> Result<WirePlan> {
    check_depth(depth)?;
    use wire_plan::Kind;
    let kind = match plan {
        PhysicalPlan::Scan {
            id,
            table_name,
            projection,
            filters,
            schema,
            ..
        } => Kind::Scan(WireScan {
            id: *id,
            table_name: table_name.clone(),
            has_projection: projection.is_some(),
            projection: projection
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|index| checked_u32(*index, "scan projection index"))
                .collect::<Result<Vec<_>>>()?,
            filters: filters
                .iter()
                .map(encode_expr)
                .collect::<Result<Vec<_>>>()?,
            schema: Some(encode_schema(schema.as_ref())?),
        }),
        PhysicalPlan::Projection {
            id,
            input,
            exprs,
            schema,
        } => Kind::Projection(WireProjection {
            id: *id,
            input: Some(Box::new(encode_plan(input, depth + 1)?)),
            exprs: exprs.iter().map(encode_expr).collect::<Result<Vec<_>>>()?,
            schema: Some(encode_schema(schema.as_ref())?),
        }),
        PhysicalPlan::Filter {
            id,
            input,
            predicate,
            schema,
        } => Kind::Filter(WireFilter {
            id: *id,
            input: Some(Box::new(encode_plan(input, depth + 1)?)),
            predicate: Some(encode_expr(predicate)?),
            schema: Some(encode_schema(schema.as_ref())?),
        }),
        PhysicalPlan::HashAggregate {
            id,
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => Kind::HashAggregate(WireHashAggregate {
            id: *id,
            input: Some(Box::new(encode_plan(input, depth + 1)?)),
            group_exprs: group_exprs
                .iter()
                .map(encode_expr)
                .collect::<Result<Vec<_>>>()?,
            aggregate_exprs: aggregate_exprs
                .iter()
                .map(encode_expr)
                .collect::<Result<Vec<_>>>()?,
            schema: Some(encode_schema(schema.as_ref())?),
        }),
        PhysicalPlan::Sort {
            id,
            input,
            exprs,
            schema,
        } => Kind::Sort(WireSort {
            id: *id,
            input: Some(Box::new(encode_plan(input, depth + 1)?)),
            exprs: exprs
                .iter()
                .map(encode_sort_expr)
                .collect::<Result<Vec<_>>>()?,
            schema: Some(encode_schema(schema.as_ref())?),
        }),
        PhysicalPlan::TopK {
            id,
            input,
            exprs,
            limit,
            schema,
        } => Kind::TopK(WireTopK {
            id: *id,
            input: Some(Box::new(encode_plan(input, depth + 1)?)),
            exprs: exprs
                .iter()
                .map(encode_sort_expr)
                .collect::<Result<Vec<_>>>()?,
            limit: checked_u64(*limit, "Top-K limit")?,
            schema: Some(encode_schema(schema.as_ref())?),
        }),
        PhysicalPlan::Limit {
            id,
            input,
            limit,
            schema,
        } => Kind::Limit(WireLimit {
            id: *id,
            input: Some(Box::new(encode_plan(input, depth + 1)?)),
            limit: checked_u64(*limit, "limit")?,
            schema: Some(encode_schema(schema.as_ref())?),
        }),
        PhysicalPlan::HashJoin {
            id,
            left,
            right,
            join_type,
            left_on,
            right_on,
            schema,
        } => Kind::HashJoin(WireHashJoin {
            id: *id,
            left: Some(Box::new(encode_plan(left, depth + 1)?)),
            right: Some(Box::new(encode_plan(right, depth + 1)?)),
            join_type: encode_join_type(*join_type) as i32,
            left_on: left_on
                .iter()
                .map(encode_expr)
                .collect::<Result<Vec<_>>>()?,
            right_on: right_on
                .iter()
                .map(encode_expr)
                .collect::<Result<Vec<_>>>()?,
            schema: Some(encode_schema(schema.as_ref())?),
        }),
    };
    Ok(WirePlan { kind: Some(kind) })
}

fn decode_plan(wire: WirePlan, catalog: &Catalog, depth: usize) -> Result<Arc<PhysicalPlan>> {
    check_depth(depth)?;
    use wire_plan::Kind;
    let plan = match required(wire.kind, "plan operator")? {
        Kind::Scan(scan) => {
            if scan.table_name.trim().is_empty() {
                return Err(codec_error("scan table name must not be empty"));
            }
            let provider = catalog.table(&scan.table_name).map_err(|error| {
                codec_error(format!(
                    "worker cannot resolve scan table '{}': {error}",
                    scan.table_name
                ))
            })?;
            let projection = decode_projection(scan.has_projection, scan.projection)?;
            let schema = decode_required_schema(scan.schema, "scan")?;
            validate_scan_schema(
                &scan.table_name,
                &provider.schema(),
                projection.as_deref(),
                schema.as_ref(),
            )?;
            Arc::new(PhysicalPlan::Scan {
                id: scan.id,
                table_name: scan.table_name,
                provider,
                projection,
                filters: decode_exprs(scan.filters)?,
                schema,
            })
        }
        Kind::Projection(node) => {
            let input = decode_plan(
                required_box(node.input, "projection input")?,
                catalog,
                depth + 1,
            )?;
            let exprs = decode_exprs(node.exprs)?;
            let schema = decode_required_schema(node.schema, "projection")?;
            let expected = schema_for_exprs(&exprs, input.schema().as_ref())?;
            validate_schema("projection", schema.as_ref(), &expected)?;
            Arc::new(PhysicalPlan::Projection {
                id: node.id,
                input,
                exprs,
                schema,
            })
        }
        Kind::Filter(node) => {
            let input = decode_plan(
                required_box(node.input, "filter input")?,
                catalog,
                depth + 1,
            )?;
            let predicate = decode_expr(required(node.predicate, "filter predicate")?)?;
            if predicate.data_type(input.schema().as_ref())? != DataType::Boolean {
                return Err(codec_error("filter predicate must be Boolean"));
            }
            let schema = decode_required_schema(node.schema, "filter")?;
            validate_schema("filter", schema.as_ref(), input.schema().as_ref())?;
            Arc::new(PhysicalPlan::Filter {
                id: node.id,
                input,
                predicate,
                schema,
            })
        }
        Kind::HashAggregate(node) => {
            let input = decode_plan(
                required_box(node.input, "hash aggregate input")?,
                catalog,
                depth + 1,
            )?;
            let group_exprs = decode_exprs(node.group_exprs)?;
            let aggregate_exprs = decode_exprs(node.aggregate_exprs)?;
            let schema = decode_required_schema(node.schema, "hash aggregate")?;
            let expected = schema_for_exprs(
                &group_exprs
                    .iter()
                    .chain(&aggregate_exprs)
                    .cloned()
                    .collect::<Vec<_>>(),
                input.schema().as_ref(),
            )?;
            validate_schema("hash aggregate", schema.as_ref(), &expected)?;
            Arc::new(PhysicalPlan::HashAggregate {
                id: node.id,
                input,
                group_exprs,
                aggregate_exprs,
                schema,
            })
        }
        Kind::Sort(node) => {
            let input = decode_plan(required_box(node.input, "sort input")?, catalog, depth + 1)?;
            let exprs = decode_sort_exprs(node.exprs, input.schema().as_ref())?;
            let schema = decode_required_schema(node.schema, "sort")?;
            validate_schema("sort", schema.as_ref(), input.schema().as_ref())?;
            Arc::new(PhysicalPlan::Sort {
                id: node.id,
                input,
                exprs,
                schema,
            })
        }
        Kind::TopK(node) => {
            let input = decode_plan(required_box(node.input, "Top-K input")?, catalog, depth + 1)?;
            let exprs = decode_sort_exprs(node.exprs, input.schema().as_ref())?;
            let schema = decode_required_schema(node.schema, "Top-K")?;
            validate_schema("Top-K", schema.as_ref(), input.schema().as_ref())?;
            Arc::new(PhysicalPlan::TopK {
                id: node.id,
                input,
                exprs,
                limit: checked_usize(node.limit, "Top-K limit")?,
                schema,
            })
        }
        Kind::Limit(node) => {
            let input = decode_plan(required_box(node.input, "limit input")?, catalog, depth + 1)?;
            let schema = decode_required_schema(node.schema, "limit")?;
            validate_schema("limit", schema.as_ref(), input.schema().as_ref())?;
            Arc::new(PhysicalPlan::Limit {
                id: node.id,
                input,
                limit: checked_usize(node.limit, "limit")?,
                schema,
            })
        }
        Kind::HashJoin(node) => {
            let left = decode_plan(
                required_box(node.left, "join left input")?,
                catalog,
                depth + 1,
            )?;
            let right = decode_plan(
                required_box(node.right, "join right input")?,
                catalog,
                depth + 1,
            )?;
            let join_type = decode_join_type(node.join_type)?;
            let left_on = decode_exprs(node.left_on)?;
            let right_on = decode_exprs(node.right_on)?;
            validate_join_keys(
                &left_on,
                left.schema().as_ref(),
                &right_on,
                right.schema().as_ref(),
            )?;
            let schema = decode_required_schema(node.schema, "hash join")?;
            let expected = join_schema(left.schema().as_ref(), right.schema().as_ref(), join_type);
            validate_schema("hash join", schema.as_ref(), &expected)?;
            Arc::new(PhysicalPlan::HashJoin {
                id: node.id,
                left,
                right,
                join_type,
                left_on,
                right_on,
                schema,
            })
        }
    };
    Ok(plan)
}

fn validate_wire_plan(wire: &WirePlan, depth: usize) -> Result<()> {
    check_depth(depth)?;
    use wire_plan::Kind;
    match required_ref(wire.kind.as_ref(), "plan operator")? {
        Kind::Scan(node) => {
            if node.table_name.trim().is_empty() {
                return Err(codec_error("scan table name must not be empty"));
            }
            decode_projection(node.has_projection, node.projection.clone())?;
            decode_required_schema(node.schema.clone(), "scan")?;
            validate_wire_exprs(&node.filters)?;
        }
        Kind::Projection(node) => {
            validate_wire_plan(
                required_box_ref(node.input.as_deref(), "projection input")?,
                depth + 1,
            )?;
            decode_required_schema(node.schema.clone(), "projection")?;
            validate_wire_exprs(&node.exprs)?;
        }
        Kind::Filter(node) => {
            validate_wire_plan(
                required_box_ref(node.input.as_deref(), "filter input")?,
                depth + 1,
            )?;
            decode_required_schema(node.schema.clone(), "filter")?;
            decode_expr(required_ref(node.predicate.as_ref(), "filter predicate")?.clone())?;
        }
        Kind::HashAggregate(node) => {
            validate_wire_plan(
                required_box_ref(node.input.as_deref(), "hash aggregate input")?,
                depth + 1,
            )?;
            decode_required_schema(node.schema.clone(), "hash aggregate")?;
            validate_wire_exprs(&node.group_exprs)?;
            validate_wire_exprs(&node.aggregate_exprs)?;
        }
        Kind::Sort(node) => {
            validate_wire_plan(
                required_box_ref(node.input.as_deref(), "sort input")?,
                depth + 1,
            )?;
            decode_required_schema(node.schema.clone(), "sort")?;
            validate_wire_sort_exprs(&node.exprs)?;
        }
        Kind::TopK(node) => {
            validate_wire_plan(
                required_box_ref(node.input.as_deref(), "Top-K input")?,
                depth + 1,
            )?;
            decode_required_schema(node.schema.clone(), "Top-K")?;
            checked_usize(node.limit, "Top-K limit")?;
            validate_wire_sort_exprs(&node.exprs)?;
        }
        Kind::Limit(node) => {
            validate_wire_plan(
                required_box_ref(node.input.as_deref(), "limit input")?,
                depth + 1,
            )?;
            decode_required_schema(node.schema.clone(), "limit")?;
            checked_usize(node.limit, "limit")?;
        }
        Kind::HashJoin(node) => {
            validate_wire_plan(
                required_box_ref(node.left.as_deref(), "join left input")?,
                depth + 1,
            )?;
            validate_wire_plan(
                required_box_ref(node.right.as_deref(), "join right input")?,
                depth + 1,
            )?;
            decode_join_type(node.join_type)?;
            decode_required_schema(node.schema.clone(), "hash join")?;
            if node.left_on.len() != node.right_on.len() {
                return Err(codec_error(format!(
                    "hash join has {} left keys but {} right keys",
                    node.left_on.len(),
                    node.right_on.len()
                )));
            }
            validate_wire_exprs(&node.left_on)?;
            validate_wire_exprs(&node.right_on)?;
        }
    }
    Ok(())
}

fn encode_expr(expr: &Expr) -> Result<WireExpr> {
    use wire_expr::Kind;
    let kind = match expr {
        Expr::Column(name) => Kind::Column(name.clone()),
        Expr::Literal(value) => Kind::Literal(encode_scalar(value)),
        Expr::Binary { left, op, right } => Kind::Binary(WireBinaryExpr {
            left: Some(Box::new(encode_expr(left)?)),
            operator: encode_operator(*op) as i32,
            right: Some(Box::new(encode_expr(right)?)),
        }),
        Expr::IsNull { expr, negated } => Kind::IsNull(WireIsNullExpr {
            expr: Some(Box::new(encode_expr(expr)?)),
            negated: *negated,
        }),
        Expr::Cast { expr, data_type } => Kind::Cast(WireCastExpr {
            expr: Some(Box::new(encode_expr(expr)?)),
            data_type: encode_data_type(data_type)? as i32,
        }),
        Expr::Alias(expr, alias) => Kind::Alias(WireAliasExpr {
            expr: Some(Box::new(encode_expr(expr)?)),
            alias: alias.clone(),
        }),
        Expr::Aggregate {
            function,
            expr,
            distinct,
        } => Kind::Aggregate(WireAggregateExpr {
            function: encode_aggregate_function(*function) as i32,
            expr: Some(Box::new(encode_expr(expr)?)),
            distinct: *distinct,
        }),
        Expr::Wildcard => Kind::Wildcard(true),
    };
    Ok(WireExpr { kind: Some(kind) })
}

fn decode_expr(wire: WireExpr) -> Result<Expr> {
    use wire_expr::Kind;
    Ok(match required(wire.kind, "expression kind")? {
        Kind::Column(name) => {
            if name.trim().is_empty() {
                return Err(codec_error("column name must not be empty"));
            }
            Expr::Column(name)
        }
        Kind::Literal(value) => Expr::Literal(decode_scalar(value)?),
        Kind::Binary(node) => Expr::Binary {
            left: Box::new(decode_expr(required_box(
                node.left,
                "binary left operand",
            )?)?),
            op: decode_operator(node.operator)?,
            right: Box::new(decode_expr(required_box(
                node.right,
                "binary right operand",
            )?)?),
        },
        Kind::IsNull(node) => Expr::IsNull {
            expr: Box::new(decode_expr(required_box(node.expr, "IS NULL operand")?)?),
            negated: node.negated,
        },
        Kind::Cast(node) => Expr::Cast {
            expr: Box::new(decode_expr(required_box(node.expr, "cast operand")?)?),
            data_type: decode_data_type(node.data_type)?,
        },
        Kind::Alias(node) => {
            if node.alias.trim().is_empty() {
                return Err(codec_error("expression alias must not be empty"));
            }
            Expr::Alias(
                Box::new(decode_expr(required_box(node.expr, "aliased expression")?)?),
                node.alias,
            )
        }
        Kind::Aggregate(node) => Expr::Aggregate {
            function: decode_aggregate_function(node.function)?,
            expr: Box::new(decode_expr(required_box(node.expr, "aggregate argument")?)?),
            distinct: node.distinct,
        },
        Kind::Wildcard(true) => Expr::Wildcard,
        Kind::Wildcard(false) => return Err(codec_error("wildcard marker must be true")),
    })
}

fn encode_scalar(value: &ScalarValue) -> WireScalar {
    use wire_scalar::Value;
    let value = match value {
        ScalarValue::Null => Value::Null(true),
        ScalarValue::Boolean(value) => Value::Boolean(*value),
        ScalarValue::Int64(value) => Value::Int64(*value),
        ScalarValue::UInt64(value) => Value::UInt64(*value),
        ScalarValue::Float64(value) => Value::Float64(*value),
        ScalarValue::Utf8(value) => Value::Utf8(value.clone()),
    };
    WireScalar { value: Some(value) }
}

fn decode_scalar(wire: WireScalar) -> Result<ScalarValue> {
    use wire_scalar::Value;
    Ok(match required(wire.value, "scalar value")? {
        Value::Null(true) => ScalarValue::Null,
        Value::Null(false) => return Err(codec_error("null scalar marker must be true")),
        Value::Boolean(value) => ScalarValue::Boolean(value),
        Value::Int64(value) => ScalarValue::Int64(value),
        Value::UInt64(value) => ScalarValue::UInt64(value),
        Value::Float64(value) => ScalarValue::Float64(value),
        Value::Utf8(value) => ScalarValue::Utf8(value),
    })
}

fn encode_sort_expr(expr: &SortExpr) -> Result<WireSortExpr> {
    Ok(WireSortExpr {
        expr: Some(encode_expr(&expr.expr)?),
        ascending: expr.ascending,
        nulls_first: expr.nulls_first,
    })
}

fn decode_sort_exprs(exprs: Vec<WireSortExpr>, schema: &Schema) -> Result<Vec<SortExpr>> {
    let decoded = exprs
        .into_iter()
        .map(|sort| {
            let expr = decode_expr(required(sort.expr, "sort expression")?)?;
            expr.data_type(schema)?;
            Ok(SortExpr {
                expr,
                ascending: sort.ascending,
                nulls_first: sort.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if decoded.is_empty() {
        return Err(codec_error(
            "sort operator requires at least one expression",
        ));
    }
    Ok(decoded)
}

fn validate_wire_sort_exprs(exprs: &[WireSortExpr]) -> Result<()> {
    if exprs.is_empty() {
        return Err(codec_error(
            "sort operator requires at least one expression",
        ));
    }
    for sort in exprs {
        decode_expr(required_ref(sort.expr.as_ref(), "sort expression")?.clone())?;
    }
    Ok(())
}

fn decode_exprs(exprs: Vec<WireExpr>) -> Result<Vec<Expr>> {
    exprs.into_iter().map(decode_expr).collect()
}

fn validate_wire_exprs(exprs: &[WireExpr]) -> Result<()> {
    for expr in exprs {
        decode_expr(expr.clone())?;
    }
    Ok(())
}

fn encode_schema(schema: &Schema) -> Result<WireSchema> {
    Ok(WireSchema {
        fields: schema
            .fields()
            .iter()
            .map(|field| {
                Ok(WireField {
                    name: field.name().clone(),
                    data_type: encode_data_type(field.data_type())? as i32,
                    nullable: field.is_nullable(),
                    metadata: encode_metadata(field.metadata()),
                })
            })
            .collect::<Result<Vec<_>>>()?,
        metadata: encode_metadata(&schema.metadata),
    })
}

fn decode_required_schema(schema: Option<WireSchema>, operator: &str) -> Result<SchemaRef> {
    Ok(Arc::new(decode_schema(required(
        schema,
        &format!("{operator} schema"),
    )?)?))
}

fn decode_schema(schema: WireSchema) -> Result<Schema> {
    let fields = schema
        .fields
        .into_iter()
        .map(|field| {
            Ok(Field::new(
                field.name,
                decode_data_type(field.data_type)?,
                field.nullable,
            )
            .with_metadata(decode_metadata(field.metadata)?))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(
        fields,
        decode_metadata(schema.metadata)?,
    ))
}

fn encode_metadata(metadata: &HashMap<String, String>) -> Vec<WireKeyValue> {
    let mut entries = metadata
        .iter()
        .map(|(key, value)| WireKeyValue {
            key: key.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

fn decode_metadata(entries: Vec<WireKeyValue>) -> Result<HashMap<String, String>> {
    let mut metadata = HashMap::with_capacity(entries.len());
    for entry in entries {
        if metadata.insert(entry.key.clone(), entry.value).is_some() {
            return Err(codec_error(format!(
                "schema metadata key '{}' appears more than once",
                entry.key
            )));
        }
    }
    Ok(metadata)
}

fn encode_data_type(data_type: &DataType) -> Result<WireDataType> {
    Ok(match data_type {
        DataType::Null => WireDataType::Null,
        DataType::Boolean => WireDataType::Boolean,
        DataType::Int32 => WireDataType::Int32,
        DataType::Int64 => WireDataType::Int64,
        DataType::UInt32 => WireDataType::UInt32,
        DataType::UInt64 => WireDataType::UInt64,
        DataType::Float32 => WireDataType::Float32,
        DataType::Float64 => WireDataType::Float64,
        DataType::Utf8 => WireDataType::Utf8,
        other => {
            return Err(codec_error(format!(
                "Arrow data type {other} is not supported by plan codec version {PLAN_CODEC_VERSION}"
            )));
        }
    })
}

fn decode_data_type(value: i32) -> Result<DataType> {
    Ok(
        match WireDataType::try_from(value)
            .map_err(|_| codec_error(format!("unknown Arrow data-type code {value}")))?
        {
            WireDataType::Null => DataType::Null,
            WireDataType::Boolean => DataType::Boolean,
            WireDataType::Int32 => DataType::Int32,
            WireDataType::Int64 => DataType::Int64,
            WireDataType::UInt32 => DataType::UInt32,
            WireDataType::UInt64 => DataType::UInt64,
            WireDataType::Float32 => DataType::Float32,
            WireDataType::Float64 => DataType::Float64,
            WireDataType::Utf8 => DataType::Utf8,
        },
    )
}

fn validate_scan_schema(
    table_name: &str,
    provider_schema: &SchemaRef,
    projection: Option<&[usize]>,
    fragment_schema: &Schema,
) -> Result<()> {
    if let Some(indices) = projection {
        for index in indices {
            if *index >= provider_schema.fields().len() {
                return Err(codec_error(format!(
                    "scan projection index {index} is outside table '{table_name}' schema"
                )));
            }
        }
    }
    let actual = projected_schema(provider_schema, projection);
    if actual.fields().len() != fragment_schema.fields().len() {
        return Err(codec_error(format!(
            "scan schema for table '{table_name}' has {} fields but worker catalog provides {}",
            fragment_schema.fields().len(),
            actual.fields().len()
        )));
    }
    for (expected, actual) in fragment_schema.fields().iter().zip(actual.fields()) {
        if unqualified(expected.name()) != unqualified(actual.name())
            || expected.data_type() != actual.data_type()
            || expected.is_nullable() != actual.is_nullable()
            || expected.metadata() != actual.metadata()
        {
            return Err(codec_error(format!(
                "scan field '{}' does not match worker catalog field '{}' for table '{table_name}'",
                expected.name(),
                actual.name()
            )));
        }
    }
    Ok(())
}

fn validate_schema(operator: &str, actual: &Schema, expected: &Schema) -> Result<()> {
    if actual != expected {
        return Err(codec_error(format!(
            "{operator} output schema does not match its decoded expressions or inputs"
        )));
    }
    Ok(())
}

fn schema_for_exprs(exprs: &[Expr], input: &Schema) -> Result<Schema> {
    Ok(Schema::new(
        exprs
            .iter()
            .map(|expr| expr.field(input))
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn validate_join_keys(
    left: &[Expr],
    left_schema: &Schema,
    right: &[Expr],
    right_schema: &Schema,
) -> Result<()> {
    if left.is_empty() || left.len() != right.len() {
        return Err(codec_error(
            "hash join requires the same non-zero number of left and right keys",
        ));
    }
    for (left, right) in left.iter().zip(right) {
        let left_type = left.data_type(left_schema)?;
        let right_type = right.data_type(right_schema)?;
        if left_type != right_type {
            return Err(codec_error(format!(
                "hash join key types differ: {left_type} versus {right_type}"
            )));
        }
    }
    Ok(())
}

fn join_schema(left: &Schema, right: &Schema, join_type: JoinType) -> Schema {
    let mut fields = left
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for field in right.fields() {
        let mut output = field.as_ref().clone();
        if join_type == JoinType::Left {
            output = output.with_nullable(true);
        }
        if fields
            .iter()
            .any(|existing| existing.name() == output.name())
        {
            let output_name = format!("right.{}", output.name());
            output = output.with_name(output_name);
        }
        fields.push(output);
    }
    Schema::new(fields)
}

fn decode_projection(has_projection: bool, indices: Vec<u32>) -> Result<Option<Vec<usize>>> {
    if !has_projection {
        if !indices.is_empty() {
            return Err(codec_error(
                "scan projection contains indices while has_projection is false",
            ));
        }
        return Ok(None);
    }
    Ok(Some(
        indices
            .into_iter()
            .map(|index| {
                usize::try_from(index).map_err(|_| codec_error("projection index overflow"))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn checked_u32(value: usize, name: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| codec_error(format!("{name} {value} exceeds u32")))
}

fn checked_u64(value: usize, name: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| codec_error(format!("{name} {value} exceeds u64")))
}

fn checked_usize(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| codec_error(format!("{name} {value} exceeds usize")))
}

fn required<T>(value: Option<T>, name: &str) -> Result<T> {
    value.ok_or_else(|| codec_error(format!("{name} is missing")))
}

fn required_ref<'a, T>(value: Option<&'a T>, name: &str) -> Result<&'a T> {
    value.ok_or_else(|| codec_error(format!("{name} is missing")))
}

fn required_box<T>(value: Option<Box<T>>, name: &str) -> Result<T> {
    required(value, name).map(|value| *value)
}

fn required_box_ref<'a, T>(value: Option<&'a T>, name: &str) -> Result<&'a T> {
    required_ref(value, name)
}

fn unqualified(name: &str) -> &str {
    name.rsplit_once('.').map_or(name, |(_, name)| name)
}

fn codec_error(message: impl Into<String>) -> SparkXError {
    SparkXError::protocol(format!(
        "invalid physical plan fragment: {}",
        message.into()
    ))
}

fn encode_operator(operator: Operator) -> WireOperator {
    match operator {
        Operator::Eq => WireOperator::Eq,
        Operator::NotEq => WireOperator::NotEq,
        Operator::Lt => WireOperator::Lt,
        Operator::LtEq => WireOperator::LtEq,
        Operator::Gt => WireOperator::Gt,
        Operator::GtEq => WireOperator::GtEq,
        Operator::And => WireOperator::And,
        Operator::Or => WireOperator::Or,
        Operator::Add => WireOperator::Add,
        Operator::Subtract => WireOperator::Subtract,
        Operator::Multiply => WireOperator::Multiply,
        Operator::Divide => WireOperator::Divide,
    }
}

fn decode_operator(value: i32) -> Result<Operator> {
    Ok(
        match WireOperator::try_from(value)
            .map_err(|_| codec_error(format!("unknown expression operator code {value}")))?
        {
            WireOperator::Eq => Operator::Eq,
            WireOperator::NotEq => Operator::NotEq,
            WireOperator::Lt => Operator::Lt,
            WireOperator::LtEq => Operator::LtEq,
            WireOperator::Gt => Operator::Gt,
            WireOperator::GtEq => Operator::GtEq,
            WireOperator::And => Operator::And,
            WireOperator::Or => Operator::Or,
            WireOperator::Add => Operator::Add,
            WireOperator::Subtract => Operator::Subtract,
            WireOperator::Multiply => Operator::Multiply,
            WireOperator::Divide => Operator::Divide,
        },
    )
}

fn encode_aggregate_function(function: AggregateFunction) -> WireAggregateFunction {
    match function {
        AggregateFunction::Count => WireAggregateFunction::Count,
        AggregateFunction::Sum => WireAggregateFunction::Sum,
        AggregateFunction::Min => WireAggregateFunction::Min,
        AggregateFunction::Max => WireAggregateFunction::Max,
        AggregateFunction::Avg => WireAggregateFunction::Avg,
    }
}

fn decode_aggregate_function(value: i32) -> Result<AggregateFunction> {
    Ok(
        match WireAggregateFunction::try_from(value)
            .map_err(|_| codec_error(format!("unknown aggregate function code {value}")))?
        {
            WireAggregateFunction::Count => AggregateFunction::Count,
            WireAggregateFunction::Sum => AggregateFunction::Sum,
            WireAggregateFunction::Min => AggregateFunction::Min,
            WireAggregateFunction::Max => AggregateFunction::Max,
            WireAggregateFunction::Avg => AggregateFunction::Avg,
        },
    )
}

fn encode_join_type(join_type: JoinType) -> WireJoinType {
    match join_type {
        JoinType::Inner => WireJoinType::Inner,
        JoinType::Left => WireJoinType::Left,
    }
}

fn decode_join_type(value: i32) -> Result<JoinType> {
    Ok(
        match WireJoinType::try_from(value)
            .map_err(|_| codec_error(format!("unknown join type code {value}")))?
        {
            WireJoinType::Inner => JoinType::Inner,
            WireJoinType::Left => JoinType::Left,
        },
    )
}

#[derive(Clone, PartialEq, Message)]
struct WirePlanEnvelope {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(message, optional, tag = "2")]
    root: Option<WirePlan>,
}

#[derive(Clone, PartialEq, Message)]
struct WirePlan {
    #[prost(oneof = "wire_plan::Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    kind: Option<wire_plan::Kind>,
}

mod wire_plan {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Scan(super::WireScan),
        #[prost(message, tag = "2")]
        Projection(super::WireProjection),
        #[prost(message, tag = "3")]
        Filter(super::WireFilter),
        #[prost(message, tag = "4")]
        HashAggregate(super::WireHashAggregate),
        #[prost(message, tag = "5")]
        Sort(super::WireSort),
        #[prost(message, tag = "6")]
        TopK(super::WireTopK),
        #[prost(message, tag = "7")]
        Limit(super::WireLimit),
        #[prost(message, tag = "8")]
        HashJoin(super::WireHashJoin),
    }
}

#[derive(Clone, PartialEq, Message)]
struct WireScan {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(string, tag = "2")]
    table_name: String,
    #[prost(bool, tag = "3")]
    has_projection: bool,
    #[prost(uint32, repeated, tag = "4")]
    projection: Vec<u32>,
    #[prost(message, repeated, tag = "5")]
    filters: Vec<WireExpr>,
    #[prost(message, optional, tag = "6")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireProjection {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(message, optional, boxed, tag = "2")]
    input: Option<Box<WirePlan>>,
    #[prost(message, repeated, tag = "3")]
    exprs: Vec<WireExpr>,
    #[prost(message, optional, tag = "4")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireFilter {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(message, optional, boxed, tag = "2")]
    input: Option<Box<WirePlan>>,
    #[prost(message, optional, tag = "3")]
    predicate: Option<WireExpr>,
    #[prost(message, optional, tag = "4")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireHashAggregate {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(message, optional, boxed, tag = "2")]
    input: Option<Box<WirePlan>>,
    #[prost(message, repeated, tag = "3")]
    group_exprs: Vec<WireExpr>,
    #[prost(message, repeated, tag = "4")]
    aggregate_exprs: Vec<WireExpr>,
    #[prost(message, optional, tag = "5")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireSort {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(message, optional, boxed, tag = "2")]
    input: Option<Box<WirePlan>>,
    #[prost(message, repeated, tag = "3")]
    exprs: Vec<WireSortExpr>,
    #[prost(message, optional, tag = "4")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireTopK {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(message, optional, boxed, tag = "2")]
    input: Option<Box<WirePlan>>,
    #[prost(message, repeated, tag = "3")]
    exprs: Vec<WireSortExpr>,
    #[prost(uint64, tag = "4")]
    limit: u64,
    #[prost(message, optional, tag = "5")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireLimit {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(message, optional, boxed, tag = "2")]
    input: Option<Box<WirePlan>>,
    #[prost(uint64, tag = "3")]
    limit: u64,
    #[prost(message, optional, tag = "4")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireHashJoin {
    #[prost(uint32, tag = "1")]
    id: OperatorId,
    #[prost(message, optional, boxed, tag = "2")]
    left: Option<Box<WirePlan>>,
    #[prost(message, optional, boxed, tag = "3")]
    right: Option<Box<WirePlan>>,
    #[prost(enumeration = "WireJoinType", tag = "4")]
    join_type: i32,
    #[prost(message, repeated, tag = "5")]
    left_on: Vec<WireExpr>,
    #[prost(message, repeated, tag = "6")]
    right_on: Vec<WireExpr>,
    #[prost(message, optional, tag = "7")]
    schema: Option<WireSchema>,
}

#[derive(Clone, PartialEq, Message)]
struct WireExpr {
    #[prost(oneof = "wire_expr::Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    kind: Option<wire_expr::Kind>,
}

mod wire_expr {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Kind {
        #[prost(string, tag = "1")]
        Column(String),
        #[prost(message, tag = "2")]
        Literal(super::WireScalar),
        #[prost(message, tag = "3")]
        Binary(super::WireBinaryExpr),
        #[prost(message, tag = "4")]
        IsNull(super::WireIsNullExpr),
        #[prost(message, tag = "5")]
        Cast(super::WireCastExpr),
        #[prost(message, tag = "6")]
        Alias(super::WireAliasExpr),
        #[prost(message, tag = "7")]
        Aggregate(super::WireAggregateExpr),
        #[prost(bool, tag = "8")]
        Wildcard(bool),
    }
}

#[derive(Clone, PartialEq, Message)]
struct WireBinaryExpr {
    #[prost(message, optional, boxed, tag = "1")]
    left: Option<Box<WireExpr>>,
    #[prost(enumeration = "WireOperator", tag = "2")]
    operator: i32,
    #[prost(message, optional, boxed, tag = "3")]
    right: Option<Box<WireExpr>>,
}

#[derive(Clone, PartialEq, Message)]
struct WireIsNullExpr {
    #[prost(message, optional, boxed, tag = "1")]
    expr: Option<Box<WireExpr>>,
    #[prost(bool, tag = "2")]
    negated: bool,
}

#[derive(Clone, PartialEq, Message)]
struct WireCastExpr {
    #[prost(message, optional, boxed, tag = "1")]
    expr: Option<Box<WireExpr>>,
    #[prost(enumeration = "WireDataType", tag = "2")]
    data_type: i32,
}

#[derive(Clone, PartialEq, Message)]
struct WireAliasExpr {
    #[prost(message, optional, boxed, tag = "1")]
    expr: Option<Box<WireExpr>>,
    #[prost(string, tag = "2")]
    alias: String,
}

#[derive(Clone, PartialEq, Message)]
struct WireAggregateExpr {
    #[prost(enumeration = "WireAggregateFunction", tag = "1")]
    function: i32,
    #[prost(message, optional, boxed, tag = "2")]
    expr: Option<Box<WireExpr>>,
    #[prost(bool, tag = "3")]
    distinct: bool,
}

#[derive(Clone, PartialEq, Message)]
struct WireSortExpr {
    #[prost(message, optional, tag = "1")]
    expr: Option<WireExpr>,
    #[prost(bool, tag = "2")]
    ascending: bool,
    #[prost(bool, tag = "3")]
    nulls_first: bool,
}

#[derive(Clone, PartialEq, Message)]
struct WireScalar {
    #[prost(oneof = "wire_scalar::Value", tags = "1, 2, 3, 4, 5, 6")]
    value: Option<wire_scalar::Value>,
}

mod wire_scalar {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(bool, tag = "1")]
        Null(bool),
        #[prost(bool, tag = "2")]
        Boolean(bool),
        #[prost(int64, tag = "3")]
        Int64(i64),
        #[prost(uint64, tag = "4")]
        UInt64(u64),
        #[prost(double, tag = "5")]
        Float64(f64),
        #[prost(string, tag = "6")]
        Utf8(String),
    }
}

#[derive(Clone, PartialEq, Message)]
struct WireSchema {
    #[prost(message, repeated, tag = "1")]
    fields: Vec<WireField>,
    #[prost(message, repeated, tag = "2")]
    metadata: Vec<WireKeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct WireField {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(enumeration = "WireDataType", tag = "2")]
    data_type: i32,
    #[prost(bool, tag = "3")]
    nullable: bool,
    #[prost(message, repeated, tag = "4")]
    metadata: Vec<WireKeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct WireKeyValue {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum WireDataType {
    Null = 0,
    Boolean = 1,
    Int32 = 2,
    Int64 = 3,
    UInt32 = 4,
    UInt64 = 5,
    Float32 = 6,
    Float64 = 7,
    Utf8 = 8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum WireOperator {
    Eq = 0,
    NotEq = 1,
    Lt = 2,
    LtEq = 3,
    Gt = 4,
    GtEq = 5,
    And = 6,
    Or = 7,
    Add = 8,
    Subtract = 9,
    Multiply = 10,
    Divide = 11,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum WireAggregateFunction {
    Count = 0,
    Sum = 1,
    Min = 2,
    Max = 3,
    Avg = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum WireJoinType {
    Inner = 0,
    Left = 1,
}
