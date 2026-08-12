use crate::catalog::TableRef;
use crate::error::{Result, SparkXError};
use crate::expr::{AggregateFunction, Expr, ScalarValue, evaluate, scalars_to_array, value_at};
use crate::logical::{JoinType, SortExpr};
use crate::metrics::MetricsRef;
use arrow::array::BooleanArray;
use arrow::compute::kernels::filter::filter_record_batch;
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    Scan {
        table_name: String,
        provider: TableRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        schema: SchemaRef,
    },
    Projection {
        input: Arc<PhysicalPlan>,
        exprs: Vec<Expr>,
        schema: SchemaRef,
    },
    Filter {
        input: Arc<PhysicalPlan>,
        predicate: Expr,
        schema: SchemaRef,
    },
    HashAggregate {
        input: Arc<PhysicalPlan>,
        group_exprs: Vec<Expr>,
        aggregate_exprs: Vec<Expr>,
        schema: SchemaRef,
    },
    Sort {
        input: Arc<PhysicalPlan>,
        exprs: Vec<SortExpr>,
        schema: SchemaRef,
    },
    Limit {
        input: Arc<PhysicalPlan>,
        limit: usize,
        schema: SchemaRef,
    },
    HashJoin {
        left: Arc<PhysicalPlan>,
        right: Arc<PhysicalPlan>,
        join_type: JoinType,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        schema: SchemaRef,
    },
}

impl PhysicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Scan { schema, .. }
            | Self::Projection { schema, .. }
            | Self::Filter { schema, .. }
            | Self::HashAggregate { schema, .. }
            | Self::Sort { schema, .. }
            | Self::Limit { schema, .. }
            | Self::HashJoin { schema, .. } => schema.clone(),
        }
    }

    pub fn explain(&self) -> String {
        let mut output = String::new();
        self.format_into(0, &mut output);
        output
    }

    fn format_into(&self, indent: usize, output: &mut String) {
        output.push_str(&"  ".repeat(indent));
        match self {
            Self::Scan {
                table_name,
                projection,
                filters,
                provider,
                ..
            } => {
                output.push_str(&format!(
                    "Parquet/Csv/MemoryScanExec: {table_name} partitions={} projection={projection:?}",
                    provider.partition_count()
                ));
                if !filters.is_empty() {
                    output.push_str(&format!(
                        " filters=[{}]",
                        filters
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                output.push('\n');
            }
            Self::Projection { input, exprs, .. } => {
                output.push_str(&format!("ProjectionExec: [{}]\n", display_exprs(exprs)));
                input.format_into(indent + 1, output);
            }
            Self::Filter {
                input, predicate, ..
            } => {
                output.push_str(&format!("FilterExec: {predicate}\n"));
                input.format_into(indent + 1, output);
            }
            Self::HashAggregate {
                input,
                group_exprs,
                aggregate_exprs,
                ..
            } => {
                output.push_str(&format!(
                    "HashAggregateExec: group=[{}] aggr=[{}]\n",
                    display_exprs(group_exprs),
                    display_exprs(aggregate_exprs)
                ));
                input.format_into(indent + 1, output);
            }
            Self::Sort { input, exprs, .. } => {
                output.push_str(&format!(
                    "SortExec: [{}]\n",
                    exprs
                        .iter()
                        .map(|sort| format!(
                            "{} {}",
                            sort.expr,
                            if sort.ascending { "ASC" } else { "DESC" }
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                input.format_into(indent + 1, output);
            }
            Self::Limit { input, limit, .. } => {
                output.push_str(&format!("LimitExec: {limit}\n"));
                input.format_into(indent + 1, output);
            }
            Self::HashJoin {
                left,
                right,
                join_type,
                left_on,
                right_on,
                ..
            } => {
                output.push_str(&format!(
                    "HashJoinExec: {join_type} [{} = {}]\n",
                    display_exprs(left_on),
                    display_exprs(right_on)
                ));
                left.format_into(indent + 1, output);
                right.format_into(indent + 1, output);
            }
        }
    }
}

fn display_exprs(exprs: &[Expr]) -> String {
    exprs
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Clone)]
pub struct TaskContext {
    pub batch_size: usize,
    pub channel_capacity: usize,
    pub partition: Option<usize>,
    pub metrics: MetricsRef,
}

pub struct BatchReceiver {
    receiver: mpsc::Receiver<Result<RecordBatch>>,
}

impl BatchReceiver {
    pub async fn recv(&mut self) -> Option<Result<RecordBatch>> {
        self.receiver.recv().await
    }

    pub async fn collect(mut self) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = self.recv().await {
            batches.push(batch?);
        }
        Ok(batches)
    }
}

pub fn execute(plan: Arc<PhysicalPlan>, context: TaskContext) -> BatchReceiver {
    match plan.as_ref() {
        PhysicalPlan::Scan {
            provider,
            projection,
            filters,
            ..
        } => execute_scan(
            provider.clone(),
            projection.clone(),
            filters.clone(),
            context,
        ),
        PhysicalPlan::Projection {
            input,
            exprs,
            schema,
        } => {
            let child = execute(input.clone(), context.clone());
            execute_projection(child, exprs.clone(), schema.clone(), context)
        }
        PhysicalPlan::Filter {
            input, predicate, ..
        } => {
            let child = execute(input.clone(), context.clone());
            execute_filter(child, predicate.clone(), context)
        }
        PhysicalPlan::HashAggregate {
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } => {
            let child = execute(input.clone(), context.clone());
            execute_aggregate(
                child,
                group_exprs.clone(),
                aggregate_exprs.clone(),
                schema.clone(),
                context,
            )
        }
        PhysicalPlan::Sort {
            input,
            exprs,
            schema,
        } => {
            let child = execute(input.clone(), context.clone());
            execute_sort(child, exprs.clone(), schema.clone(), context)
        }
        PhysicalPlan::Limit { input, limit, .. } => {
            let child = execute(input.clone(), context.clone());
            execute_limit(child, *limit, context)
        }
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_on,
            right_on,
            schema,
        } => {
            let left = execute(left.clone(), context.clone());
            let right = execute(right.clone(), context.clone());
            execute_hash_join(
                left,
                right,
                *join_type,
                left_on.clone(),
                right_on.clone(),
                schema.clone(),
                context,
            )
        }
    }
}

fn execute_scan(
    provider: TableRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    context: TaskContext,
) -> BatchReceiver {
    let (sender, receiver) = mpsc::channel(context.channel_capacity);
    let partitions = context
        .partition
        .map(|partition| vec![partition])
        .unwrap_or_else(|| (0..provider.partition_count()).collect());

    for partition in partitions {
        let provider = provider.clone();
        let projection = projection.clone();
        let filters = filters.clone();
        let sender = sender.clone();
        let context = context.clone();
        context.metrics.add_task();
        tokio::spawn(async move {
            let batch_size = context.batch_size;
            let scanned = tokio::task::spawn_blocking(move || {
                provider.scan_partition(partition, projection.as_deref(), batch_size)
            })
            .await;
            let batches = match scanned {
                Ok(Ok(batches)) => batches,
                Ok(Err(error)) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
                Err(error) => {
                    let _ = sender
                        .send(Err(SparkXError::execution(format!(
                            "scan task failed: {error}"
                        ))))
                        .await;
                    return;
                }
            };
            for mut batch in batches {
                context.metrics.record_input(batch.num_rows());
                context
                    .metrics
                    .add_scanned_bytes(batch.get_array_memory_size() as u64);
                for predicate in &filters {
                    match filter_batch(&batch, predicate) {
                        Ok(filtered) => batch = filtered,
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                    }
                }
                if sender.send(Ok(batch)).await.is_err() {
                    return;
                }
            }
        });
    }
    drop(sender);
    BatchReceiver { receiver }
}

fn execute_projection(
    mut input: BatchReceiver,
    exprs: Vec<Expr>,
    schema: SchemaRef,
    context: TaskContext,
) -> BatchReceiver {
    let (sender, receiver) = mpsc::channel(context.channel_capacity);
    tokio::spawn(async move {
        while let Some(batch) = input.recv().await {
            let result = batch.and_then(|batch| project_batch(&batch, &exprs, schema.clone()));
            if sender.send(result).await.is_err() {
                break;
            }
        }
    });
    BatchReceiver { receiver }
}

fn execute_filter(
    mut input: BatchReceiver,
    predicate: Expr,
    context: TaskContext,
) -> BatchReceiver {
    let (sender, receiver) = mpsc::channel(context.channel_capacity);
    tokio::spawn(async move {
        while let Some(batch) = input.recv().await {
            let result = batch.and_then(|batch| filter_batch(&batch, &predicate));
            if sender.send(result).await.is_err() {
                break;
            }
        }
    });
    BatchReceiver { receiver }
}

fn execute_limit(mut input: BatchReceiver, limit: usize, context: TaskContext) -> BatchReceiver {
    let (sender, receiver) = mpsc::channel(context.channel_capacity);
    tokio::spawn(async move {
        let mut remaining = limit;
        while remaining > 0 {
            let Some(batch) = input.recv().await else {
                break;
            };
            match batch {
                Ok(batch) => {
                    let take = remaining.min(batch.num_rows());
                    remaining -= take;
                    if take > 0 && sender.send(Ok(batch.slice(0, take))).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    break;
                }
            }
        }
    });
    BatchReceiver { receiver }
}

fn execute_aggregate(
    input: BatchReceiver,
    group_exprs: Vec<Expr>,
    aggregate_exprs: Vec<Expr>,
    schema: SchemaRef,
    context: TaskContext,
) -> BatchReceiver {
    let (sender, receiver) = mpsc::channel(context.channel_capacity);
    tokio::spawn(async move {
        let result = match input.collect().await {
            Ok(batches) => hash_aggregate(&batches, &group_exprs, &aggregate_exprs, schema),
            Err(error) => Err(error),
        };
        let _ = sender.send(result).await;
    });
    BatchReceiver { receiver }
}

fn execute_sort(
    input: BatchReceiver,
    exprs: Vec<SortExpr>,
    schema: SchemaRef,
    context: TaskContext,
) -> BatchReceiver {
    let (sender, receiver) = mpsc::channel(context.channel_capacity);
    tokio::spawn(async move {
        let result = async {
            let batches = input.collect().await?;
            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }
            let batch = concat_batches(&schema, &batches)?;
            let columns = exprs
                .iter()
                .map(|sort| {
                    Ok(SortColumn {
                        values: evaluate(&sort.expr, &batch)?,
                        options: Some(SortOptions {
                            descending: !sort.ascending,
                            nulls_first: sort.nulls_first,
                        }),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let indices = lexsort_to_indices(&columns, None)?;
            Ok(take_record_batch(&batch, &indices)?)
        }
        .await;
        let _ = sender.send(result).await;
    });
    BatchReceiver { receiver }
}

#[allow(clippy::too_many_arguments)]
fn execute_hash_join(
    left: BatchReceiver,
    right: BatchReceiver,
    join_type: JoinType,
    left_on: Vec<Expr>,
    right_on: Vec<Expr>,
    schema: SchemaRef,
    context: TaskContext,
) -> BatchReceiver {
    let (sender, receiver) = mpsc::channel(context.channel_capacity);
    tokio::spawn(async move {
        let result = async {
            let (left_batches, right_batches) = tokio::try_join!(left.collect(), right.collect())?;
            hash_join(
                &left_batches,
                &right_batches,
                join_type,
                &left_on,
                &right_on,
                schema,
            )
        }
        .await;
        let _ = sender.send(result).await;
    });
    BatchReceiver { receiver }
}

pub fn project_batch(
    batch: &RecordBatch,
    exprs: &[Expr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let arrays = exprs
        .iter()
        .map(|expr| evaluate(expr, batch))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

pub fn filter_batch(batch: &RecordBatch, predicate: &Expr) -> Result<RecordBatch> {
    let predicate = evaluate(predicate, batch)?;
    let predicate = predicate
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| SparkXError::execution("filter predicate did not produce Boolean values"))?;
    Ok(filter_record_batch(batch, predicate)?)
}

#[derive(Debug, Clone)]
enum AggregateState {
    Count {
        value: u64,
        distinct: Option<HashSet<ScalarValue>>,
    },
    Sum {
        int_sum: i128,
        float_sum: f64,
        seen: bool,
        distinct: Option<HashSet<ScalarValue>>,
    },
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    Avg {
        sum: f64,
        count: u64,
        distinct: Option<HashSet<ScalarValue>>,
    },
}

impl AggregateState {
    fn new(function: AggregateFunction, distinct: bool) -> Self {
        let set = distinct.then(HashSet::new);
        match function {
            AggregateFunction::Count => Self::Count {
                value: 0,
                distinct: set,
            },
            AggregateFunction::Sum => Self::Sum {
                int_sum: 0,
                float_sum: 0.0,
                seen: false,
                distinct: set,
            },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
            AggregateFunction::Avg => Self::Avg {
                sum: 0.0,
                count: 0,
                distinct: set,
            },
        }
    }

    fn update(&mut self, value: ScalarValue) -> Result<()> {
        match self {
            Self::Count {
                value: count,
                distinct,
            } => {
                if value == ScalarValue::Null {
                    return Ok(());
                }
                if let Some(values) = distinct {
                    if values.insert(value) {
                        *count += 1;
                    }
                } else {
                    *count += 1;
                }
            }
            Self::Sum {
                int_sum,
                float_sum,
                seen,
                distinct,
            } => {
                if value == ScalarValue::Null {
                    return Ok(());
                }
                if let Some(values) = distinct {
                    if !values.insert(value.clone()) {
                        return Ok(());
                    }
                }
                *seen = true;
                match value {
                    ScalarValue::Int64(value) => *int_sum += value as i128,
                    ScalarValue::UInt64(value) => *int_sum += value as i128,
                    ScalarValue::Float64(value) => *float_sum += value,
                    other => {
                        return Err(SparkXError::execution(format!(
                            "SUM does not support {other:?}"
                        )));
                    }
                }
            }
            Self::Min(current) => update_extreme(current, value, OrderingChoice::Min)?,
            Self::Max(current) => update_extreme(current, value, OrderingChoice::Max)?,
            Self::Avg {
                sum,
                count,
                distinct,
            } => {
                if value == ScalarValue::Null {
                    return Ok(());
                }
                if let Some(values) = distinct {
                    if !values.insert(value.clone()) {
                        return Ok(());
                    }
                }
                *sum += numeric_value(&value)?;
                *count += 1;
            }
        }
        Ok(())
    }

    fn finish(&self) -> ScalarValue {
        match self {
            Self::Count { value, .. } => ScalarValue::UInt64(*value),
            Self::Sum {
                int_sum,
                float_sum,
                seen,
                ..
            } => {
                if !seen {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(*float_sum + *int_sum as f64)
                }
            }
            Self::Min(value) | Self::Max(value) => value.clone().unwrap_or(ScalarValue::Null),
            Self::Avg { sum, count, .. } => {
                if *count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(*sum / *count as f64)
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum OrderingChoice {
    Min,
    Max,
}

fn update_extreme(
    current: &mut Option<ScalarValue>,
    candidate: ScalarValue,
    choice: OrderingChoice,
) -> Result<()> {
    if candidate == ScalarValue::Null {
        return Ok(());
    }
    let replace = match current {
        None => true,
        Some(value) => match candidate.partial_compare(value) {
            Some(std::cmp::Ordering::Less) => matches!(choice, OrderingChoice::Min),
            Some(std::cmp::Ordering::Greater) => matches!(choice, OrderingChoice::Max),
            Some(std::cmp::Ordering::Equal) => false,
            None => {
                return Err(SparkXError::execution(
                    "cannot compare values with different types",
                ));
            }
        },
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}

fn numeric_value(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Int64(value) => Ok(*value as f64),
        ScalarValue::UInt64(value) => Ok(*value as f64),
        ScalarValue::Float64(value) => Ok(*value),
        other => Err(SparkXError::execution(format!(
            "numeric aggregate does not support {other:?}"
        ))),
    }
}

pub fn hash_aggregate(
    batches: &[RecordBatch],
    group_exprs: &[Expr],
    aggregate_exprs: &[Expr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let specs = aggregate_exprs
        .iter()
        .map(aggregate_spec)
        .collect::<Result<Vec<_>>>()?;
    let mut groups: HashMap<Vec<ScalarValue>, Vec<AggregateState>> = HashMap::new();
    if group_exprs.is_empty() {
        groups.insert(
            Vec::new(),
            specs
                .iter()
                .map(|(function, _, distinct)| AggregateState::new(*function, *distinct))
                .collect(),
        );
    }

    for batch in batches {
        let group_arrays = group_exprs
            .iter()
            .map(|expr| evaluate(expr, batch))
            .collect::<Result<Vec<_>>>()?;
        let aggregate_arrays = specs
            .iter()
            .map(|(_, expr, _)| match expr {
                Expr::Wildcard => Ok(None),
                _ => evaluate(expr, batch).map(Some),
            })
            .collect::<Result<Vec<_>>>()?;

        for row in 0..batch.num_rows() {
            let key = group_arrays
                .iter()
                .map(|array| value_at(array.as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let states = groups.entry(key).or_insert_with(|| {
                specs
                    .iter()
                    .map(|(function, _, distinct)| AggregateState::new(*function, *distinct))
                    .collect()
            });
            for (index, state) in states.iter_mut().enumerate() {
                let value = match &aggregate_arrays[index] {
                    None => ScalarValue::Boolean(true),
                    Some(array) => value_at(array.as_ref(), row)?,
                };
                state.update(value)?;
            }
        }
    }

    let mut entries = groups.into_iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| compare_keys(left, right));
    let column_count = group_exprs.len() + aggregate_exprs.len();
    let mut columns = vec![Vec::with_capacity(entries.len()); column_count];
    for (key, states) in entries {
        for (index, value) in key.into_iter().enumerate() {
            columns[index].push(value);
        }
        for (index, state) in states.into_iter().enumerate() {
            columns[group_exprs.len() + index].push(state.finish());
        }
    }
    let arrays = columns
        .iter()
        .zip(schema.fields())
        .map(|(values, field)| scalars_to_array(values, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn aggregate_spec(expr: &Expr) -> Result<(AggregateFunction, Expr, bool)> {
    match expr.unalias() {
        Expr::Aggregate {
            function,
            expr,
            distinct,
        } => Ok((*function, expr.as_ref().clone(), *distinct)),
        other => Err(SparkXError::planning(format!(
            "expected aggregate expression, got {other}"
        ))),
    }
}

fn compare_keys(left: &[ScalarValue], right: &[ScalarValue]) -> std::cmp::Ordering {
    for (left, right) in left.iter().zip(right) {
        match left.partial_compare(right) {
            Some(std::cmp::Ordering::Equal) => continue,
            Some(ordering) => return ordering,
            None => return std::cmp::Ordering::Equal,
        }
    }
    left.len().cmp(&right.len())
}

fn hash_join(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    join_type: JoinType,
    left_on: &[Expr],
    right_on: &[Expr],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let right_width = right_batches
        .first()
        .map(RecordBatch::num_columns)
        .unwrap_or_else(|| {
            schema.fields().len().saturating_sub(
                left_batches
                    .first()
                    .map(RecordBatch::num_columns)
                    .unwrap_or(0),
            )
        });
    let mut build: HashMap<Vec<ScalarValue>, Vec<Vec<ScalarValue>>> = HashMap::new();
    for batch in right_batches {
        let keys = right_on
            .iter()
            .map(|expr| evaluate(expr, batch))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let key = keys
                .iter()
                .map(|array| value_at(array.as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            if key.iter().any(|value| value == &ScalarValue::Null) {
                continue;
            }
            let values = batch
                .columns()
                .iter()
                .map(|array| value_at(array.as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            build.entry(key).or_default().push(values);
        }
    }

    let mut output_rows = Vec::new();
    for batch in left_batches {
        let keys = left_on
            .iter()
            .map(|expr| evaluate(expr, batch))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let key = keys
                .iter()
                .map(|array| value_at(array.as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            let left_values = batch
                .columns()
                .iter()
                .map(|array| value_at(array.as_ref(), row))
                .collect::<Result<Vec<_>>>()?;
            if let Some(matches) = build.get(&key) {
                for right_values in matches {
                    let mut output = left_values.clone();
                    output.extend(right_values.clone());
                    output_rows.push(output);
                }
            } else if join_type == JoinType::Left {
                let mut output = left_values;
                output.extend(std::iter::repeat_n(ScalarValue::Null, right_width));
                output_rows.push(output);
            }
        }
    }

    let mut columns = vec![Vec::with_capacity(output_rows.len()); schema.fields().len()];
    for row in output_rows {
        for (index, value) in row.into_iter().enumerate() {
            columns[index].push(value);
        }
    }
    let arrays = columns
        .iter()
        .zip(schema.fields())
        .map(|(values, field)| scalars_to_array(values, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}
