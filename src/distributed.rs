//! In-process distributed runner.
//!
//! It schedules partition tasks through the coordinator state machine, produces Arrow partial
//! aggregates, sends them through a query-scoped loopback Arrow Flight exchange, and merges them.
//! Workers consume serialized stage plans and report protocol task updates; moving execution
//! off-process still requires remote transport and task RPC handlers.

use crate::catalog::Catalog;
use crate::coordinator::{Coordinator, CoordinatorConfig};
use crate::error::{Result, SparkXError};
use crate::execution::{
    PhysicalPlan, TaskContext, collect_with_memory, execute, hash_aggregate_with_memory,
};
use crate::expr::{AggregateFunction, Expr, ScalarValue, scalars_to_array, value_at};
use crate::flight_exchange::{LoopbackFlightExchange, ShuffleExchange};
use crate::memory::QueryMemory;
use crate::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, QueryId, StageId, StagePlan, TaskState, WorkerId,
    WorkerMessage, WorkerRegistration,
};
use crate::row_key::{EncodedKey, RowKeyEncoder, encoded_key_memory_bytes};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct LocalCluster {
    workers: usize,
}

#[derive(Debug)]
pub struct ClusterResult {
    pub batches: Vec<RecordBatch>,
    pub distributed: bool,
    pub stages: usize,
}

impl LocalCluster {
    pub fn new(workers: usize) -> Self {
        Self {
            workers: workers.max(1),
        }
    }

    pub async fn execute(
        &self,
        plan: Arc<PhysicalPlan>,
        catalog: Arc<Catalog>,
        context: TaskContext,
    ) -> Result<ClusterResult> {
        context.cancellation.check()?;
        let PhysicalPlan::HashAggregate {
            id,
            input,
            group_exprs,
            aggregate_exprs,
            schema,
        } = plan.as_ref()
        else {
            return Ok(ClusterResult {
                batches: execute(plan, context).collect().await?,
                distributed: false,
                stages: 1,
            });
        };

        if contains_join(input) {
            return Ok(ClusterResult {
                batches: execute(plan, context).collect().await?,
                distributed: false,
                stages: 1,
            });
        }
        if aggregate_exprs.iter().any(is_distinct_aggregate) {
            return Ok(ClusterResult {
                batches: execute(plan, context).collect().await?,
                distributed: false,
                stages: 1,
            });
        }
        let Some(partitions) = scan_partitions(input) else {
            return Ok(ClusterResult {
                batches: execute(plan, context).collect().await?,
                distributed: false,
                stages: 1,
            });
        };
        if partitions <= 1 {
            return Ok(ClusterResult {
                batches: execute(plan, context).collect().await?,
                distributed: false,
                stages: 1,
            });
        }

        let started = Instant::now();
        let partial_exprs = partial_aggregate_exprs(aggregate_exprs)?;
        let partial_schema = aggregate_schema(input.schema(), group_exprs, &partial_exprs)?;
        let partition_count = u32::try_from(partitions).map_err(|_| {
            SparkXError::execution(format!(
                "local cluster partition count {partitions} exceeds the protocol limit"
            ))
        })?;
        let query_id = QueryId::new("local-cluster-query")?;
        let stage = StagePlan::from_physical_plan(
            query_id.clone(),
            StageId(0),
            Vec::new(),
            partition_count,
            input.as_ref(),
        )?;
        let mut coordinator = Coordinator::new(CoordinatorConfig {
            lease_duration_ms: u64::MAX / 2,
            heartbeat_timeout_ms: u64::MAX / 2,
            max_task_attempts: 1,
            max_stage_partitions: partition_count,
        })?;
        coordinator.submit_stage(stage)?;
        for worker_index in 0..self.workers.min(partitions) {
            coordinator.handle_worker_message(
                WorkerMessage::Register {
                    version: PROTOCOL_VERSION,
                    registration: WorkerRegistration {
                        worker_id: WorkerId::new(format!("local-worker-{worker_index:04}"))?,
                        slots: 1,
                        memory_bytes: context.memory.limit_bytes(),
                    },
                },
                0,
            )?;
        }

        let mut tasks = JoinSet::new();
        let mut exchange = LoopbackFlightExchange::start().await?;
        let mut partial_batches = Vec::with_capacity(partitions);
        let mut shuffle_reservation = context.memory.try_reserve(0)?;
        while partial_batches.len() < partitions {
            context.cancellation.check()?;
            while let Some(assignment) = coordinator.next_assignment(0)? {
                let CoordinatorMessage::AssignTask {
                    stage, task, lease, ..
                } = assignment
                else {
                    return Err(SparkXError::execution(
                        "local coordinator returned a cancellation while scheduling",
                    ));
                };
                let worker_id = lease.worker_id;
                let catalog = catalog.clone();
                let group_exprs = group_exprs.clone();
                let partial_exprs = partial_exprs.clone();
                let partial_schema = partial_schema.clone();
                let mut task_context = context.clone();
                task_context.partition = Some(task.partition_id.0 as usize);
                tasks.spawn(async move {
                    let result = async {
                        task_context.cancellation.check()?;
                        let input = stage.decode_physical_plan(catalog.as_ref())?;
                        let cancellation = task_context.cancellation.clone();
                        let stream = execute(input, task_context.clone());
                        let (batches, _input_reservation) =
                            collect_with_memory(stream, &task_context.memory).await?;
                        cancellation.check()?;
                        hash_aggregate_with_memory(
                            &batches,
                            &group_exprs,
                            &partial_exprs,
                            partial_schema,
                            &task_context.memory,
                        )
                    }
                    .await;
                    (worker_id, task, result)
                });
            }

            let (worker_id, task, result) = tasks
                .join_next()
                .await
                .ok_or_else(|| {
                    SparkXError::execution(
                        "local coordinator has unfinished partitions but no running tasks",
                    )
                })?
                .map_err(|error| SparkXError::execution(format!("worker task failed: {error}")))?;
            let batch = match result {
                Ok(batch) => {
                    coordinator.handle_worker_message(
                        WorkerMessage::TaskUpdate {
                            version: PROTOCOL_VERSION,
                            worker_id,
                            task,
                            state: TaskState::Succeeded {
                                finished_at_ms: 0,
                                output_blocks: Vec::new(),
                            },
                        },
                        0,
                    )?;
                    batch
                }
                Err(error) => {
                    let state = if matches!(&error, SparkXError::Cancelled) {
                        TaskState::Cancelled {
                            finished_at_ms: 0,
                            reason: "query cancellation reached local worker".to_owned(),
                        }
                    } else {
                        TaskState::Failed {
                            finished_at_ms: 0,
                            error: error.to_string(),
                            retryable: false,
                        }
                    };
                    coordinator.handle_worker_message(
                        WorkerMessage::TaskUpdate {
                            version: PROTOCOL_VERSION,
                            worker_id,
                            task,
                            state,
                        },
                        0,
                    )?;
                    return Err(error);
                }
            };
            let input_bytes = batch.get_array_memory_size() as u64;
            shuffle_reservation.try_grow(input_bytes)?;
            let transported = exchange.exchange(batch.schema(), vec![batch]).await?;
            context.cancellation.check()?;
            let output_bytes = transported
                .iter()
                .map(|batch| batch.get_array_memory_size() as u64)
                .sum::<u64>();
            if output_bytes > input_bytes {
                shuffle_reservation.try_grow(output_bytes - input_bytes)?;
            } else {
                shuffle_reservation.shrink(input_bytes - output_bytes);
            }
            context
                .metrics
                .add_shuffled_rows(transported.iter().map(RecordBatch::num_rows).sum());
            context.metrics.add_shuffled_bytes(output_bytes);
            partial_batches.extend(transported);
        }
        exchange.close().await?;
        context.cancellation.check()?;
        let final_batch = merge_partials(
            &partial_batches,
            group_exprs.len(),
            aggregate_exprs,
            schema.clone(),
            &context.memory,
        )?;
        context
            .metrics
            .record_operator_output(*id, "HashAggregate", final_batch.num_rows());
        context
            .metrics
            .add_operator_elapsed(*id, "HashAggregate", started.elapsed());
        Ok(ClusterResult {
            batches: vec![final_batch],
            distributed: true,
            stages: 2,
        })
    }
}

fn is_distinct_aggregate(expr: &Expr) -> bool {
    matches!(expr.unalias(), Expr::Aggregate { distinct: true, .. })
}

fn contains_join(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::HashJoin { .. } => true,
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::Limit { input, .. } => contains_join(input),
        PhysicalPlan::Scan { .. } => false,
    }
}

fn scan_partitions(plan: &PhysicalPlan) -> Option<usize> {
    match plan {
        PhysicalPlan::Scan { provider, .. } => Some(provider.partition_count()),
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::Limit { input, .. } => scan_partitions(input),
        PhysicalPlan::HashJoin { .. } => None,
    }
}

fn aggregate_schema(
    input_schema: SchemaRef,
    groups: &[Expr],
    aggregates: &[Expr],
) -> Result<SchemaRef> {
    let fields = groups
        .iter()
        .chain(aggregates)
        .map(|expr| expr.field(input_schema.as_ref()))
        .collect::<Result<Vec<Field>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn partial_aggregate_exprs(aggregates: &[Expr]) -> Result<Vec<Expr>> {
    let mut partials = Vec::new();
    for (index, aggregate) in aggregates.iter().enumerate() {
        let alias = aggregate.name();
        let Expr::Aggregate {
            function,
            expr,
            distinct,
        } = aggregate.unalias()
        else {
            return Err(SparkXError::planning(format!(
                "expected aggregate expression, got {aggregate}"
            )));
        };
        if *function == AggregateFunction::Avg {
            partials.push(
                Expr::Aggregate {
                    function: AggregateFunction::Sum,
                    expr: expr.clone(),
                    distinct: *distinct,
                }
                .alias(format!("__sparkx_avg_sum_{index}")),
            );
            partials.push(
                Expr::Aggregate {
                    function: AggregateFunction::Count,
                    expr: expr.clone(),
                    distinct: *distinct,
                }
                .alias(format!("__sparkx_avg_count_{index}")),
            );
        } else {
            partials.push(aggregate.unalias().clone().alias(alias));
        }
    }
    Ok(partials)
}

#[derive(Debug, Clone)]
enum MergeState {
    Count(u64),
    Sum { value: f64, seen: bool },
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    Avg { sum: f64, count: u64 },
}

fn merge_partials(
    batches: &[RecordBatch],
    group_count: usize,
    aggregates: &[Expr],
    schema: SchemaRef,
    memory: &QueryMemory,
) -> Result<RecordBatch> {
    let functions = aggregates
        .iter()
        .map(|expr| match expr.unalias() {
            Expr::Aggregate { function, .. } => Ok(*function),
            other => Err(SparkXError::planning(format!(
                "expected aggregate expression, got {other}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut state_reservation = memory.try_reserve(0)?;
    let key_encoder = RowKeyEncoder::new(
        schema
            .fields()
            .iter()
            .take(group_count)
            .map(|field| field.data_type().clone()),
    )?;
    let mut groups: HashMap<EncodedKey, Vec<MergeState>> = HashMap::new();
    if group_count == 0 {
        state_reservation.try_grow(merge_group_memory_bytes(&[], functions.len()))?;
        groups.insert(EncodedKey::default(), new_merge_states(&functions));
    }

    for batch in batches {
        let group_columns = batch.columns()[..group_count].to_vec();
        let encoded_keys = key_encoder.encode(&group_columns, batch.num_rows())?;
        let _encoded_reservation = memory.try_reserve(encoded_keys.memory_size())?;
        for row in 0..batch.num_rows() {
            let key = encoded_keys.key(row);
            if !groups.contains_key(key) {
                state_reservation.try_grow(merge_group_memory_bytes(key, functions.len()))?;
                groups.insert(EncodedKey::from(key), new_merge_states(&functions));
            }
            let states = groups.get_mut(key).expect("encoded merge key was inserted");
            let mut column = group_count;
            for (function, state) in functions.iter().zip(states.iter_mut()) {
                match (function, state) {
                    (AggregateFunction::Count, MergeState::Count(total)) => {
                        *total += as_u64(&value_at(batch.column(column).as_ref(), row)?)?;
                        column += 1;
                    }
                    (AggregateFunction::Sum, MergeState::Sum { value, seen }) => {
                        let partial = value_at(batch.column(column).as_ref(), row)?;
                        if partial != ScalarValue::Null {
                            *value += as_f64(&partial)?;
                            *seen = true;
                        }
                        column += 1;
                    }
                    (AggregateFunction::Min, MergeState::Min(current)) => {
                        merge_extreme(
                            current,
                            value_at(batch.column(column).as_ref(), row)?,
                            true,
                        )?;
                        column += 1;
                    }
                    (AggregateFunction::Max, MergeState::Max(current)) => {
                        merge_extreme(
                            current,
                            value_at(batch.column(column).as_ref(), row)?,
                            false,
                        )?;
                        column += 1;
                    }
                    (AggregateFunction::Avg, MergeState::Avg { sum, count }) => {
                        *sum += as_f64(&value_at(batch.column(column).as_ref(), row)?)?;
                        *count += as_u64(&value_at(batch.column(column + 1).as_ref(), row)?)?;
                        column += 2;
                    }
                    _ => return Err(SparkXError::execution("invalid partial aggregate state")),
                }
            }
        }
    }

    let mut entries = groups.into_iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    let (keys, state_rows): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
    let mut arrays = key_encoder.decode(&keys)?;
    let mut columns = vec![Vec::with_capacity(state_rows.len()); aggregates.len()];
    for states in state_rows {
        for (index, state) in states.into_iter().enumerate() {
            let value = match state {
                MergeState::Count(value) => ScalarValue::UInt64(value),
                MergeState::Sum { value, seen } => {
                    if seen {
                        ScalarValue::Float64(value)
                    } else {
                        ScalarValue::Null
                    }
                }
                MergeState::Min(value) | MergeState::Max(value) => {
                    value.unwrap_or(ScalarValue::Null)
                }
                MergeState::Avg { sum, count } => {
                    if count == 0 {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Float64(sum / count as f64)
                    }
                }
            };
            columns[index].push(value);
        }
    }
    arrays.extend(
        columns
            .iter()
            .zip(schema.fields().iter().skip(group_count))
            .map(|(values, field)| scalars_to_array(values, field.data_type()))
            .collect::<Result<Vec<_>>>()?,
    );
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn merge_group_memory_bytes(key: &[u8], state_count: usize) -> u64 {
    32_u64
        .saturating_add(encoded_key_memory_bytes(key))
        .saturating_add(size_of::<Vec<MergeState>>() as u64)
        .saturating_add((state_count as u64).saturating_mul(size_of::<MergeState>() as u64))
}

fn new_merge_states(functions: &[AggregateFunction]) -> Vec<MergeState> {
    functions
        .iter()
        .map(|function| match function {
            AggregateFunction::Count => MergeState::Count(0),
            AggregateFunction::Sum => MergeState::Sum {
                value: 0.0,
                seen: false,
            },
            AggregateFunction::Min => MergeState::Min(None),
            AggregateFunction::Max => MergeState::Max(None),
            AggregateFunction::Avg => MergeState::Avg { sum: 0.0, count: 0 },
        })
        .collect()
}

fn as_f64(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Int64(value) => Ok(*value as f64),
        ScalarValue::UInt64(value) => Ok(*value as f64),
        ScalarValue::Null => Ok(0.0),
        other => Err(SparkXError::execution(format!(
            "expected numeric partial, got {other:?}"
        ))),
    }
}

fn as_u64(value: &ScalarValue) -> Result<u64> {
    match value {
        ScalarValue::UInt64(value) => Ok(*value),
        ScalarValue::Int64(value) if *value >= 0 => Ok(*value as u64),
        ScalarValue::Null => Ok(0),
        other => Err(SparkXError::execution(format!(
            "expected unsigned partial, got {other:?}"
        ))),
    }
}

fn merge_extreme(
    current: &mut Option<ScalarValue>,
    candidate: ScalarValue,
    minimum: bool,
) -> Result<()> {
    if candidate == ScalarValue::Null {
        return Ok(());
    }
    let replace = match current {
        None => true,
        Some(value) => match candidate.partial_compare(value) {
            Some(Ordering::Less) => minimum,
            Some(Ordering::Greater) => !minimum,
            Some(Ordering::Equal) => false,
            None => {
                return Err(SparkXError::execution(
                    "partial aggregate types cannot be compared",
                ));
            }
        },
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}
