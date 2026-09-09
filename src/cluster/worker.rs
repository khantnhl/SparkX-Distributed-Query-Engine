//! Standalone worker runtime for executing leased physical-plan fragments.

use crate::cancellation::CancellationToken;
use crate::catalog::{Catalog, MemoryTable};
use crate::control_plane::ControlPlaneClient;
use crate::data_plane::{FlightDataPlaneClient, FlightDataPlaneServer};
use crate::execution::{TaskContext, collect_with_memory, execute};
use crate::memory::{MemoryReservation, QueryMemory};
use crate::metrics::{MetricsSnapshot, QueryMetrics};
use crate::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, QueryId, ShuffleBlock, ShuffleLocation, StageId,
    StagePlan, TaskAttemptId, TaskLease, TaskState, WorkerHeartbeat, WorkerId, WorkerMessage,
    WorkerRegistration, stage_input_table_name,
};
use crate::{Result, SparkXError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub coordinator_endpoint: String,
    pub worker_id: WorkerId,
    pub slots: u32,
    pub memory_bytes: u64,
    pub batch_size: usize,
    pub channel_capacity: usize,
    pub heartbeat_interval: Duration,
    pub poll_interval: Duration,
    pub data_bind_address: SocketAddr,
    pub data_advertised_host: Option<String>,
    pub data_storage_bytes: u64,
    pub data_directory: Option<std::path::PathBuf>,
    /// Development/test escape hatch. Production workers leave this as `None`.
    pub max_terminal_tasks: Option<u64>,
}

impl WorkerConfig {
    pub fn new(coordinator_endpoint: impl Into<String>, worker_id: WorkerId) -> Self {
        Self {
            coordinator_endpoint: coordinator_endpoint.into(),
            worker_id,
            slots: 1,
            memory_bytes: crate::DEFAULT_MEMORY_LIMIT_BYTES,
            batch_size: 8_192,
            channel_capacity: 2,
            heartbeat_interval: Duration::from_secs(5),
            poll_interval: Duration::from_millis(100),
            data_bind_address: "127.0.0.1:0".parse().expect("valid loopback address"),
            data_advertised_host: None,
            data_storage_bytes: crate::DEFAULT_MEMORY_LIMIT_BYTES,
            data_directory: None,
            max_terminal_tasks: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.coordinator_endpoint.trim().is_empty() {
            return Err(SparkXError::planning(
                "worker coordinator endpoint must not be empty",
            ));
        }
        if self.slots == 0 {
            return Err(SparkXError::planning(
                "worker must have at least one execution slot",
            ));
        }
        if self.memory_bytes == 0 {
            return Err(SparkXError::planning(
                "worker memory limit must be greater than zero",
            ));
        }
        if self.batch_size == 0 || self.channel_capacity == 0 {
            return Err(SparkXError::planning(
                "worker batch size and channel capacity must be greater than zero",
            ));
        }
        if self.heartbeat_interval.is_zero() || self.poll_interval.is_zero() {
            return Err(SparkXError::planning(
                "worker heartbeat and poll intervals must be greater than zero",
            ));
        }
        if self.data_storage_bytes == 0 {
            return Err(SparkXError::planning(
                "worker data-plane storage must be greater than zero",
            ));
        }
        if self.max_terminal_tasks == Some(0) {
            return Err(SparkXError::planning(
                "worker maximum terminal tasks must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRunSummary {
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub cancelled_tasks: u64,
    pub output_rows: u64,
    pub output_bytes: u64,
    pub metrics: MetricsSnapshot,
}

pub struct RemoteWorker {
    config: WorkerConfig,
    catalog: Arc<Catalog>,
}

impl RemoteWorker {
    pub fn new(config: WorkerConfig, catalog: Arc<Catalog>) -> Result<Self> {
        config.validate()?;
        Ok(Self { config, catalog })
    }

    pub async fn run_until(self, shutdown: CancellationToken) -> Result<WorkerRunSummary> {
        let data_plane = FlightDataPlaneServer::bind_with_storage(
            self.config.data_bind_address,
            self.config.data_advertised_host.as_deref(),
            self.config.data_storage_bytes,
            self.config.data_directory.as_deref(),
        )
        .await?;
        let data_endpoint = data_plane.endpoint();
        let mut client =
            ControlPlaneClient::connect(self.config.coordinator_endpoint.clone()).await?;
        client
            .register(WorkerRegistration {
                worker_id: self.config.worker_id.clone(),
                slots: self.config.slots,
                memory_bytes: self.config.memory_bytes,
            })
            .await?;

        let metrics = Arc::new(QueryMetrics::default());
        let memory = QueryMemory::new(self.config.memory_bytes);
        let mut active = BTreeMap::<TaskAttemptId, CancellationToken>::new();
        let mut tasks = JoinSet::<TaskCompletion>::new();
        let mut cancelled_queries = BTreeMap::<QueryId, String>::new();
        let mut summary = MutableWorkerSummary::default();
        let mut stopping = false;
        let mut heartbeat = interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut polling = interval(self.config.poll_interval);
        polling.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            if stopping && tasks.is_empty() {
                break;
            }
            tokio::select! {
                _ = shutdown.cancelled(), if !stopping => {
                    stopping = true;
                    cancel_all(&active);
                }
                _ = heartbeat.tick() => {
                    client.heartbeat(WorkerHeartbeat {
                        worker_id: self.config.worker_id.clone(),
                        observed_at_ms: current_time_ms(),
                        available_slots: self.config.slots.saturating_sub(active.len() as u32),
                        available_memory_bytes: memory.limit_bytes().saturating_sub(memory.reserved_bytes()),
                    }).await?;
                }
                _ = polling.tick(), if !stopping => {
                    if let Some(message) = client
                        .poll_assignment(self.config.worker_id.clone())
                        .await?
                    {
                        match message {
                            CoordinatorMessage::AssignTask {
                                stage,
                                task,
                                lease,
                                input_blocks,
                                ..
                            } => {
                                if lease.worker_id != self.config.worker_id {
                                    return Err(SparkXError::protocol(format!(
                                        "worker {} received a lease owned by {}",
                                        self.config.worker_id.as_str(),
                                        lease.worker_id.as_str()
                                    )));
                                }
                                if active.len() >= self.config.slots as usize {
                                    return Err(SparkXError::protocol(format!(
                                        "worker {} received more assignments than its {} slots",
                                        self.config.worker_id.as_str(),
                                        self.config.slots
                                    )));
                                }
                                client.send_worker_message(WorkerMessage::TaskUpdate {
                                    version: PROTOCOL_VERSION,
                                    worker_id: self.config.worker_id.clone(),
                                    task: task.clone(),
                                    state: TaskState::Running {
                                        started_at_ms: lease.issued_at_ms,
                                    },
                                }).await?;
                                let cancellation = CancellationToken::new();
                                active.insert(task.clone(), cancellation.clone());
                                tasks.spawn(execute_assignment(TaskAssignmentExecution {
                                    stage,
                                    task,
                                    lease,
                                    input_blocks,
                                    catalog: self.catalog.clone(),
                                    context: TaskContext {
                                        batch_size: self.config.batch_size,
                                        channel_capacity: self.config.channel_capacity,
                                        partition: None,
                                        metrics: metrics.clone(),
                                        memory: memory.clone(),
                                        cancellation,
                                    },
                                    worker_id: self.config.worker_id.clone(),
                                    data_endpoint: data_endpoint.clone(),
                                }));
                            }
                            CoordinatorMessage::CancelQuery { query_id, reason, .. } => {
                                cancelled_queries.insert(query_id.clone(), reason);
                                for (task, cancellation) in &active {
                                    if task.query_id == query_id {
                                        cancellation.cancel();
                                    }
                                }
                            }
                        }
                    }
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    let completion = joined
                        .expect("non-empty worker task set must yield a completion")
                        .map_err(|error| SparkXError::execution(format!(
                            "worker task join failed: {error}"
                        )))?;
                    active.remove(&completion.task);
                    let query_cancellation = cancelled_queries.get(&completion.task.query_id);
                    let state = if let Some(reason) = query_cancellation {
                        if let Ok(output) = &completion.result {
                            discard_output_blocks(&output.blocks).await;
                        }
                        summary.cancelled_tasks += 1;
                        TaskState::Cancelled {
                            finished_at_ms: completion.lease.issued_at_ms,
                            reason: reason.clone(),
                        }
                    } else {
                        match completion.result {
                            Ok(output) => {
                                summary.completed_tasks += 1;
                                summary.output_rows = summary.output_rows.saturating_add(output.rows);
                                summary.output_bytes = summary.output_bytes.saturating_add(output.bytes);
                                TaskState::Succeeded {
                                    finished_at_ms: completion.lease.issued_at_ms,
                                    output_blocks: output.blocks,
                                }
                            }
                            Err(SparkXError::Cancelled) if stopping => {
                                summary.failed_tasks += 1;
                                TaskState::Failed {
                                    finished_at_ms: completion.lease.issued_at_ms,
                                    error: "worker shut down before the task completed".to_owned(),
                                    retryable: true,
                                }
                            }
                            Err(SparkXError::Cancelled) => {
                                summary.cancelled_tasks += 1;
                                TaskState::Cancelled {
                                    finished_at_ms: completion.lease.issued_at_ms,
                                    reason: "task execution was cancelled".to_owned(),
                                }
                            }
                            Err(error) => {
                                summary.failed_tasks += 1;
                                let retryable = is_retryable(&error);
                                TaskState::Failed {
                                    finished_at_ms: completion.lease.issued_at_ms,
                                    error: error.to_string(),
                                    retryable,
                                }
                            }
                        }
                    };
                    let uncommitted_blocks = match &state {
                        TaskState::Succeeded { output_blocks, .. } => output_blocks.clone(),
                        _ => Vec::new(),
                    };
                    let update = client.send_worker_message(WorkerMessage::TaskUpdate {
                        version: PROTOCOL_VERSION,
                        worker_id: self.config.worker_id.clone(),
                        task: completion.task,
                        state,
                    }).await;
                    if let Err(error) = update {
                        discard_output_blocks(&uncommitted_blocks).await;
                        return Err(error);
                    }

                    if self
                        .config
                        .max_terminal_tasks
                        .is_some_and(|maximum| summary.terminal_tasks() >= maximum)
                    {
                        stopping = true;
                        cancel_all(&active);
                    }
                }
            }
        }

        metrics.set_memory_usage(memory.reserved_bytes(), memory.peak_bytes());
        let result = WorkerRunSummary {
            completed_tasks: summary.completed_tasks,
            failed_tasks: summary.failed_tasks,
            cancelled_tasks: summary.cancelled_tasks,
            output_rows: summary.output_rows,
            output_bytes: summary.output_bytes,
            metrics: metrics.snapshot(),
        };
        data_plane.close().await?;
        Ok(result)
    }
}

#[derive(Debug)]
struct TaskCompletion {
    task: TaskAttemptId,
    lease: TaskLease,
    result: Result<TaskOutput>,
}

#[derive(Debug)]
struct TaskOutput {
    rows: u64,
    bytes: u64,
    blocks: Vec<ShuffleBlock>,
}

struct TaskAssignmentExecution {
    stage: StagePlan,
    task: TaskAttemptId,
    lease: TaskLease,
    input_blocks: Vec<ShuffleBlock>,
    catalog: Arc<Catalog>,
    context: TaskContext,
    worker_id: WorkerId,
    data_endpoint: String,
}

async fn execute_assignment(assignment: TaskAssignmentExecution) -> TaskCompletion {
    let TaskAssignmentExecution {
        stage,
        task,
        lease,
        input_blocks,
        catalog,
        mut context,
        worker_id,
        data_endpoint,
    } = assignment;
    context.partition = Some(task.partition_id.0 as usize);
    let cancellation = context.cancellation.clone();
    let mut blocks = Vec::new();
    let deadline = Duration::from_millis(lease.expires_at_ms.saturating_sub(current_time_ms()));
    let work = async {
        context.cancellation.check()?;
        let (task_catalog, _input_reservation) = materialize_stage_inputs(
            catalog.as_ref(),
            &stage.input_stages,
            &input_blocks,
            &context.memory,
        )
        .await?;
        if !stage.input_stages.is_empty() {
            // Each task's dependency catalog contains exactly its own input as partition zero.
            context.partition = Some(0);
        }
        let plan = stage.decode_physical_plan(&task_catalog)?;
        let schema = plan.schema();
        let (batches, _output_reservation) =
            collect_with_memory(execute(plan, context.clone()), &context.memory).await?;
        context.cancellation.check()?;
        let mut data_client = FlightDataPlaneClient::connect(data_endpoint).await?;
        let outputs = match &stage.output_exchange {
            Some(exchange) => crate::hash_exchange::repartition(
                &schema,
                &batches,
                exchange,
                &context.memory,
                &context.cancellation,
            )?,
            None => vec![(task.partition_id, batches, context.memory.try_reserve(0)?)],
        };
        for (partition, batches, _reservation) in outputs {
            let upload = async {
                context.cancellation.check()?;
                data_client
                    .upload(
                        worker_id.clone(),
                        task.clone(),
                        partition,
                        schema.clone(),
                        batches,
                    )
                    .await
            }
            .await;
            match upload {
                Ok(block) => blocks.push(block),
                Err(error) => {
                    return Err(error);
                }
            }
        }
        Ok(TaskOutput {
            rows: blocks.iter().map(|block| block.rows).sum(),
            bytes: blocks.iter().map(|block| block.bytes).sum(),
            blocks: std::mem::take(&mut blocks),
        })
    };
    let result = tokio::select! {
        result = work => result,
        _ = cancellation.cancelled() => Err(SparkXError::Cancelled),
        _ = tokio::time::sleep(deadline) => Err(SparkXError::transport("task lease expired during execution")),
    };
    if result.is_err() {
        cancellation.cancel();
        discard_output_blocks(&blocks).await;
    }
    TaskCompletion {
        task,
        lease,
        result,
    }
}

#[derive(Debug, Default)]
struct MaterializedStageInput {
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
}

async fn materialize_stage_inputs(
    base_catalog: &Catalog,
    input_stages: &[StageId],
    input_blocks: &[ShuffleBlock],
    memory: &QueryMemory,
) -> Result<(Catalog, MemoryReservation)> {
    let task_catalog = base_catalog.snapshot();
    let mut reservation = memory.try_reserve(0)?;
    if input_stages.is_empty() {
        if !input_blocks.is_empty() {
            return Err(SparkXError::protocol(
                "task without stage dependencies received input blocks",
            ));
        }
        return Ok((task_catalog, reservation));
    }

    let mut clients = BTreeMap::<String, FlightDataPlaneClient>::new();
    let mut materialized = input_stages
        .iter()
        .copied()
        .map(|stage_id| (stage_id, MaterializedStageInput::default()))
        .collect::<BTreeMap<_, _>>();
    for block in input_blocks {
        let input = materialized
            .get_mut(&block.producer.stage_id)
            .ok_or_else(|| {
                SparkXError::protocol(format!(
                    "worker received a block from undeclared stage {}",
                    block.producer.stage_id.0
                ))
            })?;
        let endpoint = match &block.location {
            ShuffleLocation::Flight { endpoint, .. } => endpoint.clone(),
            ShuffleLocation::Worker { .. } => {
                return Err(SparkXError::unsupported(
                    "remote workers cannot fetch worker-local input blocks",
                ));
            }
            ShuffleLocation::ObjectStore { .. } => {
                return Err(SparkXError::unsupported(
                    "remote workers cannot fetch object-store input blocks yet",
                ));
            }
        };
        if !clients.contains_key(&endpoint) {
            clients.insert(
                endpoint.clone(),
                FlightDataPlaneClient::connect(endpoint.clone()).await?,
            );
        }
        let downloaded = clients
            .get_mut(&endpoint)
            .expect("data-plane client was just inserted")
            .download_reserved(block, &mut reservation)
            .await
            .map_err(|error| match error {
                SparkXError::NotFound(message) => {
                    SparkXError::transport(format!("upstream block unavailable: {message}"))
                }
                other => other,
            })?;
        if input
            .schema
            .as_ref()
            .is_some_and(|schema| schema.as_ref() != downloaded.schema.as_ref())
        {
            return Err(SparkXError::protocol(format!(
                "stage {} input blocks contain different Arrow schemas",
                block.producer.stage_id.0
            )));
        }
        input.schema.get_or_insert(downloaded.schema);
        for batch in downloaded.batches {
            input.batches.push(batch);
        }
    }

    for stage_id in input_stages {
        let input = materialized
            .remove(stage_id)
            .expect("declared input stage was initialized");
        let schema = input.schema.ok_or_else(|| {
            SparkXError::protocol(format!(
                "stage {} dependency produced no input block manifest",
                stage_id.0
            ))
        })?;
        let table = MemoryTable::new(schema, vec![input.batches])?;
        task_catalog.register(stage_input_table_name(*stage_id), Arc::new(table));
    }
    Ok((task_catalog, reservation))
}

#[derive(Debug, Default)]
struct MutableWorkerSummary {
    completed_tasks: u64,
    failed_tasks: u64,
    cancelled_tasks: u64,
    output_rows: u64,
    output_bytes: u64,
}

impl MutableWorkerSummary {
    fn terminal_tasks(&self) -> u64 {
        self.completed_tasks
            .saturating_add(self.failed_tasks)
            .saturating_add(self.cancelled_tasks)
    }
}

fn cancel_all(active: &BTreeMap<TaskAttemptId, CancellationToken>) {
    for cancellation in active.values() {
        cancellation.cancel();
    }
}

async fn discard_output_blocks(blocks: &[ShuffleBlock]) {
    for block in blocks {
        let ShuffleLocation::Flight { endpoint, .. } = &block.location else {
            continue;
        };
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            if let Ok(mut client) = FlightDataPlaneClient::connect(endpoint).await {
                let _ = client.delete(block).await;
            }
        })
        .await;
    }
}

fn is_retryable(error: &SparkXError) -> bool {
    matches!(
        error,
        SparkXError::Io(_) | SparkXError::Parquet(_) | SparkXError::Transport(_)
    )
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
