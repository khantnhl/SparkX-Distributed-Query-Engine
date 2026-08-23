//! Standalone worker runtime for executing leased physical-plan fragments.

use crate::cancellation::CancellationToken;
use crate::catalog::Catalog;
use crate::control_plane::ControlPlaneClient;
use crate::execution::{TaskContext, execute};
use crate::memory::QueryMemory;
use crate::metrics::{MetricsSnapshot, QueryMetrics};
use crate::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, QueryId, StagePlan, TaskAttemptId, TaskLease, TaskState,
    WorkerHeartbeat, WorkerId, WorkerMessage, WorkerRegistration,
};
use crate::{Result, SparkXError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
                            CoordinatorMessage::AssignTask { stage, task, lease, .. } => {
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
                                tasks.spawn(execute_assignment(
                                    stage,
                                    task,
                                    lease,
                                    self.catalog.clone(),
                                    TaskContext {
                                        batch_size: self.config.batch_size,
                                        channel_capacity: self.config.channel_capacity,
                                        partition: None,
                                        metrics: metrics.clone(),
                                        memory: memory.clone(),
                                        cancellation,
                                    },
                                ));
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
                                    output_blocks: Vec::new(),
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
                    client.send_worker_message(WorkerMessage::TaskUpdate {
                        version: PROTOCOL_VERSION,
                        worker_id: self.config.worker_id.clone(),
                        task: completion.task,
                        state,
                    }).await?;

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
        Ok(WorkerRunSummary {
            completed_tasks: summary.completed_tasks,
            failed_tasks: summary.failed_tasks,
            cancelled_tasks: summary.cancelled_tasks,
            output_rows: summary.output_rows,
            output_bytes: summary.output_bytes,
            metrics: metrics.snapshot(),
        })
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
}

async fn execute_assignment(
    stage: StagePlan,
    task: TaskAttemptId,
    lease: TaskLease,
    catalog: Arc<Catalog>,
    mut context: TaskContext,
) -> TaskCompletion {
    context.partition = Some(task.partition_id.0 as usize);
    let result = async {
        context.cancellation.check()?;
        let plan = stage.decode_physical_plan(catalog.as_ref())?;
        let batches = execute(plan, context.clone()).collect().await?;
        context.cancellation.check()?;
        Ok(TaskOutput {
            rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
            bytes: batches
                .iter()
                .map(|batch| batch.get_array_memory_size() as u64)
                .sum(),
        })
    }
    .await;
    TaskCompletion {
        task,
        lease,
        result,
    }
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

fn is_retryable(error: &SparkXError) -> bool {
    matches!(error, SparkXError::Io(_) | SparkXError::Parquet(_))
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
