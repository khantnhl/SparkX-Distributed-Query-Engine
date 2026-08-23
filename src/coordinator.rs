//! Deterministic in-memory coordinator state for the distributed control plane.
//!
//! Transport servers can feed validated [`WorkerMessage`] values into this state machine and send
//! the returned [`CoordinatorMessage`] assignments over any RPC implementation. Keeping scheduling
//! state independent from the transport makes lease, retry, cancellation, and ownership behavior
//! directly testable before workers move out of process.

use crate::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, PartitionId, QueryId, ShuffleBlock, StageId, StagePlan,
    TaskAttemptId, TaskLease, TaskState, WorkerHeartbeat, WorkerId, WorkerMessage,
    WorkerRegistration,
};
use crate::{Result, SparkXError};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorConfig {
    pub lease_duration_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub max_task_attempts: u32,
    pub max_stage_partitions: u32,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lease_duration_ms: 30_000,
            heartbeat_timeout_ms: 15_000,
            max_task_attempts: 3,
            max_stage_partitions: 100_000,
        }
    }
}

impl CoordinatorConfig {
    fn validate(self) -> Result<Self> {
        if self.lease_duration_ms == 0 {
            return Err(coordinator_error(
                "lease duration must be greater than zero",
            ));
        }
        if self.heartbeat_timeout_ms == 0 {
            return Err(coordinator_error(
                "heartbeat timeout must be greater than zero",
            ));
        }
        if self.max_task_attempts == 0 {
            return Err(coordinator_error(
                "maximum task attempts must be greater than zero",
            ));
        }
        if self.max_stage_partitions == 0 {
            return Err(coordinator_error(
                "maximum stage partitions must be greater than zero",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageStatus {
    Blocked,
    Ready,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionStatus {
    Pending { next_attempt: u32 },
    Running { attempt: u32 },
    Succeeded { attempt: u32 },
    Failed { attempt: u32, error: String },
    Cancelled,
}

#[derive(Debug)]
pub struct Coordinator {
    config: CoordinatorConfig,
    workers: BTreeMap<WorkerId, WorkerRuntime>,
    stages: BTreeMap<StageKey, StageRuntime>,
    cancelled_queries: BTreeMap<QueryId, String>,
}

type StageKey = (QueryId, StageId);

#[derive(Debug)]
struct WorkerRuntime {
    registration: WorkerRegistration,
    last_observed_at_ms: u64,
    last_received_at_ms: u64,
    available_slots: u32,
    available_memory_bytes: u64,
    alive: bool,
}

#[derive(Debug)]
struct StageRuntime {
    plan: StagePlan,
    partitions: Vec<PartitionRuntime>,
}

#[derive(Debug)]
enum PartitionRuntime {
    Pending {
        next_attempt: u32,
    },
    Active {
        task: TaskAttemptId,
        worker_id: WorkerId,
        lease: TaskLease,
        started_at_ms: Option<u64>,
    },
    Succeeded {
        task: TaskAttemptId,
        output_blocks: Vec<ShuffleBlock>,
    },
    Failed {
        attempt: u32,
        error: String,
    },
    Cancelled,
}

impl Coordinator {
    pub fn new(config: CoordinatorConfig) -> Result<Self> {
        Ok(Self {
            config: config.validate()?,
            workers: BTreeMap::new(),
            stages: BTreeMap::new(),
            cancelled_queries: BTreeMap::new(),
        })
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn worker_available_slots(&self, worker_id: &WorkerId) -> Option<u32> {
        self.workers
            .get(worker_id)
            .filter(|worker| worker.alive)
            .map(|worker| worker.available_slots)
    }

    pub fn active_workers_for_query(&self, query_id: &QueryId) -> Result<Vec<WorkerId>> {
        if !self
            .stages
            .keys()
            .any(|(stage_query_id, _)| stage_query_id == query_id)
        {
            return Err(SparkXError::NotFound(format!(
                "query {} is not registered with the coordinator",
                query_id.as_str()
            )));
        }
        let mut workers = self
            .stages
            .iter()
            .filter(|((stage_query_id, _), _)| stage_query_id == query_id)
            .flat_map(|(_, stage)| stage.partitions.iter())
            .filter_map(|partition| match partition {
                PartitionRuntime::Active { worker_id, .. } => Some(worker_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        workers.sort();
        workers.dedup();
        Ok(workers)
    }

    pub fn submit_stage(&mut self, stage: StagePlan) -> Result<()> {
        stage.validate()?;
        if stage.partition_count > self.config.max_stage_partitions {
            return Err(coordinator_error(format!(
                "stage has {} partitions but the configured maximum is {}",
                stage.partition_count, self.config.max_stage_partitions
            )));
        }
        if self.cancelled_queries.contains_key(&stage.query_id) {
            return Err(coordinator_error(format!(
                "query {} is already cancelled",
                stage.query_id.as_str()
            )));
        }

        let key = (stage.query_id.clone(), stage.stage_id);
        if self.stages.contains_key(&key) {
            return Err(coordinator_error(format!(
                "stage {} for query {} is already submitted",
                stage.stage_id.0,
                stage.query_id.as_str()
            )));
        }
        for dependency in &stage.input_stages {
            let dependency_key = (stage.query_id.clone(), *dependency);
            if !self.stages.contains_key(&dependency_key) {
                return Err(coordinator_error(format!(
                    "stage {} depends on unsubmitted stage {}",
                    stage.stage_id.0, dependency.0
                )));
            }
        }

        let partitions = (0..stage.partition_count)
            .map(|_| PartitionRuntime::Pending { next_attempt: 0 })
            .collect();
        self.stages.insert(
            key,
            StageRuntime {
                plan: stage,
                partitions,
            },
        );
        Ok(())
    }

    pub fn handle_worker_message(
        &mut self,
        message: WorkerMessage,
        received_at_ms: u64,
    ) -> Result<()> {
        message.validate()?;
        self.advance_time(received_at_ms)?;
        match message {
            WorkerMessage::Register { registration, .. } => {
                self.register_worker(registration, received_at_ms)
            }
            WorkerMessage::Heartbeat { heartbeat, .. } => {
                self.record_heartbeat(heartbeat, received_at_ms)
            }
            WorkerMessage::TaskUpdate {
                worker_id,
                task,
                state,
                ..
            } => self.record_task_update(worker_id, task, state, received_at_ms),
        }
    }

    pub fn next_assignment(&mut self, now_ms: u64) -> Result<Option<CoordinatorMessage>> {
        self.advance_time(now_ms)?;

        let Some(worker_id) = self
            .workers
            .iter()
            .find(|(_, worker)| worker.alive && worker.available_slots > 0)
            .map(|(worker_id, _)| worker_id.clone())
        else {
            return Ok(None);
        };

        self.assign_to_worker(worker_id, now_ms)
    }

    pub fn next_assignment_for(
        &mut self,
        worker_id: &WorkerId,
        now_ms: u64,
    ) -> Result<Option<CoordinatorMessage>> {
        self.advance_time(now_ms)?;
        let worker = self.workers.get(worker_id).ok_or_else(|| {
            coordinator_error(format!(
                "unregistered worker {} requested an assignment",
                worker_id.as_str()
            ))
        })?;
        if !worker.alive || worker.available_slots == 0 {
            return Ok(None);
        }
        self.assign_to_worker(worker_id.clone(), now_ms)
    }

    fn assign_to_worker(
        &mut self,
        worker_id: WorkerId,
        now_ms: u64,
    ) -> Result<Option<CoordinatorMessage>> {
        let Some((stage_key, partition_index, attempt)) = self.next_pending_partition() else {
            return Ok(None);
        };
        let expires_at_ms = now_ms
            .checked_add(self.config.lease_duration_ms)
            .ok_or_else(|| coordinator_error("task lease timestamp overflowed"))?;
        let task = TaskAttemptId {
            query_id: stage_key.0.clone(),
            stage_id: stage_key.1,
            partition_id: PartitionId(
                u32::try_from(partition_index)
                    .map_err(|_| coordinator_error("partition index exceeds u32"))?,
            ),
            attempt,
        };
        let lease = TaskLease {
            worker_id: worker_id.clone(),
            issued_at_ms: now_ms,
            expires_at_ms,
        };

        let stage = self
            .stages
            .get_mut(&stage_key)
            .expect("selected stage must exist");
        stage.partitions[partition_index] = PartitionRuntime::Active {
            task: task.clone(),
            worker_id: worker_id.clone(),
            lease: lease.clone(),
            started_at_ms: None,
        };
        self.workers
            .get_mut(&worker_id)
            .expect("selected worker must exist")
            .available_slots -= 1;

        let assignment = CoordinatorMessage::AssignTask {
            version: PROTOCOL_VERSION,
            stage: stage.plan.clone(),
            task,
            lease,
        };
        assignment.validate()?;
        Ok(Some(assignment))
    }

    pub fn cancel_query(
        &mut self,
        query_id: QueryId,
        reason: impl Into<String>,
    ) -> Result<CoordinatorMessage> {
        let reason = reason.into();
        let message = CoordinatorMessage::CancelQuery {
            version: PROTOCOL_VERSION,
            query_id: query_id.clone(),
            reason: reason.clone(),
        };
        message.validate()?;
        if !self
            .stages
            .keys()
            .any(|(stage_query_id, _)| stage_query_id == &query_id)
        {
            return Err(SparkXError::NotFound(format!(
                "query {} is not registered with the coordinator",
                query_id.as_str()
            )));
        }
        if let Some(existing_reason) = self.cancelled_queries.get(&query_id) {
            if existing_reason == &reason {
                return Ok(message);
            }
            return Err(coordinator_error(format!(
                "query {} was already cancelled for a different reason",
                query_id.as_str()
            )));
        }

        let active_workers = self
            .stages
            .iter()
            .filter(|((stage_query_id, _), _)| stage_query_id == &query_id)
            .flat_map(|(_, stage)| stage.partitions.iter())
            .filter_map(|partition| match partition {
                PartitionRuntime::Active { worker_id, .. } => Some(worker_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        for worker_id in active_workers {
            self.release_worker_slot(&worker_id);
        }
        for ((stage_query_id, _), stage) in &mut self.stages {
            if stage_query_id != &query_id {
                continue;
            }
            for partition in &mut stage.partitions {
                if !matches!(partition, PartitionRuntime::Succeeded { .. }) {
                    *partition = PartitionRuntime::Cancelled;
                }
            }
        }
        self.cancelled_queries.insert(query_id, reason);
        Ok(message)
    }

    pub fn advance_time(&mut self, now_ms: u64) -> Result<()> {
        let timed_out_workers = self
            .workers
            .iter()
            .filter(|(_, worker)| {
                worker.alive
                    && now_ms.saturating_sub(worker.last_received_at_ms)
                        > self.config.heartbeat_timeout_ms
            })
            .map(|(worker_id, _)| worker_id.clone())
            .collect::<Vec<_>>();
        for worker_id in &timed_out_workers {
            let worker = self
                .workers
                .get_mut(worker_id)
                .expect("timed-out worker must exist");
            worker.alive = false;
            worker.available_slots = 0;
            worker.available_memory_bytes = 0;
        }

        let mut expired = Vec::new();
        for (stage_key, stage) in &self.stages {
            for (partition_index, partition) in stage.partitions.iter().enumerate() {
                let PartitionRuntime::Active {
                    task,
                    worker_id,
                    lease,
                    ..
                } = partition
                else {
                    continue;
                };
                let worker_timed_out = timed_out_workers.contains(worker_id);
                if worker_timed_out || lease.expires_at_ms <= now_ms {
                    expired.push((
                        stage_key.clone(),
                        partition_index,
                        task.attempt,
                        worker_id.clone(),
                        worker_timed_out,
                    ));
                }
            }
        }

        for (stage_key, partition_index, attempt, worker_id, worker_timed_out) in expired {
            if !worker_timed_out {
                self.release_worker_slot(&worker_id);
            }
            let replacement = retry_or_fail(
                self.config.max_task_attempts,
                attempt,
                if worker_timed_out {
                    "worker heartbeat timed out"
                } else {
                    "task lease expired"
                },
            );
            self.stages
                .get_mut(&stage_key)
                .expect("expired task stage must exist")
                .partitions[partition_index] = replacement;
        }
        Ok(())
    }

    pub fn stage_status(&self, query_id: &QueryId, stage_id: StageId) -> Result<StageStatus> {
        let key = (query_id.clone(), stage_id);
        let stage = self
            .stages
            .get(&key)
            .ok_or_else(|| stage_not_found(query_id, stage_id))?;
        if self.cancelled_queries.contains_key(query_id) {
            return Ok(StageStatus::Cancelled);
        }
        if stage
            .partitions
            .iter()
            .any(|partition| matches!(partition, PartitionRuntime::Failed { .. }))
        {
            return Ok(StageStatus::Failed);
        }
        if stage
            .partitions
            .iter()
            .all(|partition| matches!(partition, PartitionRuntime::Succeeded { .. }))
        {
            return Ok(StageStatus::Succeeded);
        }
        if stage
            .partitions
            .iter()
            .any(|partition| matches!(partition, PartitionRuntime::Active { .. }))
        {
            return Ok(StageStatus::Running);
        }
        if self.dependencies_succeeded(stage) {
            Ok(StageStatus::Ready)
        } else {
            Ok(StageStatus::Blocked)
        }
    }

    pub fn partition_status(
        &self,
        query_id: &QueryId,
        stage_id: StageId,
        partition_id: PartitionId,
    ) -> Result<PartitionStatus> {
        let stage = self
            .stages
            .get(&(query_id.clone(), stage_id))
            .ok_or_else(|| stage_not_found(query_id, stage_id))?;
        let partition = stage
            .partitions
            .get(partition_id.0 as usize)
            .ok_or_else(|| {
                SparkXError::NotFound(format!(
                    "partition {} does not exist in query {} stage {}",
                    partition_id.0,
                    query_id.as_str(),
                    stage_id.0
                ))
            })?;
        Ok(match partition {
            PartitionRuntime::Pending { next_attempt } => PartitionStatus::Pending {
                next_attempt: *next_attempt,
            },
            PartitionRuntime::Active { task, .. } => PartitionStatus::Running {
                attempt: task.attempt,
            },
            PartitionRuntime::Succeeded { task, .. } => PartitionStatus::Succeeded {
                attempt: task.attempt,
            },
            PartitionRuntime::Failed { attempt, error } => PartitionStatus::Failed {
                attempt: *attempt,
                error: error.clone(),
            },
            PartitionRuntime::Cancelled => PartitionStatus::Cancelled,
        })
    }

    pub fn stage_output_blocks(
        &self,
        query_id: &QueryId,
        stage_id: StageId,
    ) -> Result<Vec<ShuffleBlock>> {
        let stage = self
            .stages
            .get(&(query_id.clone(), stage_id))
            .ok_or_else(|| stage_not_found(query_id, stage_id))?;
        if !stage
            .partitions
            .iter()
            .all(|partition| matches!(partition, PartitionRuntime::Succeeded { .. }))
        {
            return Err(coordinator_error(format!(
                "query {} stage {} has not succeeded",
                query_id.as_str(),
                stage_id.0
            )));
        }
        Ok(stage
            .partitions
            .iter()
            .flat_map(|partition| match partition {
                PartitionRuntime::Succeeded { output_blocks, .. } => output_blocks.clone(),
                _ => Vec::new(),
            })
            .collect())
    }

    fn register_worker(
        &mut self,
        registration: WorkerRegistration,
        received_at_ms: u64,
    ) -> Result<()> {
        if self.workers.contains_key(&registration.worker_id) {
            return Err(coordinator_error(format!(
                "worker {} is already registered",
                registration.worker_id.as_str()
            )));
        }
        self.workers.insert(
            registration.worker_id.clone(),
            WorkerRuntime {
                available_slots: registration.slots,
                available_memory_bytes: registration.memory_bytes,
                registration,
                last_observed_at_ms: 0,
                last_received_at_ms: received_at_ms,
                alive: true,
            },
        );
        Ok(())
    }

    fn record_heartbeat(&mut self, heartbeat: WorkerHeartbeat, received_at_ms: u64) -> Result<()> {
        let active_tasks = self.active_tasks_for_worker(&heartbeat.worker_id);
        let worker = self.workers.get_mut(&heartbeat.worker_id).ok_or_else(|| {
            coordinator_error(format!(
                "heartbeat came from unregistered worker {}",
                heartbeat.worker_id.as_str()
            ))
        })?;
        if heartbeat.observed_at_ms < worker.last_observed_at_ms {
            return Err(coordinator_error(format!(
                "worker {} heartbeat time moved backwards",
                heartbeat.worker_id.as_str()
            )));
        }
        let maximum_available_slots = worker.registration.slots.saturating_sub(active_tasks);
        if heartbeat.available_slots > maximum_available_slots {
            return Err(coordinator_error(format!(
                "worker {} reports {} available slots but at most {} are possible",
                heartbeat.worker_id.as_str(),
                heartbeat.available_slots,
                maximum_available_slots
            )));
        }
        if heartbeat.available_memory_bytes > worker.registration.memory_bytes {
            return Err(coordinator_error(format!(
                "worker {} reports more memory than it registered",
                heartbeat.worker_id.as_str()
            )));
        }
        worker.last_observed_at_ms = heartbeat.observed_at_ms;
        worker.last_received_at_ms = received_at_ms;
        worker.available_slots = heartbeat.available_slots;
        worker.available_memory_bytes = heartbeat.available_memory_bytes;
        worker.alive = true;
        Ok(())
    }

    fn record_task_update(
        &mut self,
        worker_id: WorkerId,
        task: TaskAttemptId,
        state: TaskState,
        received_at_ms: u64,
    ) -> Result<()> {
        let max_task_attempts = self.config.max_task_attempts;
        if !self.workers.contains_key(&worker_id) {
            return Err(coordinator_error(format!(
                "task update came from unregistered worker {}",
                worker_id.as_str()
            )));
        }
        let key = (task.query_id.clone(), task.stage_id);
        let partition_index = task.partition_id.0 as usize;
        let stage = self
            .stages
            .get_mut(&key)
            .ok_or_else(|| stage_not_found(&task.query_id, task.stage_id))?;
        task.validate_for(&stage.plan)?;
        let partition = stage.partitions.get_mut(partition_index).ok_or_else(|| {
            coordinator_error(format!(
                "task partition {} is missing from coordinator state",
                task.partition_id.0
            ))
        })?;
        let PartitionRuntime::Active {
            task: active_task,
            worker_id: assigned_worker,
            lease,
            started_at_ms,
        } = partition
        else {
            return Err(coordinator_error(
                "task update is stale or was not assigned",
            ));
        };
        if active_task != &task {
            return Err(coordinator_error(
                "task update does not match the active attempt",
            ));
        }
        if assigned_worker != &worker_id || lease.worker_id != worker_id {
            return Err(coordinator_error(
                "task update came from a worker that does not own its lease",
            ));
        }
        if lease.expires_at_ms <= received_at_ms {
            return Err(coordinator_error(
                "task update arrived after its lease expired",
            ));
        }

        match state {
            TaskState::Running {
                started_at_ms: reported_start,
            } => {
                validate_reported_time("started", reported_start, lease, received_at_ms)?;
                if let Some(existing_start) = *started_at_ms {
                    if existing_start != reported_start {
                        return Err(coordinator_error("task start time changed between updates"));
                    }
                } else {
                    *started_at_ms = Some(reported_start);
                }
            }
            TaskState::Succeeded {
                finished_at_ms,
                output_blocks,
            } => {
                validate_reported_time("finished", finished_at_ms, lease, received_at_ms)?;
                *partition = PartitionRuntime::Succeeded {
                    task,
                    output_blocks,
                };
                self.release_worker_slot(&worker_id);
            }
            TaskState::Failed {
                finished_at_ms,
                error,
                retryable,
            } => {
                validate_reported_time("finished", finished_at_ms, lease, received_at_ms)?;
                let replacement = if retryable {
                    retry_or_fail(max_task_attempts, task.attempt, &error)
                } else {
                    PartitionRuntime::Failed {
                        attempt: task.attempt,
                        error,
                    }
                };
                *partition = replacement;
                self.release_worker_slot(&worker_id);
            }
            TaskState::Cancelled { finished_at_ms, .. } => {
                validate_reported_time("finished", finished_at_ms, lease, received_at_ms)?;
                *partition = PartitionRuntime::Cancelled;
                self.release_worker_slot(&worker_id);
            }
        }
        Ok(())
    }

    fn next_pending_partition(&self) -> Option<(StageKey, usize, u32)> {
        for (stage_key, stage) in &self.stages {
            if self.cancelled_queries.contains_key(&stage_key.0)
                || !self.dependencies_succeeded(stage)
            {
                continue;
            }
            if let Some((partition_index, attempt)) =
                stage
                    .partitions
                    .iter()
                    .enumerate()
                    .find_map(|(index, partition)| match partition {
                        PartitionRuntime::Pending { next_attempt } => Some((index, *next_attempt)),
                        _ => None,
                    })
            {
                return Some((stage_key.clone(), partition_index, attempt));
            }
        }
        None
    }

    fn dependencies_succeeded(&self, stage: &StageRuntime) -> bool {
        stage.plan.input_stages.iter().all(|dependency| {
            self.stages
                .get(&(stage.plan.query_id.clone(), *dependency))
                .is_some_and(|dependency| {
                    dependency
                        .partitions
                        .iter()
                        .all(|partition| matches!(partition, PartitionRuntime::Succeeded { .. }))
                })
        })
    }

    fn active_tasks_for_worker(&self, worker_id: &WorkerId) -> u32 {
        self.stages
            .values()
            .flat_map(|stage| stage.partitions.iter())
            .filter(|partition| {
                matches!(
                    partition,
                    PartitionRuntime::Active {
                        worker_id: assigned_worker,
                        ..
                    } if assigned_worker == worker_id
                )
            })
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn release_worker_slot(&mut self, worker_id: &WorkerId) {
        if let Some(worker) = self.workers.get_mut(worker_id) {
            if worker.alive {
                worker.available_slots = worker
                    .available_slots
                    .saturating_add(1)
                    .min(worker.registration.slots);
            }
        }
    }
}

fn retry_or_fail(max_task_attempts: u32, attempt: u32, error: &str) -> PartitionRuntime {
    let next_attempt = attempt.saturating_add(1);
    if next_attempt < max_task_attempts {
        PartitionRuntime::Pending { next_attempt }
    } else {
        PartitionRuntime::Failed {
            attempt,
            error: error.to_owned(),
        }
    }
}

fn validate_reported_time(
    event: &str,
    timestamp_ms: u64,
    lease: &TaskLease,
    received_at_ms: u64,
) -> Result<()> {
    if timestamp_ms < lease.issued_at_ms {
        return Err(coordinator_error(format!(
            "task {event} before its lease was issued"
        )));
    }
    if timestamp_ms >= lease.expires_at_ms {
        return Err(coordinator_error(format!(
            "task {event} after its lease expired"
        )));
    }
    if timestamp_ms > received_at_ms {
        return Err(coordinator_error(format!(
            "task {event} time is later than the coordinator receive time"
        )));
    }
    Ok(())
}

fn stage_not_found(query_id: &QueryId, stage_id: StageId) -> SparkXError {
    SparkXError::NotFound(format!(
        "query {} stage {} is not registered with the coordinator",
        query_id.as_str(),
        stage_id.0
    ))
}

fn coordinator_error(message: impl Into<String>) -> SparkXError {
    SparkXError::protocol(format!(
        "invalid coordinator transition: {}",
        message.into()
    ))
}
