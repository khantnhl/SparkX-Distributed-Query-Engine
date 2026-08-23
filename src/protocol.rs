//! Transport-neutral coordinator and worker protocol contracts.
//!
//! The local runner and Flight control-plane transport use these messages as stable, validated
//! ownership boundaries without coupling scheduling state to one transport implementation.

use crate::catalog::Catalog;
use crate::execution::PhysicalPlan;
use crate::plan_codec::PhysicalPlanCodec;
use crate::{Result, SparkXError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QueryId(String);

impl QueryId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_text_id("query", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<()> {
        validate_text_id("query", &self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_text_id("worker", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<()> {
        validate_text_id("worker", &self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StageId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagePlan {
    pub query_id: QueryId,
    pub stage_id: StageId,
    pub input_stages: Vec<StageId>,
    pub partition_count: u32,
    /// Versioned Protobuf bytes produced by [`PhysicalPlanCodec`].
    pub plan_fragment: Vec<u8>,
}

impl StagePlan {
    pub fn from_physical_plan(
        query_id: QueryId,
        stage_id: StageId,
        input_stages: Vec<StageId>,
        partition_count: u32,
        plan: &PhysicalPlan,
    ) -> Result<Self> {
        let stage = Self {
            query_id,
            stage_id,
            input_stages,
            partition_count,
            plan_fragment: PhysicalPlanCodec::encode(plan)?,
        };
        stage.validate()?;
        Ok(stage)
    }

    pub fn decode_physical_plan(&self, catalog: &Catalog) -> Result<Arc<PhysicalPlan>> {
        self.validate()?;
        PhysicalPlanCodec::decode(&self.plan_fragment, catalog)
    }

    pub fn validate(&self) -> Result<()> {
        self.query_id.validate()?;
        if self.partition_count == 0 {
            return Err(protocol_error(
                "stage partition count must be greater than zero",
            ));
        }
        PhysicalPlanCodec::validate_fragment(&self.plan_fragment)?;
        let mut dependencies = BTreeSet::new();
        for dependency in &self.input_stages {
            if dependency == &self.stage_id {
                return Err(protocol_error("stage cannot depend on itself"));
            }
            if !dependencies.insert(*dependency) {
                return Err(protocol_error("stage dependencies must be unique"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttemptId {
    pub query_id: QueryId,
    pub stage_id: StageId,
    pub partition_id: PartitionId,
    pub attempt: u32,
}

impl TaskAttemptId {
    pub fn validate_for(&self, stage: &StagePlan) -> Result<()> {
        self.query_id.validate()?;
        if self.query_id != stage.query_id {
            return Err(protocol_error("task query does not match its stage"));
        }
        if self.stage_id != stage.stage_id {
            return Err(protocol_error("task stage does not match its stage plan"));
        }
        if self.partition_id.0 >= stage.partition_count {
            return Err(protocol_error(format!(
                "task partition {} is outside stage partition count {}",
                self.partition_id.0, stage.partition_count
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskLease {
    pub worker_id: WorkerId,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

impl TaskLease {
    pub fn validate(&self) -> Result<()> {
        self.worker_id.validate()?;
        if self.expires_at_ms <= self.issued_at_ms {
            return Err(protocol_error("task lease must expire after it is issued"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "location", rename_all = "snake_case")]
pub enum ShuffleLocation {
    Worker { worker_id: WorkerId },
    ObjectStore { uri: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShuffleBlock {
    pub producer: TaskAttemptId,
    pub output_partition: PartitionId,
    pub rows: u64,
    pub bytes: u64,
    pub checksum: String,
    pub location: ShuffleLocation,
}

impl ShuffleBlock {
    fn validate_for(&self, task: &TaskAttemptId, reporting_worker: &WorkerId) -> Result<()> {
        if &self.producer != task {
            return Err(protocol_error(
                "shuffle block producer does not match task update",
            ));
        }
        if self.checksum.trim().is_empty() {
            return Err(protocol_error("shuffle block checksum must not be empty"));
        }
        match &self.location {
            ShuffleLocation::Worker { worker_id } => {
                worker_id.validate()?;
                if worker_id != reporting_worker {
                    return Err(protocol_error(
                        "in-memory shuffle block must be owned by the reporting worker",
                    ));
                }
            }
            ShuffleLocation::ObjectStore { uri } if uri.trim().is_empty() => {
                return Err(protocol_error(
                    "shuffle object-store location must not be empty",
                ));
            }
            ShuffleLocation::ObjectStore { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TaskState {
    Running {
        started_at_ms: u64,
    },
    Succeeded {
        finished_at_ms: u64,
        output_blocks: Vec<ShuffleBlock>,
    },
    Failed {
        finished_at_ms: u64,
        error: String,
        retryable: bool,
    },
    Cancelled {
        finished_at_ms: u64,
        reason: String,
    },
}

impl TaskState {
    fn validate_for(&self, task: &TaskAttemptId, worker_id: &WorkerId) -> Result<()> {
        match self {
            Self::Succeeded { output_blocks, .. } => {
                for block in output_blocks {
                    block.validate_for(task, worker_id)?;
                }
            }
            Self::Failed { error, .. } if error.trim().is_empty() => {
                return Err(protocol_error("failed task must include an error"));
            }
            Self::Cancelled { reason, .. } if reason.trim().is_empty() => {
                return Err(protocol_error("cancelled task must include a reason"));
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub worker_id: WorkerId,
    pub slots: u32,
    pub memory_bytes: u64,
}

impl WorkerRegistration {
    fn validate(&self) -> Result<()> {
        self.worker_id.validate()?;
        if self.slots == 0 {
            return Err(protocol_error("worker must advertise at least one slot"));
        }
        if self.memory_bytes == 0 {
            return Err(protocol_error("worker must advertise non-zero memory"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    pub worker_id: WorkerId,
    pub observed_at_ms: u64,
    pub available_slots: u32,
    pub available_memory_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoordinatorMessage {
    AssignTask {
        version: u16,
        stage: StagePlan,
        task: TaskAttemptId,
        lease: TaskLease,
    },
    CancelQuery {
        version: u16,
        query_id: QueryId,
        reason: String,
    },
}

impl CoordinatorMessage {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::AssignTask {
                version,
                stage,
                task,
                lease,
            } => {
                validate_version(*version)?;
                stage.validate()?;
                task.validate_for(stage)?;
                lease.validate()
            }
            Self::CancelQuery {
                version,
                query_id,
                reason,
            } => {
                validate_version(*version)?;
                query_id.validate()?;
                if reason.trim().is_empty() {
                    return Err(protocol_error("query cancellation must include a reason"));
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    Register {
        version: u16,
        registration: WorkerRegistration,
    },
    Heartbeat {
        version: u16,
        heartbeat: WorkerHeartbeat,
    },
    TaskUpdate {
        version: u16,
        worker_id: WorkerId,
        task: TaskAttemptId,
        state: TaskState,
    },
}

impl WorkerMessage {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Register {
                version,
                registration,
            } => {
                validate_version(*version)?;
                registration.validate()
            }
            Self::Heartbeat { version, heartbeat } => {
                validate_version(*version)?;
                heartbeat.worker_id.validate()
            }
            Self::TaskUpdate {
                version,
                worker_id,
                task,
                state,
            } => {
                validate_version(*version)?;
                worker_id.validate()?;
                task.query_id.validate()?;
                state.validate_for(task, worker_id)
            }
        }
    }
}

fn validate_version(version: u16) -> Result<()> {
    if version != PROTOCOL_VERSION {
        return Err(protocol_error(format!(
            "unsupported protocol version {version}; expected {PROTOCOL_VERSION}"
        )));
    }
    Ok(())
}

fn validate_text_id(kind: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(protocol_error(format!("{kind} ID must not be empty")));
    }
    if value.len() > 128 {
        return Err(protocol_error(format!(
            "{kind} ID must not exceed 128 bytes"
        )));
    }
    Ok(())
}

fn protocol_error(message: impl Into<String>) -> SparkXError {
    SparkXError::protocol(format!(
        "invalid distributed protocol message: {}",
        message.into()
    ))
}
