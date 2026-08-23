use arrow::datatypes::{DataType, Field, Schema};
use sparkx::SparkXError;
use sparkx::catalog::MemoryTable;
use sparkx::coordinator::{Coordinator, CoordinatorConfig, PartitionStatus, StageStatus};
use sparkx::execution::PhysicalPlan;
use sparkx::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, PartitionId, QueryId, ShuffleBlock, ShuffleLocation,
    StageId, StagePlan, TaskAttemptId, TaskState, WorkerHeartbeat, WorkerId, WorkerMessage,
    WorkerRegistration,
};
use std::sync::Arc;

fn query_id() -> QueryId {
    QueryId::new("query-coordinator").unwrap()
}

fn stage(stage_id: u32, input_stages: Vec<u32>, partition_count: u32) -> StagePlan {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let provider = Arc::new(MemoryTable::new(schema.clone(), vec![Vec::new()]).unwrap());
    let plan = PhysicalPlan::Scan {
        id: 0,
        table_name: "input".to_owned(),
        provider,
        projection: None,
        filters: Vec::new(),
        schema,
    };
    StagePlan::from_physical_plan(
        query_id(),
        StageId(stage_id),
        input_stages.into_iter().map(StageId).collect(),
        partition_count,
        &plan,
    )
    .unwrap()
}

fn coordinator(lease_duration_ms: u64, heartbeat_timeout_ms: u64, attempts: u32) -> Coordinator {
    Coordinator::new(CoordinatorConfig {
        lease_duration_ms,
        heartbeat_timeout_ms,
        max_task_attempts: attempts,
        ..CoordinatorConfig::default()
    })
    .unwrap()
}

fn register(coordinator: &mut Coordinator, name: &str, slots: u32, at_ms: u64) {
    coordinator
        .handle_worker_message(
            WorkerMessage::Register {
                version: PROTOCOL_VERSION,
                registration: WorkerRegistration {
                    worker_id: WorkerId::new(name).unwrap(),
                    slots,
                    memory_bytes: 1_024,
                },
            },
            at_ms,
        )
        .unwrap();
}

fn assignment(coordinator: &mut Coordinator, at_ms: u64) -> (StagePlan, TaskAttemptId, String) {
    match coordinator.next_assignment(at_ms).unwrap().unwrap() {
        CoordinatorMessage::AssignTask {
            stage, task, lease, ..
        } => (stage, task, lease.worker_id.as_str().to_owned()),
        CoordinatorMessage::CancelQuery { .. } => panic!("expected task assignment"),
    }
}

fn succeed(
    coordinator: &mut Coordinator,
    worker: &str,
    task: TaskAttemptId,
    at_ms: u64,
    output_blocks: Vec<ShuffleBlock>,
) {
    coordinator
        .handle_worker_message(
            WorkerMessage::TaskUpdate {
                version: PROTOCOL_VERSION,
                worker_id: WorkerId::new(worker).unwrap(),
                task,
                state: TaskState::Succeeded {
                    finished_at_ms: at_ms,
                    output_blocks,
                },
            },
            at_ms,
        )
        .unwrap();
}

#[test]
fn schedules_ready_stages_deterministically_and_unblocks_dependencies() {
    let mut coordinator = coordinator(100, 50, 3);
    coordinator.submit_stage(stage(0, Vec::new(), 2)).unwrap();
    coordinator.submit_stage(stage(1, vec![0], 1)).unwrap();
    register(&mut coordinator, "worker-b", 1, 0);
    register(&mut coordinator, "worker-a", 1, 0);

    assert_eq!(
        coordinator.stage_status(&query_id(), StageId(0)).unwrap(),
        StageStatus::Ready
    );
    assert_eq!(
        coordinator.stage_status(&query_id(), StageId(1)).unwrap(),
        StageStatus::Blocked
    );

    let (first_stage, first, first_worker) = assignment(&mut coordinator, 1);
    let (_, second, second_worker) = assignment(&mut coordinator, 1);
    assert_eq!(first_stage.stage_id, StageId(0));
    assert_eq!(first.partition_id, PartitionId(0));
    assert_eq!(second.partition_id, PartitionId(1));
    assert_eq!(first_worker, "worker-a");
    assert_eq!(second_worker, "worker-b");
    assert!(coordinator.next_assignment(1).unwrap().is_none());

    succeed(&mut coordinator, &first_worker, first, 2, Vec::new());
    succeed(&mut coordinator, &second_worker, second, 2, Vec::new());
    assert_eq!(
        coordinator.stage_status(&query_id(), StageId(0)).unwrap(),
        StageStatus::Succeeded
    );
    assert_eq!(
        coordinator.stage_status(&query_id(), StageId(1)).unwrap(),
        StageStatus::Ready
    );

    let (dependent_stage, dependent, dependent_worker) = assignment(&mut coordinator, 3);
    assert_eq!(dependent_stage.stage_id, StageId(1));
    assert_eq!(dependent.partition_id, PartitionId(0));
    assert_eq!(dependent_worker, "worker-a");
}

#[test]
fn worker_specific_polling_never_leases_another_workers_task() {
    let mut coordinator = coordinator(100, 50, 3);
    coordinator.submit_stage(stage(0, Vec::new(), 2)).unwrap();
    register(&mut coordinator, "worker-a", 1, 0);
    register(&mut coordinator, "worker-b", 1, 0);
    let worker_a = WorkerId::new("worker-a").unwrap();
    let worker_b = WorkerId::new("worker-b").unwrap();

    let first = coordinator
        .next_assignment_for(&worker_b, 1)
        .unwrap()
        .unwrap();
    let CoordinatorMessage::AssignTask { task, lease, .. } = first else {
        panic!("expected task assignment");
    };
    assert_eq!(lease.worker_id, worker_b);
    assert_eq!(task.partition_id, PartitionId(0));
    assert!(
        coordinator
            .next_assignment_for(&lease.worker_id, 1)
            .unwrap()
            .is_none()
    );

    let second = coordinator
        .next_assignment_for(&worker_a, 1)
        .unwrap()
        .unwrap();
    let CoordinatorMessage::AssignTask { task, lease, .. } = second else {
        panic!("expected task assignment");
    };
    assert_eq!(lease.worker_id, worker_a);
    assert_eq!(task.partition_id, PartitionId(1));
}

#[test]
fn validates_registration_and_heartbeat_resources() {
    assert!(
        Coordinator::new(CoordinatorConfig {
            lease_duration_ms: 0,
            ..CoordinatorConfig::default()
        })
        .is_err()
    );

    let mut coordinator = coordinator(100, 50, 3);
    register(&mut coordinator, "worker-a", 2, 100);
    assert_eq!(coordinator.worker_count(), 1);
    assert_eq!(
        coordinator.worker_available_slots(&WorkerId::new("worker-a").unwrap()),
        Some(2)
    );

    let duplicate = WorkerMessage::Register {
        version: PROTOCOL_VERSION,
        registration: WorkerRegistration {
            worker_id: WorkerId::new("worker-a").unwrap(),
            slots: 2,
            memory_bytes: 1_024,
        },
    };
    assert!(matches!(
        coordinator.handle_worker_message(duplicate, 101),
        Err(SparkXError::Protocol(_))
    ));

    let oversized = WorkerMessage::Heartbeat {
        version: PROTOCOL_VERSION,
        heartbeat: WorkerHeartbeat {
            worker_id: WorkerId::new("worker-a").unwrap(),
            observed_at_ms: 101,
            available_slots: 3,
            available_memory_bytes: 1_024,
        },
    };
    assert!(matches!(
        coordinator.handle_worker_message(oversized, 101),
        Err(SparkXError::Protocol(_))
    ));

    let heartbeat = WorkerMessage::Heartbeat {
        version: PROTOCOL_VERSION,
        heartbeat: WorkerHeartbeat {
            worker_id: WorkerId::new("worker-a").unwrap(),
            observed_at_ms: 102,
            available_slots: 1,
            available_memory_bytes: 900,
        },
    };
    coordinator.handle_worker_message(heartbeat, 102).unwrap();
    assert_eq!(
        coordinator.worker_available_slots(&WorkerId::new("worker-a").unwrap()),
        Some(1)
    );
}

#[test]
fn rejects_invalid_stage_and_task_ownership_transitions() {
    let mut coordinator = Coordinator::new(CoordinatorConfig {
        max_stage_partitions: 2,
        ..CoordinatorConfig::default()
    })
    .unwrap();
    assert!(matches!(
        coordinator.submit_stage(stage(1, vec![0], 1)),
        Err(SparkXError::Protocol(_))
    ));
    assert!(matches!(
        coordinator.submit_stage(stage(0, Vec::new(), 3)),
        Err(SparkXError::Protocol(_))
    ));

    coordinator.submit_stage(stage(0, Vec::new(), 1)).unwrap();
    assert!(matches!(
        coordinator.submit_stage(stage(0, Vec::new(), 1)),
        Err(SparkXError::Protocol(_))
    ));
    register(&mut coordinator, "worker-a", 1, 0);
    register(&mut coordinator, "worker-b", 1, 0);
    let (_, task, assigned_worker) = assignment(&mut coordinator, 1);
    assert_eq!(assigned_worker, "worker-a");

    let wrong_owner = WorkerMessage::TaskUpdate {
        version: PROTOCOL_VERSION,
        worker_id: WorkerId::new("worker-b").unwrap(),
        task,
        state: TaskState::Running { started_at_ms: 1 },
    };
    assert!(matches!(
        coordinator.handle_worker_message(wrong_owner, 2),
        Err(SparkXError::Protocol(_))
    ));
    assert!(matches!(
        coordinator.cancel_query(QueryId::new("unknown-query").unwrap(), "not needed"),
        Err(SparkXError::NotFound(_))
    ));
}

#[test]
fn retries_expired_leases_and_rejects_stale_attempt_updates() {
    let mut coordinator = coordinator(10, 100, 2);
    coordinator.submit_stage(stage(0, Vec::new(), 1)).unwrap();
    register(&mut coordinator, "worker-a", 1, 0);

    let (_, first_attempt, _) = assignment(&mut coordinator, 0);
    assert_eq!(first_attempt.attempt, 0);
    coordinator.advance_time(10).unwrap();
    assert_eq!(
        coordinator
            .partition_status(&query_id(), StageId(0), PartitionId(0))
            .unwrap(),
        PartitionStatus::Pending { next_attempt: 1 }
    );

    let (_, second_attempt, _) = assignment(&mut coordinator, 10);
    assert_eq!(second_attempt.attempt, 1);
    let stale_update = WorkerMessage::TaskUpdate {
        version: PROTOCOL_VERSION,
        worker_id: WorkerId::new("worker-a").unwrap(),
        task: first_attempt,
        state: TaskState::Running { started_at_ms: 1 },
    };
    assert!(matches!(
        coordinator.handle_worker_message(stale_update, 11),
        Err(SparkXError::Protocol(_))
    ));

    coordinator
        .handle_worker_message(
            WorkerMessage::TaskUpdate {
                version: PROTOCOL_VERSION,
                worker_id: WorkerId::new("worker-a").unwrap(),
                task: second_attempt,
                state: TaskState::Failed {
                    finished_at_ms: 12,
                    error: "transient read failure".to_owned(),
                    retryable: true,
                },
            },
            12,
        )
        .unwrap();
    assert_eq!(
        coordinator.stage_status(&query_id(), StageId(0)).unwrap(),
        StageStatus::Failed
    );
    assert_eq!(
        coordinator
            .partition_status(&query_id(), StageId(0), PartitionId(0))
            .unwrap(),
        PartitionStatus::Failed {
            attempt: 1,
            error: "transient read failure".to_owned(),
        }
    );
    assert!(coordinator.next_assignment(13).unwrap().is_none());
}

#[test]
fn requeues_tasks_after_worker_timeout_and_accepts_recovery() {
    let mut coordinator = coordinator(100, 5, 3);
    coordinator.submit_stage(stage(0, Vec::new(), 1)).unwrap();
    register(&mut coordinator, "worker-a", 1, 0);
    let (_, first, _) = assignment(&mut coordinator, 0);

    coordinator.advance_time(6).unwrap();
    assert_eq!(
        coordinator.worker_available_slots(&WorkerId::new("worker-a").unwrap()),
        None
    );
    assert_eq!(
        coordinator
            .partition_status(&query_id(), StageId(0), PartitionId(0))
            .unwrap(),
        PartitionStatus::Pending { next_attempt: 1 }
    );

    coordinator
        .handle_worker_message(
            WorkerMessage::Heartbeat {
                version: PROTOCOL_VERSION,
                heartbeat: WorkerHeartbeat {
                    worker_id: WorkerId::new("worker-a").unwrap(),
                    observed_at_ms: 7,
                    available_slots: 1,
                    available_memory_bytes: 1_024,
                },
            },
            7,
        )
        .unwrap();
    let (_, retried, _) = assignment(&mut coordinator, 7);
    assert_eq!(retried.attempt, first.attempt + 1);
}

#[test]
fn records_successful_blocks_and_cancels_active_queries() {
    let mut completed = coordinator(100, 50, 3);
    completed.submit_stage(stage(0, Vec::new(), 1)).unwrap();
    register(&mut completed, "worker-a", 1, 0);
    let (_, task, worker) = assignment(&mut completed, 1);
    let block = ShuffleBlock {
        producer: task.clone(),
        output_partition: PartitionId(0),
        rows: 10,
        bytes: 80,
        checksum: "crc32c:42".to_owned(),
        location: ShuffleLocation::Worker {
            worker_id: WorkerId::new(&worker).unwrap(),
        },
    };
    succeed(&mut completed, &worker, task, 2, vec![block.clone()]);
    assert_eq!(
        completed
            .stage_output_blocks(&query_id(), StageId(0))
            .unwrap(),
        vec![block]
    );

    let mut cancelled = coordinator(100, 50, 3);
    cancelled.submit_stage(stage(0, Vec::new(), 2)).unwrap();
    register(&mut cancelled, "worker-a", 1, 0);
    assignment(&mut cancelled, 1);
    let cancel = cancelled
        .cancel_query(query_id(), "user requested cancellation")
        .unwrap();
    cancel.validate().unwrap();
    assert_eq!(
        cancelled.stage_status(&query_id(), StageId(0)).unwrap(),
        StageStatus::Cancelled
    );
    assert_eq!(
        cancelled
            .partition_status(&query_id(), StageId(0), PartitionId(0))
            .unwrap(),
        PartitionStatus::Cancelled
    );
    assert_eq!(
        cancelled.worker_available_slots(&WorkerId::new("worker-a").unwrap()),
        Some(1)
    );
    assert!(cancelled.next_assignment(2).unwrap().is_none());
    cancelled
        .cancel_query(query_id(), "user requested cancellation")
        .unwrap();
    assert!(matches!(
        cancelled.cancel_query(query_id(), "different reason"),
        Err(SparkXError::Protocol(_))
    ));
}
