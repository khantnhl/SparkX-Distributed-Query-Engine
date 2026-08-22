use sparkx::SparkXError;
use sparkx::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, PartitionId, QueryId, ShuffleBlock, ShuffleLocation,
    StageId, StagePlan, TaskAttemptId, TaskLease, TaskState, WorkerHeartbeat, WorkerId,
    WorkerMessage, WorkerRegistration,
};

fn stage() -> StagePlan {
    StagePlan {
        query_id: QueryId::new("query-42").unwrap(),
        stage_id: StageId(2),
        input_stages: vec![StageId(1)],
        partition_count: 4,
        plan_fragment: vec![1, 2, 3],
    }
}

fn task() -> TaskAttemptId {
    TaskAttemptId {
        query_id: QueryId::new("query-42").unwrap(),
        stage_id: StageId(2),
        partition_id: PartitionId(3),
        attempt: 1,
    }
}

#[test]
fn coordinator_assignment_round_trips_and_validates() {
    let message = CoordinatorMessage::AssignTask {
        version: PROTOCOL_VERSION,
        stage: stage(),
        task: task(),
        lease: TaskLease {
            worker_id: WorkerId::new("worker-a").unwrap(),
            issued_at_ms: 1_000,
            expires_at_ms: 31_000,
        },
    };
    message.validate().unwrap();

    let json = serde_json::to_string(&message).unwrap();
    assert!(json.contains("\"type\":\"assign_task\""));
    let decoded: CoordinatorMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, message);
    decoded.validate().unwrap();
}

#[test]
fn rejects_inconsistent_stage_task_and_lease_contracts() {
    let mut invalid_stage = stage();
    invalid_stage.partition_count = 0;
    assert!(matches!(
        invalid_stage.validate(),
        Err(SparkXError::Protocol(_))
    ));

    let mut recursive_stage = stage();
    recursive_stage.input_stages.push(recursive_stage.stage_id);
    assert!(matches!(
        recursive_stage.validate(),
        Err(SparkXError::Protocol(_))
    ));

    let invalid_partition = CoordinatorMessage::AssignTask {
        version: PROTOCOL_VERSION,
        stage: stage(),
        task: TaskAttemptId {
            partition_id: PartitionId(4),
            ..task()
        },
        lease: TaskLease {
            worker_id: WorkerId::new("worker-a").unwrap(),
            issued_at_ms: 1_000,
            expires_at_ms: 31_000,
        },
    };
    assert!(matches!(
        invalid_partition.validate(),
        Err(SparkXError::Protocol(_))
    ));

    let expired_lease = CoordinatorMessage::AssignTask {
        version: PROTOCOL_VERSION,
        stage: stage(),
        task: task(),
        lease: TaskLease {
            worker_id: WorkerId::new("worker-a").unwrap(),
            issued_at_ms: 1_000,
            expires_at_ms: 1_000,
        },
    };
    assert!(matches!(
        expired_lease.validate(),
        Err(SparkXError::Protocol(_))
    ));

    let wrong_version = CoordinatorMessage::CancelQuery {
        version: PROTOCOL_VERSION + 1,
        query_id: QueryId::new("query-42").unwrap(),
        reason: "deadline exceeded".to_owned(),
    };
    assert!(matches!(
        wrong_version.validate(),
        Err(SparkXError::Protocol(_))
    ));
}

#[test]
fn worker_lifecycle_messages_round_trip_and_validate() {
    let messages = vec![
        WorkerMessage::Register {
            version: PROTOCOL_VERSION,
            registration: WorkerRegistration {
                worker_id: WorkerId::new("worker-a").unwrap(),
                slots: 8,
                memory_bytes: 16 * 1024 * 1024 * 1024,
            },
        },
        WorkerMessage::Heartbeat {
            version: PROTOCOL_VERSION,
            heartbeat: WorkerHeartbeat {
                worker_id: WorkerId::new("worker-a").unwrap(),
                observed_at_ms: 2_000,
                available_slots: 6,
                available_memory_bytes: 12 * 1024 * 1024 * 1024,
            },
        },
        WorkerMessage::TaskUpdate {
            version: PROTOCOL_VERSION,
            worker_id: WorkerId::new("worker-a").unwrap(),
            task: task(),
            state: TaskState::Succeeded {
                finished_at_ms: 4_000,
                output_blocks: vec![ShuffleBlock {
                    producer: task(),
                    output_partition: PartitionId(0),
                    rows: 10_000,
                    bytes: 640_000,
                    checksum: "crc32c:42".to_owned(),
                    location: ShuffleLocation::Worker {
                        worker_id: WorkerId::new("worker-a").unwrap(),
                    },
                }],
            },
        },
    ];

    for message in messages {
        message.validate().unwrap();
        let json = serde_json::to_string(&message).unwrap();
        let decoded: WorkerMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, message);
        decoded.validate().unwrap();
    }
}

#[test]
fn rejects_output_blocks_owned_by_another_attempt() {
    let message = WorkerMessage::TaskUpdate {
        version: PROTOCOL_VERSION,
        worker_id: WorkerId::new("worker-a").unwrap(),
        task: task(),
        state: TaskState::Succeeded {
            finished_at_ms: 4_000,
            output_blocks: vec![ShuffleBlock {
                producer: TaskAttemptId {
                    attempt: 2,
                    ..task()
                },
                output_partition: PartitionId(0),
                rows: 1,
                bytes: 8,
                checksum: "crc32c:99".to_owned(),
                location: ShuffleLocation::ObjectStore {
                    uri: "s3://shuffle/query-42/stage-2/block-0".to_owned(),
                },
            }],
        },
    };

    assert!(matches!(message.validate(), Err(SparkXError::Protocol(_))));
}

#[test]
fn rejects_in_memory_blocks_owned_by_another_worker() {
    let message = WorkerMessage::TaskUpdate {
        version: PROTOCOL_VERSION,
        worker_id: WorkerId::new("worker-a").unwrap(),
        task: task(),
        state: TaskState::Succeeded {
            finished_at_ms: 4_000,
            output_blocks: vec![ShuffleBlock {
                producer: task(),
                output_partition: PartitionId(0),
                rows: 1,
                bytes: 8,
                checksum: "crc32c:11".to_owned(),
                location: ShuffleLocation::Worker {
                    worker_id: WorkerId::new("worker-b").unwrap(),
                },
            }],
        },
    };

    assert!(matches!(message.validate(), Err(SparkXError::Protocol(_))));
}

#[test]
fn rejects_workers_without_executable_resources() {
    let message = WorkerMessage::Register {
        version: PROTOCOL_VERSION,
        registration: WorkerRegistration {
            worker_id: WorkerId::new("worker-empty").unwrap(),
            slots: 0,
            memory_bytes: 0,
        },
    };

    assert!(matches!(message.validate(), Err(SparkXError::Protocol(_))));
}
