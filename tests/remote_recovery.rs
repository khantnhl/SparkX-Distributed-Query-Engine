use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::CancellationToken;
use sparkx::catalog::{Catalog, MemoryTable};
use sparkx::cluster::join::plan_remote_join;
use sparkx::control_plane::ControlPlaneServer;
use sparkx::coordinator::{Coordinator, CoordinatorConfig, StageStatus};
use sparkx::data_plane::{FlightDataPlaneClient, FlightDataPlaneServer};
use sparkx::execution::PhysicalPlan;
use sparkx::expr::Expr;
use sparkx::logical::JoinType;
use sparkx::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, PartitionId, QueryId, StageId, TaskState,
    WorkerHeartbeat, WorkerId, WorkerMessage, WorkerRegistration,
};
use sparkx::remote::{RemoteStageConfig, RemoteStageRunner};
use sparkx::worker::{RemoteWorker, WorkerConfig};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn worker_loss(retries: u32, consumer: bool) {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig {
            max_task_attempts: if consumer { 2 } else { 1 },
            // Leave room for slower CI hosts and platform-specific connection refusal delays.
            heartbeat_timeout_ms: 2_000,
            lease_duration_ms: 10_000,
            ..CoordinatorConfig::default()
        })
        .unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let catalog = Arc::new(Catalog::default());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let provider = Arc::new(MemoryTable::from_batches(vec![batch.clone()], 1).unwrap());
    catalog.register("a", provider.clone());
    catalog.register("b", provider.clone());
    let scan = |name: &str, id| {
        Arc::new(PhysicalPlan::Scan {
            id,
            table_name: name.into(),
            provider: provider.clone(),
            projection: None,
            filters: vec![],
            schema: schema.clone(),
        })
    };
    let plan = PhysicalPlan::HashJoin {
        id: 0,
        left: scan("a", 1),
        right: scan("b", 2),
        join_type: JoinType::Inner,
        left_on: vec![Expr::column("id")],
        right_on: vec![Expr::column("id")],
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("right.id", DataType::Int64, false),
        ])),
    };
    let query = QueryId::new(format!("lost-producer-{retries}")).unwrap();
    let graph = plan_remote_join(&plan, query.clone()).unwrap().unwrap();
    let lost_worker = WorkerId::new("lost-producer").unwrap();
    coordinator
        .lock()
        .await
        .handle_worker_message(
            WorkerMessage::Register {
                version: PROTOCOL_VERSION,
                registration: WorkerRegistration {
                    worker_id: lost_worker.clone(),
                    slots: 1,
                    memory_bytes: 1_000_000,
                },
            },
            now(),
        )
        .unwrap();
    let mut config = RemoteStageConfig::new(server.endpoint());
    config.poll_interval = Duration::from_millis(5);
    config.timeout = Duration::from_secs(30);
    config.max_query_retries = retries;
    let runner = RemoteStageRunner::new(config).unwrap();
    let handle = tokio::spawn(async move {
        runner
            .execute_graph(graph, StageId(2), CancellationToken::new())
            .await
    });
    let assignment = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(assignment) = coordinator
                .lock()
                .await
                .next_assignment_for(&lost_worker, now())
                .unwrap()
            {
                break assignment;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let CoordinatorMessage::AssignTask { task, .. } = assignment else {
        panic!("expected producer")
    };
    assert_eq!(task.stage_id, StageId(0));
    let data = FlightDataPlaneServer::start_loopback(1_000_000)
        .await
        .unwrap();
    let mut client = FlightDataPlaneClient::connect(data.endpoint())
        .await
        .unwrap();
    let block = client
        .upload(
            lost_worker.clone(),
            task.clone(),
            PartitionId(0),
            schema.clone(),
            vec![batch.clone()],
        )
        .await
        .unwrap();
    coordinator
        .lock()
        .await
        .handle_worker_message(
            WorkerMessage::TaskUpdate {
                version: PROTOCOL_VERSION,
                worker_id: lost_worker.clone(),
                task,
                state: TaskState::Succeeded {
                    finished_at_ms: now(),
                    output_blocks: vec![block],
                },
            },
            now(),
        )
        .unwrap();
    if consumer {
        let assignment = coordinator
            .lock()
            .await
            .next_assignment_for(&lost_worker, now())
            .unwrap()
            .unwrap();
        let CoordinatorMessage::AssignTask { task, .. } = assignment else {
            panic!("expected right producer")
        };
        assert_eq!(task.stage_id, StageId(1));
        let block = client
            .upload(
                lost_worker.clone(),
                task.clone(),
                PartitionId(0),
                schema.clone(),
                vec![batch.clone()],
            )
            .await
            .unwrap();
        coordinator
            .lock()
            .await
            .handle_worker_message(
                WorkerMessage::TaskUpdate {
                    version: PROTOCOL_VERSION,
                    worker_id: lost_worker.clone(),
                    task,
                    state: TaskState::Succeeded {
                        finished_at_ms: now(),
                        output_blocks: vec![block],
                    },
                },
                now(),
            )
            .unwrap();
        let assignment = coordinator
            .lock()
            .await
            .next_assignment_for(&lost_worker, now())
            .unwrap()
            .unwrap();
        let CoordinatorMessage::AssignTask { task, .. } = assignment else {
            panic!("expected join consumer")
        };
        assert_eq!(task.stage_id, StageId(2));
        // Abandon this leased consumer task without a terminal update or further heartbeats.
    }
    coordinator
        .lock()
        .await
        .handle_worker_message(
            WorkerMessage::Heartbeat {
                version: PROTOCOL_VERSION,
                heartbeat: WorkerHeartbeat {
                    worker_id: lost_worker,
                    observed_at_ms: now(),
                    available_slots: 0,
                    available_memory_bytes: 0,
                },
            },
            now(),
        )
        .unwrap();
    // The producer disappears after publication. Its committed manifest now points to a dead service.
    drop(client);
    let data = if consumer {
        Some(data)
    } else {
        data.close().await.unwrap();
        None
    };
    let stop = CancellationToken::new();
    let mut worker = WorkerConfig::new(server.endpoint(), WorkerId::new("survivor").unwrap());
    worker.poll_interval = Duration::from_millis(5);
    worker.heartbeat_interval = Duration::from_millis(25);
    let survivor = tokio::spawn(
        RemoteWorker::new(worker, catalog)
            .unwrap()
            .run_until(stop.clone()),
    );
    let result = tokio::time::timeout(Duration::from_secs(35), handle)
        .await
        .unwrap()
        .unwrap();
    if retries > 0 {
        let result = result.unwrap();
        assert_eq!(result.row_count(), 3);
        assert_eq!(result.recovery_attempts, if consumer { 0 } else { 1 });
        assert!(
            result
                .output_blocks
                .iter()
                .all(|block| (block.producer.query_id == query) == consumer)
        );
        let mut pairs = result
            .batches
            .iter()
            .flat_map(|batch| {
                let left = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let right = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| (left.value(row), right.value(row)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        pairs.sort();
        assert_eq!(pairs, vec![(1, 1), (2, 2), (3, 3)]);
    } else {
        assert!(result.is_err());
    }
    assert_eq!(
        coordinator
            .lock()
            .await
            .stage_status(&query, StageId(0))
            .unwrap(),
        if consumer {
            StageStatus::Succeeded
        } else {
            StageStatus::Cancelled
        }
    );
    stop.cancel();
    survivor.await.unwrap().unwrap();
    server.close().await.unwrap();
    if let Some(data) = data {
        data.close().await.unwrap();
    }
}

#[tokio::test]
async fn recomputes_join_after_losing_a_committed_producer() {
    worker_loss(1, false).await;
}
#[tokio::test]
async fn producer_loss_without_retry_budget_fails_cleanly() {
    worker_loss(0, false).await;
}

#[tokio::test]
async fn retries_lost_consumer_using_committed_producer_blocks() {
    worker_loss(1, true).await;
}
