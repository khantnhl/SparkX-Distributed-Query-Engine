use arrow::datatypes::{DataType, Field, Schema};
use sparkx::SparkXError;
use sparkx::catalog::MemoryTable;
use sparkx::control_plane::{ControlPlaneClient, ControlPlaneServer};
use sparkx::coordinator::{Coordinator, CoordinatorConfig, StageStatus};
use sparkx::execution::PhysicalPlan;
use sparkx::protocol::{
    CoordinatorMessage, PROTOCOL_VERSION, QueryId, StageId, StagePlan, TaskState, WorkerHeartbeat,
    WorkerId, WorkerMessage, WorkerRegistration,
};
use std::sync::Arc;
use tokio::sync::Mutex;

fn stage(query: &str, stage_id: u32, partitions: u32) -> StagePlan {
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
        QueryId::new(query).unwrap(),
        StageId(stage_id),
        Vec::new(),
        partitions,
        &plan,
    )
    .unwrap()
}

#[tokio::test]
async fn flight_control_plane_runs_worker_and_query_lifecycle() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let mut client = ControlPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    let query_id = QueryId::new("query-flight-control").unwrap();
    let worker_id = WorkerId::new("worker-flight-a").unwrap();

    client
        .submit_stage(&stage(query_id.as_str(), 0, 1))
        .await
        .unwrap();
    client
        .register(WorkerRegistration {
            worker_id: worker_id.clone(),
            slots: 1,
            memory_bytes: 1024,
        })
        .await
        .unwrap();
    client
        .heartbeat(WorkerHeartbeat {
            worker_id: worker_id.clone(),
            observed_at_ms: 1,
            available_slots: 1,
            available_memory_bytes: 900,
        })
        .await
        .unwrap();

    let assignment = client
        .poll_assignment(worker_id.clone())
        .await
        .unwrap()
        .unwrap();
    let CoordinatorMessage::AssignTask {
        task,
        lease,
        stage: assigned_stage,
        ..
    } = assignment
    else {
        panic!("expected task assignment");
    };
    assert_eq!(lease.worker_id, worker_id);
    assert_eq!(assigned_stage.query_id, query_id);
    assert!(
        client
            .poll_assignment(worker_id.clone())
            .await
            .unwrap()
            .is_none()
    );

    client
        .send_worker_message(WorkerMessage::TaskUpdate {
            version: PROTOCOL_VERSION,
            worker_id: worker_id.clone(),
            task: task.clone(),
            state: TaskState::Running {
                started_at_ms: lease.issued_at_ms,
            },
        })
        .await
        .unwrap();
    client
        .send_worker_message(WorkerMessage::TaskUpdate {
            version: PROTOCOL_VERSION,
            worker_id: worker_id.clone(),
            task,
            state: TaskState::Succeeded {
                finished_at_ms: lease.issued_at_ms,
                output_blocks: Vec::new(),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .lock()
            .await
            .stage_status(&query_id, StageId(0))
            .unwrap(),
        StageStatus::Succeeded
    );

    let duplicate = client
        .register(WorkerRegistration {
            worker_id: worker_id.clone(),
            slots: 1,
            memory_bytes: 1024,
        })
        .await;
    assert!(matches!(duplicate, Err(SparkXError::Protocol(_))));
    let unknown_poll = client
        .poll_assignment(WorkerId::new("worker-unknown").unwrap())
        .await;
    assert!(matches!(unknown_poll, Err(SparkXError::Protocol(_))));

    let cancelled_query = QueryId::new("query-flight-cancel").unwrap();
    client
        .submit_stage(&stage(cancelled_query.as_str(), 0, 1))
        .await
        .unwrap();
    let cancelled_assignment = client
        .poll_assignment(worker_id.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        cancelled_assignment,
        CoordinatorMessage::AssignTask { ref task, .. }
            if task.query_id == cancelled_query
    ));
    let cancellation = client
        .cancel_query(cancelled_query.clone(), "test cancellation")
        .await
        .unwrap();
    assert!(matches!(
        cancellation,
        CoordinatorMessage::CancelQuery {
            ref query_id,
            ref reason,
            ..
        } if query_id == &cancelled_query && reason == "test cancellation"
    ));
    assert_eq!(
        coordinator
            .lock()
            .await
            .stage_status(&cancelled_query, StageId(0))
            .unwrap(),
        StageStatus::Cancelled
    );
    let worker_cancellation = client.poll_assignment(worker_id).await.unwrap().unwrap();
    assert_eq!(worker_cancellation, cancellation);

    server.close().await.unwrap();
}
