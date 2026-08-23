use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::catalog::{Catalog, MemoryTable, TableRef};
use sparkx::control_plane::ControlPlaneServer;
use sparkx::coordinator::{Coordinator, CoordinatorConfig, StageStatus};
use sparkx::data_plane::FlightDataPlaneClient;
use sparkx::execution::PhysicalPlan;
use sparkx::protocol::{QueryId, ShuffleLocation, StageId, StagePlan, WorkerId};
use sparkx::remote::{RemoteStageConfig, RemoteStageRunner};
use sparkx::worker::{RemoteWorker, WorkerConfig};
use sparkx::{CancellationToken, Session, SessionConfig, SparkXError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

fn batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef]).unwrap()
}

fn stage(query: &str, provider: TableRef, partitions: u32) -> StagePlan {
    let plan = PhysicalPlan::Scan {
        id: 0,
        table_name: "input".to_owned(),
        schema: provider.schema(),
        provider,
        projection: None,
        filters: Vec::new(),
    };
    StagePlan::from_physical_plan(
        QueryId::new(query).unwrap(),
        StageId(0),
        Vec::new(),
        partitions,
        &plan,
    )
    .unwrap()
}

fn worker_config(endpoint: String, id: &str, max_tasks: Option<u64>) -> WorkerConfig {
    let mut config = WorkerConfig::new(endpoint, WorkerId::new(id).unwrap());
    config.slots = 1;
    config.memory_bytes = 16 * 1024 * 1024;
    config.data_storage_bytes = 16 * 1024 * 1024;
    config.heartbeat_interval = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_terminal_tasks = max_tasks;
    config
}

fn runner(endpoint: String) -> RemoteStageRunner {
    let mut config = RemoteStageConfig::new(endpoint);
    config.poll_interval = Duration::from_millis(5);
    config.timeout = Duration::from_secs(5);
    RemoteStageRunner::new(config).unwrap()
}

#[tokio::test]
async fn remote_stage_runner_fetches_verifies_and_cleans_worker_output() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator)
        .await
        .unwrap();
    let table = Arc::new(
        MemoryTable::new(
            batch(vec![1]).schema(),
            vec![vec![batch(vec![1, 2])], vec![batch(vec![3, 4])]],
        )
        .unwrap(),
    );
    let catalog = Arc::new(Catalog::default());
    catalog.register("input", table.clone());
    let shutdown = CancellationToken::new();
    let worker = RemoteWorker::new(
        worker_config(server.endpoint(), "worker-stage-success", None),
        catalog,
    )
    .unwrap();
    let worker_handle = tokio::spawn(worker.run_until(shutdown.clone()));

    let result = runner(server.endpoint())
        .execute(
            stage("query-stage-success", table, 2),
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(result.row_count(), 4);
    assert!(result.schema.is_some());
    assert_eq!(result.output_blocks.len(), 2);
    assert!(result.cleanup_errors.is_empty());
    let mut values = result
        .batches
        .iter()
        .flat_map(|output| {
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3, 4]);

    let first = &result.output_blocks[0];
    let endpoint = match &first.location {
        ShuffleLocation::Flight { endpoint, .. } => endpoint,
        other => panic!("expected Flight output, got {other:?}"),
    };
    let mut data_client = FlightDataPlaneClient::connect(endpoint).await.unwrap();
    assert!(matches!(
        data_client.download(first).await.unwrap_err(),
        SparkXError::NotFound(_)
    ));

    shutdown.cancel();
    worker_handle.await.unwrap().unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn remote_stage_runner_reports_worker_partition_failures() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator)
        .await
        .unwrap();
    let table = Arc::new(MemoryTable::from_batches(vec![batch(vec![1])], 1).unwrap());
    let worker = RemoteWorker::new(
        worker_config(server.endpoint(), "worker-stage-failure", Some(1)),
        Arc::new(Catalog::default()),
    )
    .unwrap();
    let worker_handle = tokio::spawn(worker.run_until(CancellationToken::new()));

    let error = runner(server.endpoint())
        .execute(
            stage("query-stage-failure", table, 1),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    let SparkXError::Execution(message) = error else {
        panic!("expected execution failure, got {error}");
    };
    assert!(message.contains("partition 0 attempt 0"));
    assert!(message.contains("input"));
    worker_handle.await.unwrap().unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn remote_stage_runner_propagates_client_cancellation() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let table = Arc::new(MemoryTable::from_batches(vec![batch(vec![1])], 1).unwrap());
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        signal.cancel();
    });
    let query_id = QueryId::new("query-stage-cancel").unwrap();

    let error = runner(server.endpoint())
        .execute(stage(query_id.as_str(), table, 1), cancellation)
        .await
        .unwrap_err();

    assert!(matches!(error, SparkXError::Cancelled));
    assert_eq!(
        coordinator
            .lock()
            .await
            .stage_status(&query_id, StageId(0))
            .unwrap(),
        StageStatus::Cancelled
    );
    server.close().await.unwrap();
}

#[tokio::test]
async fn remote_stage_runner_cancels_work_after_timeout() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let table = Arc::new(MemoryTable::from_batches(vec![batch(vec![1])], 1).unwrap());
    let query_id = QueryId::new("query-stage-timeout").unwrap();
    let mut config = RemoteStageConfig::new(server.endpoint());
    config.poll_interval = Duration::from_millis(5);
    config.timeout = Duration::from_millis(20);

    let error = RemoteStageRunner::new(config)
        .unwrap()
        .execute(stage(query_id.as_str(), table, 1), CancellationToken::new())
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        SparkXError::Execution(message) if message.contains("timeout")
    ));
    assert_eq!(
        coordinator
            .lock()
            .await
            .stage_status(&query_id, StageId(0))
            .unwrap(),
        StageStatus::Cancelled
    );
    server.close().await.unwrap();
}

#[tokio::test]
async fn session_executes_partition_local_sql_on_remote_workers() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator)
        .await
        .unwrap();
    let table = Arc::new(
        MemoryTable::new(
            batch(vec![1]).schema(),
            vec![vec![batch(vec![1, 2])], vec![batch(vec![3, 4])]],
        )
        .unwrap(),
    );
    let worker_catalog = Arc::new(Catalog::default());
    worker_catalog.register("input", table.clone());
    let shutdown = CancellationToken::new();
    let worker = RemoteWorker::new(
        worker_config(server.endpoint(), "worker-session-sql", None),
        worker_catalog,
    )
    .unwrap();
    let worker_handle = tokio::spawn(worker.run_until(shutdown.clone()));
    let session = Session::new(SessionConfig::default());
    session.register_table("input", table);
    let mut remote = RemoteStageConfig::new(server.endpoint());
    remote.poll_interval = Duration::from_millis(5);
    remote.timeout = Duration::from_secs(5);

    let result = session
        .execute_sql_remote(
            "SELECT value + 10 AS shifted FROM input WHERE value > 1",
            QueryId::new("query-session-sql").unwrap(),
            remote,
        )
        .await
        .unwrap();

    let mut values = result
        .batches
        .iter()
        .flat_map(|output| {
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![12, 13, 14]);
    assert!(result.distributed);
    assert_eq!(result.stages, 1);
    assert_eq!(result.metrics.tasks, 2);
    assert_eq!(result.metrics.output_rows, 3);
    assert!(result.cleanup_errors.is_empty());

    shutdown.cancel();
    worker_handle.await.unwrap().unwrap();
    server.close().await.unwrap();
}

#[tokio::test]
async fn session_rejects_global_remote_sql_before_submission() {
    let session = Session::new(SessionConfig::default());
    session.register_memory(
        "input",
        MemoryTable::from_batches(vec![batch(vec![1, 2])], 1).unwrap(),
    );

    let error = session
        .execute_sql_remote(
            "SELECT COUNT(*) FROM input",
            QueryId::new("query-global-rejected").unwrap(),
            RemoteStageConfig::new("http://127.0.0.1:9"),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        SparkXError::Unsupported(message) if message.contains("multi-stage merge planning")
    ));
}
