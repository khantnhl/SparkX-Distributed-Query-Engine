use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use sparkx::CancellationToken;
use sparkx::catalog::{Catalog, MemoryTable, TableProvider, TableRef};
use sparkx::control_plane::{ControlPlaneClient, ControlPlaneServer};
use sparkx::coordinator::{Coordinator, CoordinatorConfig, PartitionStatus, StageStatus};
use sparkx::execution::PhysicalPlan;
use sparkx::protocol::{PartitionId, QueryId, StageId, StagePlan, WorkerId};
use sparkx::worker::{RemoteWorker, WorkerConfig};
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

fn stage(query_id: QueryId, provider: TableRef, partitions: u32) -> StagePlan {
    let plan = PhysicalPlan::Scan {
        id: 0,
        table_name: "input".to_owned(),
        schema: provider.schema(),
        provider,
        projection: None,
        filters: Vec::new(),
    };
    StagePlan::from_physical_plan(query_id, StageId(0), Vec::new(), partitions, &plan).unwrap()
}

fn worker_config(endpoint: String, worker_id: WorkerId, terminal_tasks: u64) -> WorkerConfig {
    let mut config = WorkerConfig::new(endpoint, worker_id);
    config.slots = 1;
    config.memory_bytes = 16 * 1024 * 1024;
    config.batch_size = 1_024;
    config.channel_capacity = 2;
    config.heartbeat_interval = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(5);
    config.max_terminal_tasks = Some(terminal_tasks);
    config
}

#[tokio::test]
async fn remote_worker_executes_leased_partitions_over_flight_control() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig::default()).unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let query_id = QueryId::new("query-remote-worker").unwrap();
    let table = Arc::new(
        MemoryTable::new(
            batch(vec![1]).schema(),
            vec![vec![batch(vec![1, 2])], vec![batch(vec![3, 4])]],
        )
        .unwrap(),
    );
    let catalog = Arc::new(Catalog::default());
    catalog.register("input", table.clone());
    let mut admin = ControlPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    admin
        .submit_stage(&stage(query_id.clone(), table, 2))
        .await
        .unwrap();

    let worker_id = WorkerId::new("worker-remote-a").unwrap();
    let worker = RemoteWorker::new(
        worker_config(server.endpoint(), worker_id.clone(), 2),
        catalog,
    )
    .unwrap();
    let summary = tokio::time::timeout(
        Duration::from_secs(5),
        worker.run_until(CancellationToken::new()),
    )
    .await
    .expect("worker should finish two partitions")
    .unwrap();

    assert_eq!(summary.completed_tasks, 2);
    assert_eq!(summary.failed_tasks, 0);
    assert_eq!(summary.cancelled_tasks, 0);
    assert_eq!(summary.output_rows, 4);
    assert_eq!(summary.metrics.tasks, 2);
    let coordinator = coordinator.lock().await;
    assert_eq!(
        coordinator.stage_status(&query_id, StageId(0)).unwrap(),
        StageStatus::Succeeded
    );
    assert_eq!(coordinator.worker_available_slots(&worker_id), Some(1));
    drop(coordinator);
    server.close().await.unwrap();
}

#[derive(Debug)]
struct SlowTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

impl TableProvider for SlowTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn estimated_bytes(&self) -> u64 {
        self.batch.get_array_memory_size() as u64
    }

    fn scan_partition(
        &self,
        partition: usize,
        _projection: Option<&[usize]>,
        _batch_size: usize,
    ) -> sparkx::Result<Vec<RecordBatch>> {
        assert_eq!(partition, 0);
        std::thread::sleep(Duration::from_millis(150));
        Ok(vec![self.batch.clone()])
    }
}

#[tokio::test]
async fn remote_worker_acknowledges_cancellation_before_releasing_its_slot() {
    let coordinator = Arc::new(Mutex::new(
        Coordinator::new(CoordinatorConfig {
            lease_duration_ms: 2_000,
            heartbeat_timeout_ms: 1_000,
            max_task_attempts: 2,
            max_stage_partitions: 10,
        })
        .unwrap(),
    ));
    let server = ControlPlaneServer::start_loopback(coordinator.clone())
        .await
        .unwrap();
    let query_id = QueryId::new("query-worker-cancel").unwrap();
    let source = batch(vec![1, 2]);
    let table = Arc::new(SlowTable {
        schema: source.schema(),
        batch: source,
    });
    let catalog = Arc::new(Catalog::default());
    catalog.register("input", table.clone());
    let mut admin = ControlPlaneClient::connect(server.endpoint())
        .await
        .unwrap();
    admin
        .submit_stage(&stage(query_id.clone(), table, 1))
        .await
        .unwrap();

    let worker_id = WorkerId::new("worker-cancel-a").unwrap();
    let worker = RemoteWorker::new(
        worker_config(server.endpoint(), worker_id.clone(), 1),
        catalog,
    )
    .unwrap();
    let handle = tokio::spawn(worker.run_until(CancellationToken::new()));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if coordinator
                .lock()
                .await
                .partition_status(&query_id, StageId(0), PartitionId(0))
                .is_ok_and(|status| matches!(status, PartitionStatus::Running { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("worker should start the leased task");

    admin
        .cancel_query(query_id.clone(), "cancel integration test")
        .await
        .unwrap();
    assert_eq!(
        coordinator.lock().await.worker_available_slots(&worker_id),
        Some(0)
    );
    let summary = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("worker should acknowledge cancellation")
        .unwrap()
        .unwrap();
    assert_eq!(summary.cancelled_tasks, 1);
    assert_eq!(summary.completed_tasks, 0);
    let coordinator = coordinator.lock().await;
    assert_eq!(
        coordinator
            .partition_status(&query_id, StageId(0), PartitionId(0))
            .unwrap(),
        PartitionStatus::Cancelled
    );
    assert_eq!(coordinator.worker_available_slots(&worker_id), Some(1));
    drop(coordinator);
    server.close().await.unwrap();
}
