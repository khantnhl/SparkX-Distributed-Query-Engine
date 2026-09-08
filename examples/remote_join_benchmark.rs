//! Reproducible loopback benchmark; measures remote services in one process, not multi-host speedup.
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::catalog::{Catalog, MemoryTable};
use sparkx::control_plane::ControlPlaneServer;
use sparkx::coordinator::{Coordinator, CoordinatorConfig};
use sparkx::protocol::{QueryId, WorkerId};
use sparkx::remote::RemoteStageConfig;
use sparkx::worker::{RemoteWorker, WorkerConfig};
use sparkx::{CancellationToken, Result, Session, SessionConfig};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<()> {
    let row_count = 2_000;
    let partition_count = 4;
    for skew in [false, true] {
        let catalog = Arc::new(Catalog::default());
        let session = Session::new(SessionConfig::default());
        for name in ["a", "b"] {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let mut partitions = Vec::new();
            for partition in 0..partition_count {
                let values = (0..row_count)
                    .filter(|row| row % partition_count == partition)
                    .map(|row| {
                        if skew && name == "a" && row < row_count * 9 / 10 {
                            0
                        } else {
                            row as i64
                        }
                    })
                    .collect::<Vec<_>>();
                partitions.push(vec![RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from(values))],
                )?]);
            }
            let provider = Arc::new(MemoryTable::new(schema, partitions)?);
            catalog.register(name, provider.clone());
            session.register_table(name, provider);
        }
        let coordinator = Arc::new(Mutex::new(Coordinator::new(CoordinatorConfig::default())?));
        let server = ControlPlaneServer::start_loopback(coordinator).await?;
        let stop = CancellationToken::new();
        let mut workers = Vec::new();
        for id in ["benchmark-a", "benchmark-b"] {
            let mut config = WorkerConfig::new(server.endpoint(), WorkerId::new(id)?);
            config.poll_interval = Duration::from_millis(5);
            workers.push(tokio::spawn(
                RemoteWorker::new(config, catalog.clone())?.run_until(stop.clone()),
            ));
        }
        let sql = "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id";
        for iteration in 0..3 {
            for remote in [false, true] {
                let started = Instant::now();
                let result = if remote {
                    let mut config = RemoteStageConfig::new(server.endpoint());
                    config.poll_interval = Duration::from_millis(5);
                    session
                        .execute_sql_remote(
                            sql,
                            QueryId::new(format!("benchmark-{iteration}"))?,
                            config,
                        )
                        .await?
                } else {
                    session.execute_sql(sql).await?
                };
                assert_eq!(result.row_count(), row_count);
                println!(
                    "{}",
                    serde_json::json!({
                        "mode": if remote { "remote-loopback" } else { "native" }, "skew": skew,
                        "iteration": iteration, "input_rows_per_table": row_count, "partitions": partition_count,
                        "workers": 2, "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
                        "metrics": result.metrics,
                    })
                );
            }
        }
        stop.cancel();
        for worker in workers {
            worker.await.expect("worker join")?;
        }
        server.close().await?;
    }
    Ok(())
}
