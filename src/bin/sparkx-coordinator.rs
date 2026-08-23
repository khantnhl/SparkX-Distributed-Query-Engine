use clap::Parser;
use sparkx::control_plane::ControlPlaneServer;
use sparkx::coordinator::{Coordinator, CoordinatorConfig};
use sparkx::{Result, SparkXError};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Parser)]
#[command(
    name = "sparkx-coordinator",
    version,
    about = "SparkX coordinator Flight/gRPC service"
)]
struct Args {
    /// Interface and port for the Flight/gRPC control service.
    #[arg(long, default_value = "127.0.0.1:50051")]
    bind: SocketAddr,

    /// Milliseconds before an unacknowledged task lease expires.
    #[arg(long, default_value_t = 30_000)]
    lease_ms: u64,

    /// Milliseconds without a received heartbeat before a worker is unavailable.
    #[arg(long, default_value_t = 15_000)]
    heartbeat_timeout_ms: u64,

    /// Maximum attempts allowed for one stage partition.
    #[arg(long, default_value_t = 3)]
    max_task_attempts: u32,

    /// Maximum partitions accepted in one submitted stage.
    #[arg(long, default_value_t = 100_000)]
    max_stage_partitions: u32,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sparkx-coordinator: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let coordinator = Coordinator::new(CoordinatorConfig {
        lease_duration_ms: args.lease_ms,
        heartbeat_timeout_ms: args.heartbeat_timeout_ms,
        max_task_attempts: args.max_task_attempts,
        max_stage_partitions: args.max_stage_partitions,
    })?;
    let server = ControlPlaneServer::bind(args.bind, Arc::new(Mutex::new(coordinator))).await?;
    println!("SparkX coordinator listening on {}", server.address());
    tokio::signal::ctrl_c()
        .await
        .map_err(|error| SparkXError::execution(format!("listen for Ctrl+C: {error}")))?;
    server.close().await
}
