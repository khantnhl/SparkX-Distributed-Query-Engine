use clap::Parser;
use sparkx::catalog::{Catalog, CsvTable, ParquetTable, TableRef};
use sparkx::protocol::WorkerId;
use sparkx::worker::{RemoteWorker, WorkerConfig};
use sparkx::{CancellationToken, Result, SparkXError};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
struct TableSpec {
    name: String,
    path: PathBuf,
}

impl FromStr for TableSpec {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (name, path) = value
            .split_once('=')
            .ok_or_else(|| "table must use NAME=PATH".to_owned())?;
        if name.trim().is_empty() || path.trim().is_empty() {
            return Err("table name and path must not be empty".to_owned());
        }
        Ok(Self {
            name: name.to_owned(),
            path: PathBuf::from(path),
        })
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "sparkx-worker",
    version,
    about = "SparkX remote worker process"
)]
struct Args {
    /// Coordinator Flight/gRPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    coordinator: String,

    /// Stable worker identity used for leases and task updates.
    #[arg(long, default_value = "worker-1")]
    worker_id: String,

    /// Catalog entry in NAME=PATH form. Repeat for every CSV/Parquet table.
    #[arg(long = "table", value_name = "NAME=PATH", required = true)]
    tables: Vec<TableSpec>,

    /// Maximum concurrent task attempts.
    #[arg(long, default_value_t = default_slots())]
    slots: u32,

    /// Total bytes available to blocking operators on this worker.
    #[arg(long, default_value_t = sparkx::DEFAULT_MEMORY_LIMIT_BYTES)]
    memory_bytes: u64,

    /// Arrow record-batch row count.
    #[arg(long, default_value_t = 8_192)]
    batch_size: usize,

    /// Number of batches buffered between operators.
    #[arg(long, default_value_t = 2)]
    channel_capacity: usize,

    /// Worker heartbeat period in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    heartbeat_ms: u64,

    /// Assignment and cancellation polling period in milliseconds.
    #[arg(long, default_value_t = 100)]
    poll_ms: u64,

    /// Interface and port for serving task output over Arrow Flight.
    #[arg(long, default_value = "127.0.0.1:0")]
    data_bind: SocketAddr,

    /// Hostname or IP advertised to output consumers. Required with 0.0.0.0/:: binds.
    #[arg(long)]
    data_advertised_host: Option<String>,

    /// Maximum bytes retained for task output until consumers delete blocks.
    #[arg(long, default_value_t = sparkx::DEFAULT_MEMORY_LIMIT_BYTES)]
    data_storage_bytes: u64,

    /// Exit after this many terminal task attempts; intended for development and tests.
    #[arg(long)]
    max_tasks: Option<u64>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sparkx-worker: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let catalog = Arc::new(Catalog::default());
    for table in args.tables {
        catalog.register(table.name, open_table(&table.path)?);
    }

    let mut config = WorkerConfig::new(args.coordinator, WorkerId::new(args.worker_id)?);
    config.slots = args.slots;
    config.memory_bytes = args.memory_bytes;
    config.batch_size = args.batch_size;
    config.channel_capacity = args.channel_capacity;
    config.heartbeat_interval = Duration::from_millis(args.heartbeat_ms);
    config.poll_interval = Duration::from_millis(args.poll_ms);
    config.data_bind_address = args.data_bind;
    config.data_advertised_host = args.data_advertised_host;
    config.data_storage_bytes = args.data_storage_bytes;
    config.max_terminal_tasks = args.max_tasks;

    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });

    println!(
        "worker {} connecting to {} with {} slot(s)",
        config.worker_id.as_str(),
        config.coordinator_endpoint,
        config.slots
    );
    let summary = RemoteWorker::new(config, catalog)?
        .run_until(shutdown)
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&summary)
            .map_err(|error| SparkXError::execution(error.to_string()))?
    );
    Ok(())
}

fn open_table(path: &PathBuf) -> Result<TableRef> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("csv") => Ok(Arc::new(CsvTable::try_new(path)?)),
        Some("parquet") | Some("pq") => Ok(Arc::new(ParquetTable::try_new(path)?)),
        _ => Err(SparkXError::planning(format!(
            "cannot infer table format for {}; use a .csv, .parquet, or .pq filename",
            path.display()
        ))),
    }
}

fn default_slots() -> u32 {
    u32::try_from(num_cpus::get().max(1)).unwrap_or(u32::MAX)
}
