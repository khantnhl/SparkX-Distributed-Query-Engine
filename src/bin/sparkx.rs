use clap::{Parser, ValueEnum};
use sparkx::protocol::QueryId;
use sparkx::remote::RemoteStageConfig;
use sparkx::{CancellationToken, Result, Session, SessionConfig, SparkXError};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum InputFormat {
    Auto,
    Csv,
    Parquet,
}

#[derive(Debug, Parser)]
#[command(
    name = "sparkx",
    version,
    about = "Arrow-native Rust query engine prototype"
)]
struct Args {
    /// CSV or Parquet file to register.
    #[arg(short, long)]
    input: PathBuf,

    /// Catalog name assigned to the input file.
    #[arg(short, long, default_value = "data")]
    table: String,

    /// SQL SELECT statement to execute.
    #[arg(short = 'q', long)]
    sql: String,

    /// Input format. Auto detects from the filename extension.
    #[arg(long, value_enum, default_value = "auto")]
    format: InputFormat,

    /// Print plans without executing the query.
    #[arg(long)]
    explain: bool,

    /// Enable the in-process distributed runner.
    #[arg(long)]
    distributed: bool,

    /// Execute eligible partition-local SQL through a standalone coordinator and workers.
    #[arg(long, conflicts_with = "distributed")]
    remote_coordinator: Option<String>,

    /// Query ID used by the remote coordinator. Generated when omitted.
    #[arg(long, requires = "remote_coordinator")]
    remote_query_id: Option<String>,

    /// Maximum remote stage runtime in milliseconds.
    #[arg(long, default_value_t = 300_000, requires = "remote_coordinator")]
    remote_timeout_ms: u64,

    /// Retain verified remote output blocks instead of deleting them after collection.
    #[arg(long, requires = "remote_coordinator")]
    keep_remote_output: bool,

    /// Number of local cluster workers.
    #[arg(long, default_value_t = num_cpus::get().max(1))]
    workers: usize,

    /// Arrow record-batch row count.
    #[arg(long, default_value_t = 8_192)]
    batch_size: usize,

    /// Number of batches buffered between operators.
    #[arg(long, default_value_t = 2)]
    channel_capacity: usize,

    /// Maximum memory reserved by blocking operators in one query.
    #[arg(long, default_value_t = sparkx::DEFAULT_MEMORY_LIMIT_BYTES)]
    memory_limit_bytes: u64,

    /// Print optimized and physical plans after execution.
    #[arg(long)]
    show_plan: bool,

    /// Print query metrics as JSON.
    #[arg(long)]
    metrics: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sparkx: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();
    let session = Session::new(SessionConfig {
        batch_size: args.batch_size.max(1),
        channel_capacity: args.channel_capacity.max(1),
        workers: args.workers.max(1),
        distributed: args.distributed,
        memory_limit_bytes: args.memory_limit_bytes.max(1),
    });
    let format = match args.format {
        InputFormat::Auto => match args
            .input
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("csv") => InputFormat::Csv,
            Some("parquet") | Some("pq") => InputFormat::Parquet,
            _ => {
                return Err(SparkXError::planning(
                    "cannot infer input format; pass --format csv or --format parquet",
                ));
            }
        },
        format => format,
    };
    match format {
        InputFormat::Csv => session.register_csv(&args.table, &args.input)?,
        InputFormat::Parquet => session.register_parquet(&args.table, &args.input)?,
        InputFormat::Auto => unreachable!(),
    }

    if args.explain {
        println!("{}", session.explain(&args.sql)?);
        return Ok(());
    }

    let remote_execution = args.remote_coordinator.is_some();
    let result = if let Some(endpoint) = &args.remote_coordinator {
        let mut remote = RemoteStageConfig::new(endpoint.clone());
        remote.timeout = Duration::from_millis(args.remote_timeout_ms);
        remote.delete_output_after_fetch = !args.keep_remote_output;
        let query_id = match &args.remote_query_id {
            Some(query_id) => QueryId::new(query_id.clone())?,
            None => generated_query_id()?,
        };
        let cancellation = CancellationToken::new();
        let signal = cancellation.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                signal.cancel();
            }
        });
        session
            .execute_sql_remote_with_cancellation(&args.sql, query_id, remote, cancellation)
            .await?
    } else {
        session.execute_sql(&args.sql).await?
    };
    println!("{}", result.pretty()?);
    for error in &result.cleanup_errors {
        eprintln!("warning: remote output cleanup failed: {error}");
    }
    if args.show_plan {
        println!("\n== Optimized Logical Plan ==\n{}", result.optimized_plan);
        println!("== Physical Plan ==\n{}", result.physical_plan);
    }
    if args.metrics {
        println!(
            "\n{}",
            serde_json::to_string_pretty(&result.metrics)
                .map_err(|error| SparkXError::execution(error.to_string()))?
        );
        println!(
            "runner: {} ({} stage{})",
            if remote_execution {
                "remote-flight"
            } else if result.distributed {
                "local-flight"
            } else {
                "native"
            },
            result.stages,
            if result.stages == 1 { "" } else { "s" }
        );
    }
    Ok(())
}

fn generated_query_id() -> Result<QueryId> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SparkXError::execution(format!("read system clock: {error}")))?
        .as_millis();
    QueryId::new(format!("cli-{}-{timestamp}", std::process::id()))
}
