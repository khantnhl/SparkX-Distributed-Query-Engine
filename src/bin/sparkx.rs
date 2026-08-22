use clap::{Parser, ValueEnum};
use sparkx::{Result, Session, SessionConfig, SparkXError};
use std::path::PathBuf;

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

    let result = session.execute_sql(&args.sql).await?;
    println!("{}", result.pretty()?);
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
            if result.distributed {
                "local-cluster"
            } else {
                "native"
            },
            result.stages,
            if result.stages == 1 { "" } else { "s" }
        );
    }
    Ok(())
}
