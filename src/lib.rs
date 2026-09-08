//! SparkX is a deliberately small, inspectable distributed query engine prototype.
//!
//! It owns the query pipeline (catalog, logical plan, optimizer, physical planner,
//! vectorized operators, scheduler, and metrics) while using Apache Arrow as its
//! in-memory ABI and Parquet/CSV as storage formats.

pub mod query;
pub use query::expr;
pub use query::logical;
pub use query::optimizer;
pub use query::planner;
pub use query::session;
pub mod runtime;
pub use runtime::cancellation;
pub use runtime::execution;
pub use runtime::memory;
pub use runtime::metrics;
pub(crate) use runtime::row_key;
pub mod storage;
pub use storage::catalog;
pub(crate) use storage::pruning;
pub mod cluster;
pub use cluster::control_plane;
pub use cluster::coordinator;
pub use cluster::data_plane;
pub use cluster::distributed;
pub(crate) use cluster::flight_exchange;
pub(crate) use cluster::hash_exchange;
pub use cluster::plan_codec;
pub use cluster::protocol;
pub use cluster::remote;
pub use cluster::worker;
pub mod error;

pub use cancellation::CancellationToken;
pub use error::{Result, SparkXError};
pub use memory::{DEFAULT_MEMORY_LIMIT_BYTES, MemoryReservation, QueryMemory};
pub use session::{QueryResult, Session, SessionConfig};
