//! SparkX is a deliberately small, inspectable distributed query engine prototype.
//!
//! It owns the query pipeline (catalog, logical plan, optimizer, physical planner,
//! vectorized operators, scheduler, and metrics) while using Apache Arrow as its
//! in-memory ABI and Parquet/CSV as storage formats.

pub mod cancellation;
pub mod catalog;
pub mod distributed;
pub mod error;
pub mod execution;
pub mod expr;
pub mod logical;
pub mod memory;
pub mod metrics;
pub mod optimizer;
pub mod planner;
pub mod protocol;
mod pruning;
mod row_key;
pub mod session;

pub use cancellation::CancellationToken;
pub use error::{Result, SparkXError};
pub use memory::{DEFAULT_MEMORY_LIMIT_BYTES, MemoryReservation, QueryMemory};
pub use session::{QueryResult, Session, SessionConfig};
