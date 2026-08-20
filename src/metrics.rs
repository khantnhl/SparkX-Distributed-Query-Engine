use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct QueryMetrics {
    input_rows: AtomicU64,
    output_rows: AtomicU64,
    input_batches: AtomicU64,
    output_batches: AtomicU64,
    bytes_scanned: AtomicU64,
    shuffled_rows: AtomicU64,
    tasks: AtomicU64,
    elapsed_ns: AtomicU64,
    operators: Mutex<BTreeMap<u32, OperatorMetricsSnapshot>>,
}

pub type MetricsRef = Arc<QueryMetrics>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub input_rows: u64,
    pub output_rows: u64,
    pub input_batches: u64,
    pub output_batches: u64,
    pub bytes_scanned: u64,
    pub shuffled_rows: u64,
    pub tasks: u64,
    pub elapsed_ns: u64,
    pub operators: Vec<OperatorMetricsSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperatorMetricsSnapshot {
    pub operator_id: u32,
    pub name: String,
    pub output_rows: u64,
    pub output_batches: u64,
    pub elapsed_ns: u64,
}

impl QueryMetrics {
    pub fn record_input(&self, rows: usize) {
        self.input_rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.input_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_output(&self, rows: usize) {
        self.output_rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.output_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_scanned_bytes(&self, bytes: u64) {
        self.bytes_scanned.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_shuffled_rows(&self, rows: usize) {
        self.shuffled_rows.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn add_task(&self) {
        self.tasks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_elapsed(&self, elapsed: Duration) {
        self.elapsed_ns
            .store(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn record_operator_output(&self, operator_id: u32, name: &str, rows: usize) {
        let mut operators = self.operators.lock();
        let operator = operators
            .entry(operator_id)
            .or_insert_with(|| OperatorMetricsSnapshot {
                operator_id,
                name: name.to_owned(),
                output_rows: 0,
                output_batches: 0,
                elapsed_ns: 0,
            });
        operator.output_rows += rows as u64;
        operator.output_batches += 1;
    }

    pub fn add_operator_elapsed(&self, operator_id: u32, name: &str, elapsed: Duration) {
        let mut operators = self.operators.lock();
        let operator = operators
            .entry(operator_id)
            .or_insert_with(|| OperatorMetricsSnapshot {
                operator_id,
                name: name.to_owned(),
                output_rows: 0,
                output_batches: 0,
                elapsed_ns: 0,
            });
        operator.elapsed_ns += elapsed.as_nanos() as u64;
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            input_rows: self.input_rows.load(Ordering::Relaxed),
            output_rows: self.output_rows.load(Ordering::Relaxed),
            input_batches: self.input_batches.load(Ordering::Relaxed),
            output_batches: self.output_batches.load(Ordering::Relaxed),
            bytes_scanned: self.bytes_scanned.load(Ordering::Relaxed),
            shuffled_rows: self.shuffled_rows.load(Ordering::Relaxed),
            tasks: self.tasks.load(Ordering::Relaxed),
            elapsed_ns: self.elapsed_ns.load(Ordering::Relaxed),
            operators: self.operators.lock().values().cloned().collect(),
        }
    }
}
