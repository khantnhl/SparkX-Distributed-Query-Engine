use crate::error::{Result, SparkXError};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const DEFAULT_MEMORY_LIMIT_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug)]
struct MemoryState {
    limit_bytes: u64,
    reserved_bytes: AtomicU64,
    peak_bytes: AtomicU64,
}

/// Query-scoped accounting for memory owned by blocking operators.
#[derive(Debug, Clone)]
pub struct QueryMemory {
    state: Arc<MemoryState>,
}

impl QueryMemory {
    pub fn new(limit_bytes: u64) -> Self {
        Self {
            state: Arc::new(MemoryState {
                limit_bytes: limit_bytes.max(1),
                reserved_bytes: AtomicU64::new(0),
                peak_bytes: AtomicU64::new(0),
            }),
        }
    }

    pub fn limit_bytes(&self) -> u64 {
        self.state.limit_bytes
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.state.reserved_bytes.load(Ordering::Acquire)
    }

    pub fn peak_bytes(&self) -> u64 {
        self.state.peak_bytes.load(Ordering::Acquire)
    }

    pub fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation> {
        self.reserve(bytes)?;
        Ok(MemoryReservation {
            memory: self.clone(),
            bytes,
        })
    }

    fn reserve(&self, bytes: u64) -> Result<()> {
        let mut current = self.state.reserved_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                SparkXError::resource_exhausted("query memory reservation overflowed")
            })?;
            if next > self.state.limit_bytes {
                return Err(SparkXError::resource_exhausted(format!(
                    "query requires at least {next} bytes but its limit is {} bytes",
                    self.state.limit_bytes
                )));
            }
            match self.state.reserved_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.state.peak_bytes.fetch_max(next, Ordering::AcqRel);
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let previous = self.state.reserved_bytes.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(
            previous >= bytes,
            "released more query memory than reserved"
        );
    }
}

/// An RAII memory claim. Dropping it returns its bytes to the query budget.
#[derive(Debug)]
pub struct MemoryReservation {
    memory: QueryMemory,
    bytes: u64,
}

impl MemoryReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn try_grow(&mut self, additional_bytes: u64) -> Result<()> {
        let next = self
            .bytes
            .checked_add(additional_bytes)
            .ok_or_else(|| SparkXError::resource_exhausted("memory reservation size overflowed"))?;
        self.memory.reserve(additional_bytes)?;
        self.bytes = next;
        Ok(())
    }

    pub fn shrink(&mut self, bytes: u64) {
        let released = bytes.min(self.bytes);
        self.bytes -= released;
        self.memory.release(released);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}
