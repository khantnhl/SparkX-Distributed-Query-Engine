//! Immutable block storage. Disk publication uses an fsynced temporary file and atomic rename.
use super::data_plane::{StoredBlock, block_metadata};
use crate::{Result, SparkXError};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub(super) trait BlockStorage: Send {
    fn available_bytes(&self) -> u64;
    fn upload_budget(&self, ticket: &str) -> u64;
    fn insert(&mut self, ticket: String, block: StoredBlock) -> Result<()>;
    fn get(&mut self, ticket: &str) -> Result<StoredBlock>;
    fn remove(&mut self, ticket: &str) -> Result<()>;
}

pub(super) struct MemoryBlockStore {
    capacity: u64,
    used: u64,
    blocks: BTreeMap<String, StoredBlock>,
}
impl MemoryBlockStore {
    pub(super) fn new(capacity: u64) -> Result<Self> {
        validate_capacity(capacity)?;
        Ok(Self {
            capacity,
            used: 0,
            blocks: BTreeMap::new(),
        })
    }
}
impl BlockStorage for MemoryBlockStore {
    fn upload_budget(&self, ticket: &str) -> u64 {
        self.available_bytes().saturating_add(
            self.blocks
                .get(ticket)
                .map_or(0, |block| block.charged_bytes),
        )
    }
    fn available_bytes(&self) -> u64 {
        self.capacity.saturating_sub(self.used)
    }
    fn insert(&mut self, ticket: String, block: StoredBlock) -> Result<()> {
        if let Some(existing) = self.blocks.get(&ticket) {
            return same_checksum(&existing.checksum, &block.checksum);
        }
        if block.charged_bytes > self.available_bytes() {
            return Err(full());
        }
        self.used += block.charged_bytes;
        self.blocks.insert(ticket, block);
        Ok(())
    }
    fn get(&mut self, ticket: &str) -> Result<StoredBlock> {
        self.blocks.get(ticket).cloned().ok_or_else(missing)
    }
    fn remove(&mut self, ticket: &str) -> Result<()> {
        let block = self.blocks.remove(ticket).ok_or_else(missing)?;
        self.used -= block.charged_bytes;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Header {
    version: u16,
    ticket: String,
    checksum: String,
}
struct DiskEntry {
    path: PathBuf,
    bytes: u64,
    checksum: String,
}
pub(super) struct DiskBlockStore {
    directory: PathBuf,
    capacity: u64,
    used: u64,
    blocks: BTreeMap<String, DiskEntry>,
    // Prevent two live services from modifying the same directory.
    _lock: File,
}
impl DiskBlockStore {
    pub(super) fn open(directory: PathBuf, capacity: u64) -> Result<Self> {
        validate_capacity(capacity)?;
        fs::create_dir_all(&directory)?;
        // OS file locks are released even if the process crashes.
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join(".sparkx.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|error| {
            SparkXError::execution(format!("shuffle directory is already in use: {error}"))
        })?;
        let mut store = Self {
            directory,
            capacity,
            used: 0,
            blocks: BTreeMap::new(),
            _lock: lock,
        };
        for entry in fs::read_dir(&store.directory)? {
            let path = entry?.path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if !name.starts_with("sparkx-") {
                continue;
            }
            if name.ends_with(".pending") {
                fs::remove_file(path)?;
                continue;
            }
            if !name.ends_with(".block") {
                continue;
            }
            let bytes = fs::metadata(&path)?.len();
            if bytes > store.available_bytes() {
                return Err(full());
            }
            let (header, _) = read_block(&path)?;
            if store.blocks.contains_key(&header.ticket) {
                return Err(SparkXError::protocol("duplicate persisted block ticket"));
            }
            store.used += bytes;
            store.blocks.insert(
                header.ticket,
                DiskEntry {
                    path,
                    bytes,
                    checksum: header.checksum,
                },
            );
        }
        Ok(store)
    }
}
impl BlockStorage for DiskBlockStore {
    fn upload_budget(&self, ticket: &str) -> u64 {
        self.available_bytes()
            .saturating_add(self.blocks.get(ticket).map_or(0, |block| block.bytes))
    }
    fn available_bytes(&self) -> u64 {
        self.capacity.saturating_sub(self.used)
    }
    fn insert(&mut self, ticket: String, block: StoredBlock) -> Result<()> {
        if let Some(existing) = self.blocks.get(&ticket) {
            return same_checksum(&existing.checksum, &block.checksum);
        }
        let name = format!("sparkx-{:08x}", crc32fast::hash(ticket.as_bytes()));
        let path = self.directory.join(format!("{name}.block"));
        // Hash collisions fail explicitly instead of overwriting another immutable ticket.
        if path.exists() {
            return Err(SparkXError::protocol("persistent block filename collision"));
        }
        let pending = self.directory.join(format!("{name}.pending"));
        let write = (|| -> Result<u64> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&pending)?;
            let header = serde_json::to_vec(&Header {
                version: crate::protocol::PROTOCOL_VERSION,
                ticket: ticket.clone(),
                checksum: block.checksum.clone(),
            })
            .map_err(|error| SparkXError::protocol(error.to_string()))?;
            let mut counter = ByteCounter(0);
            {
                let mut writer = StreamWriter::try_new(&mut counter, &block.schema)?;
                for batch in &block.batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            if counter
                .0
                .saturating_add(header.len() as u64)
                .saturating_add(4)
                > self.available_bytes()
            {
                return Err(full());
            }
            file.write_all(&(header.len() as u32).to_le_bytes())?;
            file.write_all(&header)?;
            {
                let mut writer = StreamWriter::try_new(&mut file, &block.schema)?;
                for batch in &block.batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            let bytes = file.metadata()?.len();
            if bytes > self.available_bytes() {
                return Err(full());
            }
            file.sync_all()?;
            fs::rename(&pending, &path)?;
            sync_directory(&self.directory)?;
            Ok(bytes)
        })();
        if write.is_err() {
            let _ = fs::remove_file(&pending);
        }
        let bytes = write?;
        self.used += bytes;
        self.blocks.insert(
            ticket,
            DiskEntry {
                path,
                bytes,
                checksum: block.checksum,
            },
        );
        Ok(())
    }
    fn get(&mut self, ticket: &str) -> Result<StoredBlock> {
        let entry = self.blocks.get(ticket).ok_or_else(missing)?;
        let (header, block) = read_block(&entry.path)?;
        if header.ticket != ticket {
            return Err(SparkXError::protocol("persisted block ticket mismatch"));
        }
        same_checksum(&entry.checksum, &block.checksum)?;
        Ok(block)
    }
    fn remove(&mut self, ticket: &str) -> Result<()> {
        let entry = self.blocks.get(ticket).ok_or_else(missing)?;
        fs::remove_file(&entry.path)?;
        sync_directory(&self.directory)?;
        let entry = self.blocks.remove(ticket).expect("checked disk entry");
        self.used -= entry.bytes;
        Ok(())
    }
}
fn read_block(path: &Path) -> Result<(Header, StoredBlock)> {
    let mut file = File::open(path)?;
    let mut size = [0; 4];
    file.read_exact(&mut size)?;
    let size = u32::from_le_bytes(size) as usize;
    if size > 4096 {
        return Err(SparkXError::protocol(
            "invalid persisted block header length",
        ));
    }
    let mut header = vec![0; size];
    file.read_exact(&mut header)?;
    let header: Header = serde_json::from_slice(&header)
        .map_err(|error| SparkXError::protocol(error.to_string()))?;
    if header.version != crate::protocol::PROTOCOL_VERSION {
        return Err(SparkXError::protocol("persisted block version mismatch"));
    }
    let reader = StreamReader::try_new(file, None)?;
    let schema = reader.schema();
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    let (_, bytes, checksum) = block_metadata(&schema, &batches)?;
    same_checksum(&header.checksum, &checksum)?;
    Ok((
        header,
        StoredBlock {
            schema,
            batches,
            checksum,
            charged_bytes: bytes.max(1),
        },
    ))
}
fn validate_capacity(capacity: u64) -> Result<()> {
    if capacity == 0 {
        return Err(SparkXError::planning(
            "data-plane storage capacity must be greater than zero",
        ));
    }
    Ok(())
}
fn same_checksum(left: &str, right: &str) -> Result<()> {
    if left != right {
        return Err(SparkXError::protocol("immutable block checksum mismatch"));
    }
    Ok(())
}
fn full() -> SparkXError {
    SparkXError::resource_exhausted("data-plane storage capacity exceeded")
}
fn missing() -> SparkXError {
    SparkXError::NotFound("data-plane block does not exist".into())
}
fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

struct ByteCounter(u64);
impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
