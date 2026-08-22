use crate::error::{Result, SparkXError};
use crate::expr::Expr;
use crate::pruning::row_group_may_match;
use arrow::csv::reader::{Format, ReaderBuilder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub trait TableProvider: std::fmt::Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn partition_count(&self) -> usize;
    fn estimated_bytes(&self) -> u64;
    fn partition_may_match(&self, _partition: usize, _filters: &[Expr]) -> Result<bool> {
        Ok(true)
    }
    fn scan_partition(
        &self,
        partition: usize,
        projection: Option<&[usize]>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>>;
}

pub type TableRef = Arc<dyn TableProvider>;

#[derive(Debug, Default)]
pub struct Catalog {
    tables: RwLock<HashMap<String, TableRef>>,
}

impl Catalog {
    pub fn register(&self, name: impl Into<String>, provider: TableRef) {
        self.tables
            .write()
            .insert(name.into().to_ascii_lowercase(), provider);
    }

    pub fn table(&self, name: &str) -> Result<TableRef> {
        self.tables
            .read()
            .get(&name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| SparkXError::NotFound(format!("table '{name}'")))
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names = self.tables.read().keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }
}

#[derive(Debug, Clone)]
pub struct MemoryTable {
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    estimated_bytes: u64,
}

impl MemoryTable {
    pub fn new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        for batch in partitions.iter().flatten() {
            if batch.schema() != schema {
                return Err(SparkXError::planning(
                    "all in-memory batches must use the table schema",
                ));
            }
        }
        let estimated_bytes = partitions
            .iter()
            .flatten()
            .map(|batch| batch.get_array_memory_size() as u64)
            .sum();
        Ok(Self {
            schema,
            partitions: if partitions.is_empty() {
                vec![Vec::new()]
            } else {
                partitions
            },
            estimated_bytes,
        })
    }

    pub fn from_batches(batches: Vec<RecordBatch>, partitions: usize) -> Result<Self> {
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| SparkXError::planning("at least one batch is required"))?;
        let mut output = vec![Vec::new(); partitions.max(1)];
        for (index, batch) in batches.into_iter().enumerate() {
            let target = index % output.len();
            output[target].push(batch);
        }
        Self::new(schema, output)
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    fn scan_partition(
        &self,
        partition: usize,
        projection: Option<&[usize]>,
        _batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self
            .partitions
            .get(partition)
            .ok_or_else(|| SparkXError::execution(format!("invalid partition {partition}")))?;
        batches
            .iter()
            .map(|batch| match projection {
                Some(indices) => Ok(batch.project(indices)?),
                None => Ok(batch.clone()),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct CsvTable {
    path: PathBuf,
    schema: SchemaRef,
    estimated_bytes: u64,
}

impl CsvTable {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let mut file = File::open(&path)?;
        let format = Format::default().with_header(true);
        let (schema, _) = format.infer_schema(&mut file, Some(1_000))?;
        file.seek(SeekFrom::Start(0))?;
        let estimated_bytes = file.metadata()?.len();
        Ok(Self {
            path,
            schema: Arc::new(schema),
            estimated_bytes,
        })
    }
}

impl TableProvider for CsvTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    fn scan_partition(
        &self,
        partition: usize,
        projection: Option<&[usize]>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        if partition != 0 {
            return Err(SparkXError::execution(format!(
                "CSV source has no partition {partition}"
            )));
        }
        let file = File::open(&self.path)?;
        let mut builder = ReaderBuilder::new(self.schema.clone())
            .with_header(true)
            .with_batch_size(batch_size);
        if let Some(indices) = projection {
            builder = builder.with_projection(indices.to_vec());
        }
        builder
            .build(file)?
            .map(|batch| batch.map_err(SparkXError::from))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct ParquetTable {
    path: PathBuf,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
    parquet_columns: Vec<Option<usize>>,
    estimated_bytes: u64,
}

impl ParquetTable {
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let file = File::open(&path)?;
        let estimated_bytes = file.metadata()?.len();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let parquet_columns = schema
            .fields()
            .iter()
            .map(|field| {
                let mut matches = builder
                    .parquet_schema()
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| column.path().parts().first() == Some(field.name()))
                    .map(|(index, _)| index);
                let first = matches.next();
                if matches.next().is_none() {
                    first
                } else {
                    None
                }
            })
            .collect();
        Ok(Self {
            path,
            schema,
            metadata: builder.metadata().clone(),
            parquet_columns,
            estimated_bytes,
        })
    }
}

impl TableProvider for ParquetTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partition_count(&self) -> usize {
        self.metadata.num_row_groups()
    }

    fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    fn partition_may_match(&self, partition: usize, filters: &[Expr]) -> Result<bool> {
        if partition >= self.metadata.num_row_groups() {
            return Err(SparkXError::execution(format!(
                "Parquet source has no row group {partition}"
            )));
        }
        Ok(row_group_may_match(
            self.schema.as_ref(),
            &self.parquet_columns,
            self.metadata.row_group(partition),
            filters,
        ))
    }

    fn scan_partition(
        &self,
        partition: usize,
        projection: Option<&[usize]>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        if partition >= self.metadata.num_row_groups() {
            return Err(SparkXError::execution(format!(
                "Parquet source has no row group {partition}"
            )));
        }
        let file = File::open(&self.path)?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(batch_size)
            .with_row_groups(vec![partition]);
        if let Some(indices) = projection {
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
            builder = builder.with_projection(mask);
        }
        builder
            .build()?
            .map(|batch| batch.map_err(SparkXError::from))
            .collect()
    }
}

pub fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    match projection {
        None => schema.clone(),
        Some(indices) => Arc::new(Schema::new(
            indices
                .iter()
                .map(|index| schema.field(*index).clone())
                .collect::<Vec<_>>(),
        )),
    }
}
