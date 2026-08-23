use crate::{Result, SparkXError};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, Rows, SortField};
use std::mem::size_of;

pub(crate) type EncodedKey = Box<[u8]>;

pub(crate) struct RowKeyEncoder {
    converter: Option<RowConverter>,
}

impl RowKeyEncoder {
    pub(crate) fn new(data_types: impl IntoIterator<Item = DataType>) -> Result<Self> {
        let fields = data_types
            .into_iter()
            .map(SortField::new)
            .collect::<Vec<_>>();
        let converter = if fields.is_empty() {
            None
        } else {
            Some(RowConverter::new(fields)?)
        };
        Ok(Self { converter })
    }

    pub(crate) fn from_columns(columns: &[ArrayRef]) -> Result<Self> {
        Self::new(columns.iter().map(|column| column.data_type().clone()))
    }

    pub(crate) fn encode(&self, columns: &[ArrayRef], row_count: usize) -> Result<EncodedRows> {
        match &self.converter {
            Some(converter) => {
                let rows = converter.convert_columns(columns)?;
                if rows.num_rows() != row_count {
                    return Err(SparkXError::execution(format!(
                        "encoded key row count {} does not match input row count {row_count}",
                        rows.num_rows()
                    )));
                }
                Ok(EncodedRows::Rows(rows))
            }
            None if columns.is_empty() => Ok(EncodedRows::Global { row_count }),
            None => Err(SparkXError::execution(
                "global row-key encoder received key columns",
            )),
        }
    }

    pub(crate) fn decode(&self, keys: &[EncodedKey]) -> Result<Vec<ArrayRef>> {
        let Some(converter) = &self.converter else {
            if keys.iter().any(|key| !key.is_empty()) {
                return Err(SparkXError::execution(
                    "global row-key encoder received non-empty keys",
                ));
            }
            return Ok(Vec::new());
        };

        let total_bytes = keys
            .iter()
            .map(|key| key.len())
            .fold(0_usize, usize::saturating_add);
        let parser = converter.parser();
        let mut rows = converter.empty_rows(keys.len(), total_bytes);
        for key in keys {
            rows.push(parser.parse(key));
        }
        Ok(converter.convert_rows(&rows)?)
    }
}

pub(crate) enum EncodedRows {
    Global { row_count: usize },
    Rows(Rows),
}

impl EncodedRows {
    pub(crate) fn key(&self, row: usize) -> &[u8] {
        match self {
            Self::Global { row_count } => {
                assert!(row < *row_count, "encoded row index out of bounds");
                &[]
            }
            Self::Rows(rows) => rows.row(row).data(),
        }
    }

    pub(crate) fn memory_size(&self) -> u64 {
        match self {
            Self::Global { .. } => 0,
            Self::Rows(rows) => rows.size() as u64,
        }
    }
}

pub(crate) fn key_has_null(columns: &[ArrayRef], row: usize) -> bool {
    columns.iter().any(|column| column.is_null(row))
}

pub(crate) fn encoded_key_memory_bytes(key: &[u8]) -> u64 {
    (size_of::<EncodedKey>() as u64).saturating_add(key.len() as u64)
}
