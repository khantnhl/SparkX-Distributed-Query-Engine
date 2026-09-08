//! Deterministic hash routing for materialized, bounded remote stage outputs.
use crate::protocol::{HashExchange, PartitionId};
use crate::row_key::RowKeyEncoder;
use crate::{CancellationToken, MemoryReservation, QueryMemory, Result, SparkXError};
use arrow::array::UInt64Array;
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

pub(crate) type PartitionOutput = (PartitionId, Vec<RecordBatch>, MemoryReservation);

pub(crate) fn repartition(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    exchange: &HashExchange,
    memory: &QueryMemory,
    cancellation: &CancellationToken,
) -> Result<Vec<PartitionOutput>> {
    cancellation.check()?;
    if exchange.partition_count == 0
        || exchange.columns.is_empty()
        || exchange
            .columns
            .iter()
            .any(|column| *column >= schema.fields().len())
    {
        return Err(SparkXError::protocol(
            "invalid hash exchange keys or partition count",
        ));
    }
    let encoder = RowKeyEncoder::new(
        exchange
            .columns
            .iter()
            .map(|column| schema.field(*column).data_type().clone()),
    )?;
    let mut outputs = (0..exchange.partition_count)
        .map(|partition| Ok((PartitionId(partition), Vec::new(), memory.try_reserve(0)?)))
        .collect::<Result<Vec<PartitionOutput>>>()?;
    for batch in batches {
        cancellation.check()?;
        let columns = exchange
            .columns
            .iter()
            .map(|column| batch.column(*column).clone())
            .collect::<Vec<_>>();
        let keys = encoder.encode(&columns, batch.num_rows())?;
        let _keys = memory.try_reserve(keys.memory_size())?;
        // Index buffers and the Arrow take index array can coexist.
        let _indices = memory.try_reserve((batch.num_rows() as u64).saturating_mul(24))?;
        let mut indices = vec![Vec::<u64>::new(); exchange.partition_count as usize];
        for row in 0..batch.num_rows() {
            if row % 8192 == 0 {
                cancellation.check()?;
            }
            let partition = crc32fast::hash(keys.key(row)) % exchange.partition_count;
            indices[partition as usize].push(row as u64);
        }
        for (partition, rows) in indices.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let indices = UInt64Array::from(rows);
            let arrays = batch
                .columns()
                .iter()
                .map(|array| take(array.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let output = RecordBatch::try_new(schema.clone(), arrays)?;
            outputs[partition]
                .2
                .try_grow(output.get_array_memory_size() as u64)?;
            outputs[partition].1.push(output);
        }
    }
    // Empty destinations still receive an uploaded schema-bearing manifest.
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[test]
    fn composite_and_null_keys_route_consistently_across_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("a"),
                    Some("b"),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 3])),
            ],
        )
        .unwrap();
        let memory = QueryMemory::new(1_000_000);
        let exchange = HashExchange {
            columns: vec![0, 1],
            partition_count: 7,
        };
        let outputs = repartition(
            &schema,
            &[batch.clone(), batch],
            &exchange,
            &memory,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(outputs.len(), 7);
        let mut keys = BTreeMap::new();
        let mut count = 0;
        for (partition, batches, _) in &outputs {
            for batch in batches {
                let strings = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let key = (
                        if strings.is_null(row) {
                            None
                        } else {
                            Some(strings.value(row).to_owned())
                        },
                        values.value(row),
                    );
                    if let Some(previous) = keys.insert(key, *partition) {
                        assert_eq!(previous, *partition);
                    }
                    count += 1;
                }
            }
        }
        assert_eq!(count, 8);
        assert_eq!(keys.len(), 3);
        drop(outputs);
        assert_eq!(memory.reserved_bytes(), 0);
    }

    #[test]
    fn empty_input_preserves_all_destinations_and_invalid_keys_fail() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let memory = QueryMemory::new(1024);
        let mut exchange = HashExchange {
            columns: vec![0],
            partition_count: 3,
        };
        let cancellation = CancellationToken::new();
        let outputs = repartition(&schema, &[], &exchange, &memory, &cancellation).unwrap();
        assert_eq!(outputs.len(), 3);
        assert!(outputs.iter().all(|(_, batches, _)| batches.is_empty()));
        exchange.columns = vec![1];
        assert!(repartition(&schema, &[], &exchange, &memory, &cancellation).is_err());
    }

    #[test]
    fn exhaustion_and_cancellation_release_reservations() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let memory = QueryMemory::new(1);
        let exchange = HashExchange {
            columns: vec![0],
            partition_count: 2,
        };
        let cancellation = CancellationToken::new();
        assert!(repartition(&schema, &[batch], &exchange, &memory, &cancellation).is_err());
        assert_eq!(memory.reserved_bytes(), 0);
        cancellation.cancel();
        assert!(matches!(
            repartition(&schema, &[], &exchange, &memory, &cancellation),
            Err(SparkXError::Cancelled)
        ));
    }
}
