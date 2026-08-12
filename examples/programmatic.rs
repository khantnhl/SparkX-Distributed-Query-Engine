use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::catalog::MemoryTable;
use sparkx::{Result, Session, SessionConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["east", "west", "east", "west"])),
            Arc::new(Float64Array::from(vec![12.0, 30.0, 8.0, 50.0])),
        ],
    )?;

    let session = Session::new(SessionConfig {
        distributed: true,
        workers: 2,
        ..SessionConfig::default()
    });
    session.register_memory(
        "orders",
        MemoryTable::new(
            schema,
            vec![vec![batch.slice(0, 2)], vec![batch.slice(2, 2)]],
        )?,
    );

    let result = session
        .execute_sql(
            "SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue FROM orders GROUP BY region",
        )
        .await?;
    println!("{}", result.pretty()?);
    println!("{}", serde_json::to_string_pretty(&result.metrics).unwrap());
    Ok(())
}
