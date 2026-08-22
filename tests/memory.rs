use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::catalog::MemoryTable;
use sparkx::{QueryMemory, Session, SessionConfig, SparkXError};
use std::sync::Arc;

#[test]
fn reservations_enforce_the_query_limit_and_release_on_drop() {
    let memory = QueryMemory::new(100);
    let reservation = memory.try_reserve(60).unwrap();

    assert_eq!(reservation.bytes(), 60);
    assert_eq!(memory.reserved_bytes(), 60);
    assert_eq!(memory.peak_bytes(), 60);

    let error = memory.try_reserve(41).unwrap_err();
    assert!(matches!(error, SparkXError::ResourceExhausted(_)));
    assert_eq!(memory.reserved_bytes(), 60);

    drop(reservation);
    assert_eq!(memory.reserved_bytes(), 0);
    assert_eq!(memory.peak_bytes(), 60);
}

#[test]
fn reservations_can_grow_and_shrink_without_losing_peak_usage() {
    let memory = QueryMemory::new(100);
    let mut reservation = memory.try_reserve(20).unwrap();

    reservation.try_grow(50).unwrap();
    assert_eq!(reservation.bytes(), 70);
    assert_eq!(memory.reserved_bytes(), 70);

    reservation.shrink(30);
    assert_eq!(reservation.bytes(), 40);
    assert_eq!(memory.reserved_bytes(), 40);
    assert_eq!(memory.peak_bytes(), 70);

    drop(reservation);
    assert_eq!(memory.reserved_bytes(), 0);
}

fn execution_session(memory_limit_bytes: u64, distributed: bool) -> (Session, u64, u64) {
    let session = Session::new(SessionConfig {
        memory_limit_bytes,
        workers: 2,
        distributed,
        ..SessionConfig::default()
    });
    let sales_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("customer_id", DataType::Int64, false),
    ]));
    let sales_batches = vec![
        RecordBatch::try_new(
            sales_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["east", "west"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap(),
        RecordBatch::try_new(
            sales_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec!["east", "west"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap(),
    ];
    let sales_bytes = sales_batches
        .iter()
        .map(|batch| batch.get_array_memory_size() as u64)
        .sum();
    session.register_memory(
        "sales",
        MemoryTable::new(
            sales_schema,
            sales_batches.into_iter().map(|batch| vec![batch]).collect(),
        )
        .unwrap(),
    );

    let customers_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let customers = RecordBatch::try_new(
        customers_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Ada", "Ben"])),
        ],
    )
    .unwrap();
    let join_bytes = sales_bytes + customers.get_array_memory_size() as u64;
    session.register_memory(
        "customers",
        MemoryTable::new(customers_schema, vec![vec![customers]]).unwrap(),
    );
    (session, sales_bytes, join_bytes)
}

#[tokio::test]
async fn blocking_operators_enforce_memory_limits() {
    let (_, sales_bytes, join_bytes) = execution_session(u64::MAX, false);

    let (aggregate, _, _) = execution_session(sales_bytes, false);
    let error = aggregate
        .execute_sql("SELECT region, COUNT(*) AS rows FROM sales GROUP BY region")
        .await
        .unwrap_err();
    assert!(matches!(error, SparkXError::ResourceExhausted(_)));

    let (sort, _, _) = execution_session(sales_bytes, false);
    let error = sort
        .execute_sql("SELECT id, region, customer_id FROM sales ORDER BY id")
        .await
        .unwrap_err();
    assert!(matches!(error, SparkXError::ResourceExhausted(_)));

    let (join, _, _) = execution_session(join_bytes, false);
    let error = join
        .execute_sql(
            "SELECT sales.id, customers.name FROM sales \
             JOIN customers ON sales.customer_id = customers.id",
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SparkXError::ResourceExhausted(_)));
}

#[tokio::test]
async fn successful_queries_report_peak_and_release_reserved_memory() {
    for distributed in [false, true] {
        let (session, _, _) = execution_session(1024 * 1024, distributed);
        let result = session
            .execute_sql("SELECT region, COUNT(*) AS rows FROM sales GROUP BY region")
            .await
            .unwrap();

        assert_eq!(result.distributed, distributed);
        assert!(result.metrics.memory_peak_bytes > 0);
        assert_eq!(result.metrics.memory_reserved_bytes, 0);
    }
}
