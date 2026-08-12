use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use sparkx::catalog::MemoryTable;
use sparkx::expr::{ScalarValue, value_at};
use sparkx::{Session, SessionConfig, SparkXError};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;

fn sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]))
}

fn sales_batch(offset: i64) -> RecordBatch {
    RecordBatch::try_new(
        sales_schema(),
        vec![
            Arc::new(Int64Array::from(vec![offset, offset + 1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["east", "west"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Float64Array::from(vec![
                10.0 + offset as f64,
                20.0 + offset as f64,
            ])),
        ],
    )
    .unwrap()
}

fn session(distributed: bool) -> Session {
    let session = Session::new(SessionConfig {
        batch_size: 2,
        channel_capacity: 1,
        workers: 2,
        distributed,
    });
    let partitions = (0..4)
        .map(|partition| vec![sales_batch(partition * 2)])
        .collect();
    session.register_memory(
        "sales",
        MemoryTable::new(sales_schema(), partitions).unwrap(),
    );
    session
}

#[test]
fn normalizes_zero_resource_settings() {
    let session = Session::new(SessionConfig {
        batch_size: 0,
        channel_capacity: 0,
        workers: 0,
        distributed: false,
    });
    assert_eq!(session.config().batch_size, 1);
    assert_eq!(session.config().channel_capacity, 1);
    assert_eq!(session.config().workers, 1);
}

fn grouped_totals(result: &sparkx::QueryResult) -> BTreeMap<String, (u64, f64)> {
    let mut values = BTreeMap::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            let ScalarValue::Utf8(region) = value_at(batch.column(0).as_ref(), row).unwrap() else {
                panic!("region must be UTF-8")
            };
            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row);
            let total = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            values.insert(region, (count, total));
        }
    }
    values
}

#[tokio::test]
async fn filters_projects_sorts_and_limits() {
    let result = session(false)
        .execute_sql(
            "SELECT id, amount * 2 AS doubled FROM sales WHERE amount >= 22 ORDER BY id DESC LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(result.row_count(), 3);
    let ids = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!([ids.value(0), ids.value(1), ids.value(2)], [7, 5, 3]);
    assert!(result.metrics.input_rows >= 8);
}

#[tokio::test]
async fn preserves_aliases_and_coerces_mixed_numeric_arithmetic() {
    let result = session(false)
        .execute_sql("SELECT id AS order_id, id * 1.5 AS weighted FROM sales LIMIT 1")
        .await
        .unwrap();
    assert_eq!(result.batches[0].schema().field(0).name(), "order_id");
    assert_eq!(result.batches[0].schema().field(1).name(), "weighted");
    assert_eq!(
        result.batches[0].schema().field(1).data_type(),
        &DataType::Float64
    );
}

#[tokio::test]
async fn native_grouped_aggregates_are_correct() {
    let result = session(false)
        .execute_sql(
            "SELECT region, COUNT(*) AS rows, SUM(amount) AS revenue FROM sales GROUP BY region",
        )
        .await
        .unwrap();
    let totals = grouped_totals(&result);
    assert_eq!(totals["east"].0, 4);
    assert_eq!(totals["west"].0, 4);
    assert!((totals["east"].1 - 52.0).abs() < f64::EPSILON);
    assert!((totals["west"].1 - 92.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn aggregate_states_cover_distinct_avg_min_and_max() {
    let result = session(false)
        .execute_sql(
            "SELECT COUNT(DISTINCT region) AS regions, AVG(amount) AS average, MIN(amount) AS minimum, MAX(amount) AS maximum FROM sales",
        )
        .await
        .unwrap();
    let batch = &result.batches[0];
    assert_eq!(
        value_at(batch.column(0).as_ref(), 0).unwrap(),
        ScalarValue::UInt64(2)
    );
    assert_eq!(
        value_at(batch.column(1).as_ref(), 0).unwrap(),
        ScalarValue::Float64(18.0)
    );
    assert_eq!(
        value_at(batch.column(2).as_ref(), 0).unwrap(),
        ScalarValue::Float64(10.0)
    );
    assert_eq!(
        value_at(batch.column(3).as_ref(), 0).unwrap(),
        ScalarValue::Float64(26.0)
    );
}

#[tokio::test]
async fn global_aggregates_handle_empty_input() {
    let session = Session::new(SessionConfig::default());
    session.register_memory(
        "empty_sales",
        MemoryTable::new(sales_schema(), Vec::new()).unwrap(),
    );
    let result = session
        .execute_sql("SELECT COUNT(*) AS rows, SUM(amount) AS revenue FROM empty_sales")
        .await
        .unwrap();
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::UInt64(0)
    );
    assert_eq!(
        value_at(result.batches[0].column(1).as_ref(), 0).unwrap(),
        ScalarValue::Null
    );
}

#[tokio::test]
async fn local_cluster_matches_native_aggregate() {
    let sql = "SELECT region, COUNT(*) AS rows, SUM(amount) AS revenue FROM sales GROUP BY region";
    let native = session(false).execute_sql(sql).await.unwrap();
    let cluster = session(true).execute_sql(sql).await.unwrap();
    assert_eq!(grouped_totals(&native), grouped_totals(&cluster));
    assert!(cluster.distributed);
    assert_eq!(cluster.stages, 2);
    assert!(cluster.metrics.shuffled_rows > 0);
}

#[tokio::test]
async fn distinct_aggregate_safely_falls_back_to_native() {
    let result = session(true)
        .execute_sql("SELECT COUNT(DISTINCT customer_id) AS customers FROM sales")
        .await
        .unwrap();
    assert!(!result.distributed);
    assert_eq!(result.stages, 1);
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::UInt64(2)
    );
}

#[tokio::test]
async fn executes_inner_hash_join() {
    let session = session(false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("tier", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["gold", "silver"])),
        ],
    )
    .unwrap();
    session.register_memory(
        "customers",
        MemoryTable::new(schema, vec![vec![batch]]).unwrap(),
    );
    let result = session
        .execute_sql(
            "SELECT sales.id, customers.tier FROM sales JOIN customers ON sales.customer_id = customers.customer_id",
        )
        .await
        .unwrap();
    assert_eq!(result.row_count(), 8);
}

#[tokio::test]
async fn executes_left_hash_join_with_null_extension() {
    let session = session(false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("tier", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["gold"])),
        ],
    )
    .unwrap();
    session.register_memory(
        "customers",
        MemoryTable::new(schema, vec![vec![batch]]).unwrap(),
    );
    let result = session
        .execute_sql(
            "SELECT COUNT(customers.tier) AS matched FROM sales LEFT JOIN customers ON sales.customer_id = customers.customer_id",
        )
        .await
        .unwrap();
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::UInt64(4)
    );
}

#[tokio::test]
async fn reads_csv_and_pushes_scan_projection() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sales.csv");
    std::fs::write(&path, "id,region,amount\n1,east,12.5\n2,west,7.5\n").unwrap();
    let session = Session::new(SessionConfig::default());
    session.register_csv("sales", &path).unwrap();
    let result = session
        .execute_sql("SELECT region FROM sales WHERE amount > 10")
        .await
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert!(result.optimized_plan.contains("projection="));
    assert!(result.optimized_plan.contains("filters="));
}

#[tokio::test]
async fn reads_parquet_row_groups_as_partitions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sales.parquet");
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, sales_schema(), None).unwrap();
    writer.write(&sales_batch(0)).unwrap();
    writer.flush().unwrap();
    writer.write(&sales_batch(2)).unwrap();
    writer.close().unwrap();

    let session = Session::new(SessionConfig {
        distributed: true,
        workers: 2,
        ..SessionConfig::default()
    });
    session.register_parquet("sales", &path).unwrap();
    let result = session
        .execute_sql("SELECT COUNT(*) AS rows FROM sales")
        .await
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::UInt64(4)
    );
}

#[test]
fn explains_all_three_plan_layers() {
    let explanation = session(false)
        .explain(
            "SELECT region, SUM(amount) AS revenue FROM sales WHERE amount > 10 GROUP BY region",
        )
        .unwrap();
    assert!(explanation.contains("== Logical Plan =="));
    assert!(explanation.contains("== Optimized Logical Plan =="));
    assert!(explanation.contains("== Physical Plan =="));
    assert!(explanation.contains("HashAggregate"));
}

#[test]
fn rejects_unknown_columns_during_planning() {
    let error = session(false).sql("SELECT missing FROM sales").unwrap_err();
    assert!(matches!(error, SparkXError::Planning(_)));
}
