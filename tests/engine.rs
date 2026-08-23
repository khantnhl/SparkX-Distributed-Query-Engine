use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use sparkx::catalog::{MemoryTable, TableProvider};
use sparkx::expr::{ScalarValue, value_at};
use sparkx::{CancellationToken, Session, SessionConfig, SparkXError};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

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
        ..SessionConfig::default()
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
        memory_limit_bytes: 0,
    });
    assert_eq!(session.config().batch_size, 1);
    assert_eq!(session.config().channel_capacity, 1);
    assert_eq!(session.config().workers, 1);
    assert_eq!(session.config().memory_limit_bytes, 1);
}

#[tokio::test]
async fn cancellation_stops_native_and_in_flight_distributed_queries() {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = session(false)
        .execute_sql_with_cancellation("SELECT id FROM sales", cancelled)
        .await
        .unwrap_err();
    assert!(matches!(error, SparkXError::Cancelled));

    #[derive(Debug)]
    struct SlowTable {
        schema: SchemaRef,
        batch: RecordBatch,
        started: parking_lot::Mutex<Option<oneshot::Sender<()>>>,
    }

    impl TableProvider for SlowTable {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn partition_count(&self) -> usize {
            2
        }

        fn estimated_bytes(&self) -> u64 {
            self.batch.get_array_memory_size() as u64
        }

        fn scan_partition(
            &self,
            _partition: usize,
            projection: Option<&[usize]>,
            _batch_size: usize,
        ) -> sparkx::Result<Vec<RecordBatch>> {
            if let Some(started) = self.started.lock().take() {
                let _ = started.send(());
            }
            std::thread::sleep(Duration::from_millis(100));
            let batch = match projection {
                Some(indices) => self.batch.project(indices)?,
                None => self.batch.clone(),
            };
            Ok(vec![batch])
        }
    }

    let schema = sales_schema();
    let (started_tx, started_rx) = oneshot::channel();
    let session = Arc::new(Session::new(SessionConfig {
        distributed: true,
        workers: 2,
        ..SessionConfig::default()
    }));
    session.register_table(
        "slow_sales",
        Arc::new(SlowTable {
            schema,
            batch: sales_batch(0),
            started: parking_lot::Mutex::new(Some(started_tx)),
        }),
    );
    let cancellation = CancellationToken::new();
    let query_cancellation = cancellation.clone();
    let query_session = session.clone();
    let query = tokio::spawn(async move {
        query_session
            .execute_sql_with_cancellation(
                "SELECT COUNT(*) AS rows FROM slow_sales",
                query_cancellation,
            )
            .await
    });

    started_rx.await.unwrap();
    cancellation.cancel();
    let error = query.await.unwrap().unwrap_err();
    assert!(matches!(error, SparkXError::Cancelled));
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

fn result_rows(result: &sparkx::QueryResult) -> Vec<Vec<ScalarValue>> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| value_at(column.as_ref(), row).unwrap())
                    .collect()
            })
        })
        .collect()
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

#[test]
fn plans_only_ordered_limits_as_topk() {
    let ordered = session(false)
        .explain("SELECT id FROM sales ORDER BY id DESC LIMIT 3")
        .unwrap();
    assert!(ordered.contains("TopKExec#0"));
    assert!(!ordered.contains("SortExec"));
    assert!(!ordered.contains("LimitExec"));

    let unordered = session(false)
        .explain("SELECT id FROM sales LIMIT 3")
        .unwrap();
    assert!(unordered.contains("LimitExec#0"));
    assert!(!unordered.contains("TopKExec"));
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
async fn executes_explicit_casts_and_preserves_nulls() {
    let result = session(false)
        .execute_sql(
            "SELECT CAST(id AS DOUBLE) AS id_double, \
                    CAST(amount AS BIGINT) AS amount_integer, \
                    CAST(id AS VARCHAR) AS id_text, \
                    CAST(NULL AS BOOLEAN) AS missing \
             FROM sales ORDER BY id_double LIMIT 1",
        )
        .await
        .unwrap();
    let batch = &result.batches[0];

    assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
    assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
    assert_eq!(batch.schema().field(2).data_type(), &DataType::Utf8);
    assert_eq!(batch.schema().field(3).data_type(), &DataType::Boolean);
    assert_eq!(
        value_at(batch.column(0).as_ref(), 0).unwrap(),
        ScalarValue::Float64(0.0)
    );
    assert_eq!(
        value_at(batch.column(1).as_ref(), 0).unwrap(),
        ScalarValue::Int64(10)
    );
    assert_eq!(
        value_at(batch.column(2).as_ref(), 0).unwrap(),
        ScalarValue::Utf8("0".to_owned())
    );
    assert_eq!(
        value_at(batch.column(3).as_ref(), 0).unwrap(),
        ScalarValue::Null
    );
}

#[tokio::test]
async fn null_arithmetic_propagates_and_invalid_implicit_coercions_are_rejected() {
    let result = session(false)
        .execute_sql("SELECT id + NULL AS missing FROM sales LIMIT 1")
        .await
        .unwrap();
    assert_eq!(
        result.batches[0].schema().field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Null
    );

    let error = session(false)
        .sql("SELECT amount + region FROM sales")
        .unwrap_err();
    assert!(
        matches!(error, SparkXError::Planning(message) if message.contains("requires numeric operands"))
    );

    let error = session(false)
        .sql("SELECT CAST(id AS DATE) FROM sales")
        .unwrap_err();
    assert!(matches!(error, SparkXError::Unsupported(message) if message.contains("DATE")));
}

#[tokio::test]
async fn optimizer_folds_constants_simplifies_booleans_and_propagates_nulls() {
    let result = session(false)
        .execute_sql(
            "SELECT 1 + 2 AS three, amount + NULL AS missing \
             FROM sales WHERE TRUE AND amount > (5 + 5) LIMIT 1",
        )
        .await
        .unwrap();

    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Int64(3)
    );
    assert_eq!(
        value_at(result.batches[0].column(1).as_ref(), 0).unwrap(),
        ScalarValue::Null
    );
    assert!(result.optimized_plan.contains("3 AS three"));
    assert!(
        result
            .optimized_plan
            .contains("CAST(NULL AS Float64) AS missing")
    );
    assert!(result.optimized_plan.contains("filters=[(#amount > 10)]"));
    assert!(!result.optimized_plan.contains("(5 + 5)"));
    assert!(!result.optimized_plan.contains("true AND"));
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
async fn encoded_multi_column_group_keys_match_across_runners() {
    fn keyed_session(distributed: bool) -> Session {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
            Field::new("amount", DataType::Float64, false),
        ]));
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some("a"), Some("a"), None])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(2)])),
                    Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])),
                ],
            )
            .unwrap(),
        ];
        let session = Session::new(SessionConfig {
            distributed,
            workers: 2,
            ..SessionConfig::default()
        });
        session.register_memory(
            "keyed",
            MemoryTable::new(
                schema,
                batches.into_iter().map(|batch| vec![batch]).collect(),
            )
            .unwrap(),
        );
        session
    }

    let sql = "SELECT category, code, COUNT(*) AS rows, SUM(amount) AS total \
               FROM keyed GROUP BY category, code";
    let native = keyed_session(false).execute_sql(sql).await.unwrap();
    let cluster = keyed_session(true).execute_sql(sql).await.unwrap();
    let rows = result_rows(&native);

    assert_eq!(result_rows(&cluster), rows);
    assert_eq!(rows.len(), 4);
    for expected in [
        vec![
            ScalarValue::Null,
            ScalarValue::Int64(2),
            ScalarValue::UInt64(2),
            ScalarValue::Float64(9.0),
        ],
        vec![
            ScalarValue::Utf8("a".to_owned()),
            ScalarValue::Int64(1),
            ScalarValue::UInt64(2),
            ScalarValue::Float64(3.0),
        ],
        vec![
            ScalarValue::Utf8("a".to_owned()),
            ScalarValue::Int64(2),
            ScalarValue::UInt64(1),
            ScalarValue::Float64(4.0),
        ],
        vec![
            ScalarValue::Utf8("b".to_owned()),
            ScalarValue::Int64(1),
            ScalarValue::UInt64(1),
            ScalarValue::Float64(5.0),
        ],
    ] {
        assert!(rows.contains(&expected));
    }
    assert!(cluster.distributed);
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

    let grouped = session
        .execute_sql("SELECT region, COUNT(*) AS rows FROM empty_sales GROUP BY region")
        .await
        .unwrap();
    assert_eq!(grouped.row_count(), 0);
    assert_eq!(grouped.batches.len(), 1);
    assert_eq!(grouped.batches[0].num_columns(), 2);
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
    assert!(cluster.metrics.shuffled_bytes > 0);
    assert_eq!(cluster.metrics.tasks, 4);
    assert_eq!(cluster.metrics.operators.len(), 2);
    assert_eq!(cluster.metrics.operators[0].operator_id, 0);
    assert_eq!(cluster.metrics.operators[0].name, "HashAggregate");
    assert_eq!(cluster.metrics.operators[0].output_rows, 2);
    assert_eq!(cluster.metrics.operators[1].operator_id, 1);
    assert_eq!(cluster.metrics.operators[1].name, "Scan");
}

#[tokio::test]
async fn local_cluster_matches_native_null_filter_semantics() {
    fn nullable_session(distributed: bool) -> Session {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]));
        let partitions = vec![
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(BooleanArray::from(vec![Some(true), Some(false)]))],
                )
                .unwrap(),
            ],
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(BooleanArray::from(vec![None, Some(true)]))],
                )
                .unwrap(),
            ],
        ];
        let session = Session::new(SessionConfig {
            distributed,
            workers: 2,
            ..SessionConfig::default()
        });
        session.register_memory("flags", MemoryTable::new(schema, partitions).unwrap());
        session
    }

    let sql = "SELECT COUNT(*) AS rows FROM flags WHERE flag OR NULL";
    let native = nullable_session(false).execute_sql(sql).await.unwrap();
    let cluster = nullable_session(true).execute_sql(sql).await.unwrap();
    let native_count = value_at(native.batches[0].column(0).as_ref(), 0).unwrap();
    let cluster_count = value_at(cluster.batches[0].column(0).as_ref(), 0).unwrap();

    assert_eq!(native_count, ScalarValue::UInt64(2));
    assert_eq!(cluster_count, native_count);
    assert!(cluster.distributed);
    assert_eq!(cluster.stages, 2);
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
async fn encoded_multi_column_join_keys_preserve_matches() {
    let session = session(false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("east"),
                Some("west"),
                Some("east"),
            ])),
            Arc::new(StringArray::from(vec!["east-10", "west-20", "null-key"])),
        ],
    )
    .unwrap();
    session.register_memory(
        "lookup",
        MemoryTable::new(schema, vec![vec![batch]]).unwrap(),
    );

    let result = session
        .execute_sql(
            "SELECT sales.id, lookup.label FROM sales JOIN lookup \
             ON sales.customer_id = lookup.customer_id AND sales.region = lookup.region \
             ORDER BY sales.id",
        )
        .await
        .unwrap();
    let rows = result_rows(&result);
    assert_eq!(rows.len(), 8);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row[0], ScalarValue::Int64(index as i64));
        assert_eq!(
            row[1],
            ScalarValue::Utf8(if index % 2 == 0 { "east-10" } else { "west-20" }.to_owned())
        );
    }
}

#[tokio::test]
async fn detects_ambiguous_join_columns_and_resolves_qualified_columns() {
    let session = session(false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    session.register_memory(
        "customers",
        MemoryTable::new(schema, vec![vec![batch]]).unwrap(),
    );

    let error = session
        .sql("SELECT id FROM sales JOIN customers ON sales.customer_id = customers.customer_id")
        .unwrap_err();
    assert!(matches!(error, SparkXError::Planning(message) if message.contains("ambiguous")));

    let error = session
        .sql("SELECT sales.id FROM sales JOIN customers ON customer_id = customer_id")
        .unwrap_err();
    assert!(matches!(error, SparkXError::Planning(message) if message.contains("ambiguous")));

    let result = session
        .execute_sql(
            "SELECT sales.id AS sale_id, customers.id AS customer_row_id \
             FROM sales JOIN customers \
             ON sales.customer_id = customers.customer_id \
             ORDER BY sale_id LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Int64(0)
    );
    assert_eq!(
        value_at(result.batches[0].column(1).as_ref(), 0).unwrap(),
        ScalarValue::Int64(100)
    );
}

#[tokio::test]
async fn table_aliases_define_the_visible_qualifier() {
    let session = session(false);
    let result = session
        .execute_sql("SELECT s.id FROM sales AS s ORDER BY s.id LIMIT 1")
        .await
        .unwrap();
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Int64(0)
    );

    let error = session.sql("SELECT sales.id FROM sales AS s").unwrap_err();
    assert!(matches!(error, SparkXError::Planning(message) if message.contains("does not exist")));
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
async fn implements_sql_three_valued_boolean_logic() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "flag",
        DataType::Boolean,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
        ]))],
    )
    .unwrap();
    let session = Session::new(SessionConfig::default());
    session.register_memory(
        "flags",
        MemoryTable::new(schema, vec![vec![batch]]).unwrap(),
    );

    let result = session
        .execute_sql(
            "SELECT flag, flag AND NULL AS and_unknown, flag OR NULL AS or_unknown, \
             flag IS NULL AS missing, flag IS NOT NULL AS present FROM flags",
        )
        .await
        .unwrap();
    let batch = &result.batches[0];
    let expected = [
        [
            ScalarValue::Boolean(true),
            ScalarValue::Null,
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(true),
        ],
        [
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(false),
            ScalarValue::Null,
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(true),
        ],
        [
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
        ],
    ];
    for (row, expected_row) in expected.iter().enumerate() {
        for (column, expected_value) in expected_row.iter().enumerate() {
            assert_eq!(
                value_at(batch.column(column).as_ref(), row).unwrap(),
                *expected_value
            );
        }
    }

    let filtered = session
        .execute_sql("SELECT flag FROM flags WHERE flag OR NULL")
        .await
        .unwrap();
    assert_eq!(filtered.row_count(), 1);
    assert_eq!(
        value_at(filtered.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Boolean(true)
    );

    let missing = session
        .execute_sql("SELECT flag FROM flags WHERE flag IS NULL")
        .await
        .unwrap();
    assert_eq!(missing.row_count(), 1);
    assert_eq!(
        value_at(missing.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Null
    );
}

#[tokio::test]
async fn null_comparisons_produce_unknown() {
    let session = session(false);
    let result = session
        .execute_sql("SELECT id = NULL AS comparison FROM sales LIMIT 1")
        .await
        .unwrap();
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Null
    );

    let filtered = session
        .execute_sql("SELECT id FROM sales WHERE id = NULL")
        .await
        .unwrap();
    assert_eq!(filtered.row_count(), 0);
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
async fn single_partition_csv_safely_falls_back_to_native() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sales.csv");
    std::fs::write(&path, "id,region,amount\n1,east,12.5\n2,west,7.5\n").unwrap();
    let session = Session::new(SessionConfig {
        distributed: true,
        workers: 2,
        ..SessionConfig::default()
    });
    session.register_csv("sales", &path).unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) AS rows FROM sales")
        .await
        .unwrap();

    assert!(!result.distributed);
    assert_eq!(result.stages, 1);
    assert_eq!(result.metrics.tasks, 1);
    assert_eq!(result.metrics.shuffled_rows, 0);
    assert_eq!(result.metrics.shuffled_bytes, 0);
    assert_eq!(
        value_at(result.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::UInt64(2)
    );
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
    assert!(result.distributed);
    assert_eq!(result.stages, 2);
    assert_eq!(result.metrics.tasks, 2);
    assert_eq!(result.metrics.shuffled_rows, 2);
}

#[tokio::test]
async fn prunes_parquet_row_groups_with_column_statistics() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pruned-sales.parquet");
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, sales_schema(), None).unwrap();
    for offset in [0, 2, 4] {
        writer.write(&sales_batch(offset)).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    let session = Session::new(SessionConfig::default());
    session.register_parquet("sales", &path).unwrap();

    let result = session
        .execute_sql("SELECT id FROM sales WHERE id >= 4 ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.metrics.input_rows, 2);
    assert_eq!(result.metrics.tasks, 1);
    assert_eq!(result.metrics.pruned_partitions, 2);

    let reversed = session
        .execute_sql("SELECT id FROM sales WHERE 4 <= id")
        .await
        .unwrap();
    assert_eq!(reversed.row_count(), 2);
    assert_eq!(reversed.metrics.pruned_partitions, 2);

    let impossible = session
        .execute_sql("SELECT id FROM sales WHERE id < 0 OR id > 100")
        .await
        .unwrap();
    assert_eq!(impossible.row_count(), 0);
    assert_eq!(impossible.metrics.input_rows, 0);
    assert_eq!(impossible.metrics.tasks, 0);
    assert_eq!(impossible.metrics.pruned_partitions, 3);

    let impossible_string = session
        .execute_sql("SELECT id FROM sales WHERE region = 'zzz'")
        .await
        .unwrap();
    assert_eq!(impossible_string.row_count(), 0);
    assert_eq!(impossible_string.metrics.pruned_partitions, 3);

    let impossible_null = session
        .execute_sql("SELECT id FROM sales WHERE id IS NULL")
        .await
        .unwrap();
    assert_eq!(impossible_null.row_count(), 0);
    assert_eq!(impossible_null.metrics.pruned_partitions, 3);

    let distributed = Session::new(SessionConfig {
        distributed: true,
        workers: 2,
        ..SessionConfig::default()
    });
    distributed.register_parquet("sales", &path).unwrap();
    let aggregate = distributed
        .execute_sql("SELECT COUNT(*) AS rows FROM sales WHERE id >= 4")
        .await
        .unwrap();
    assert_eq!(
        value_at(aggregate.batches[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::UInt64(2)
    );
    assert!(aggregate.distributed);
    assert_eq!(aggregate.metrics.tasks, 1);
    assert_eq!(aggregate.metrics.pruned_partitions, 2);
}

#[tokio::test]
async fn prunes_parquet_row_groups_with_null_counts() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nullable.parquet");
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    for values in [
        vec![None, None],
        vec![Some(1), None],
        vec![Some(2), Some(3)],
    ] {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    let session = Session::new(SessionConfig::default());
    session.register_parquet("nullable", &path).unwrap();

    let nulls = session
        .execute_sql("SELECT value FROM nullable WHERE value IS NULL")
        .await
        .unwrap();
    assert_eq!(nulls.row_count(), 3);
    assert_eq!(nulls.metrics.input_rows, 4);
    assert_eq!(nulls.metrics.pruned_partitions, 1);

    let values = session
        .execute_sql("SELECT value FROM nullable WHERE value IS NOT NULL")
        .await
        .unwrap();
    assert_eq!(values.row_count(), 3);
    assert_eq!(values.metrics.input_rows, 4);
    assert_eq!(values.metrics.pruned_partitions, 1);
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

#[tokio::test]
async fn physical_operator_ids_and_metrics_are_stable() {
    let sql = "SELECT id, amount FROM sales WHERE amount >= 22 ORDER BY id LIMIT 2";
    let first = session(false).execute_sql(sql).await.unwrap();
    let second = session(false).execute_sql(sql).await.unwrap();

    assert_eq!(first.physical_plan, second.physical_plan);
    assert!(first.physical_plan.contains("TopKExec#0"));
    assert!(first.physical_plan.contains("ProjectionExec#1"));
    assert!(first.physical_plan.contains("ScanExec#2"));

    let operators = first
        .metrics
        .operators
        .iter()
        .map(|operator| (operator.operator_id, operator.name.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(operators, vec![(0, "TopK"), (1, "Projection"), (2, "Scan")]);
    assert!(
        first
            .metrics
            .operators
            .iter()
            .all(|operator| operator.output_batches > 0)
    );
    assert_eq!(first.metrics.operators[0].output_rows, 2);
}

#[test]
fn rejects_unknown_columns_during_planning() {
    let error = session(false).sql("SELECT missing FROM sales").unwrap_err();
    assert!(matches!(error, SparkXError::Planning(_)));
}
