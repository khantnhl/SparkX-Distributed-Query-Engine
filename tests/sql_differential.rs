use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use duckdb::types::ValueRef;
use sparkx::catalog::MemoryTable;
use sparkx::expr::{ScalarValue, value_at};
use sparkx::{Session, SessionConfig};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Null,
    Boolean(bool),
    Integer(i128),
    Float(String),
    Text(String),
}

#[derive(Debug)]
struct DifferentialCase<'a> {
    name: &'a str,
    sql: &'a str,
}

fn corpus() -> Vec<DifferentialCase<'static>> {
    include_str!("sql/differential.sql")
        .split("-- name: ")
        .skip(1)
        .map(|case| {
            let (name, sql) = case
                .split_once('\n')
                .expect("each differential case needs a name and SQL body");
            DifferentialCase {
                name: name.trim(),
                sql: sql.trim().trim_end_matches(';'),
            }
        })
        .collect()
}

fn sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}

fn sales_batch(
    ids: Vec<i64>,
    regions: Vec<&str>,
    customer_ids: Vec<i64>,
    amounts: Vec<Option<f64>>,
) -> RecordBatch {
    RecordBatch::try_new(
        sales_schema(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(regions)),
            Arc::new(Int64Array::from(customer_ids)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap()
}

fn sparkx_session(distributed: bool) -> Session {
    let session = Session::new(SessionConfig {
        distributed,
        workers: 2,
        ..SessionConfig::default()
    });
    session.register_memory(
        "sales",
        MemoryTable::new(
            sales_schema(),
            vec![
                vec![sales_batch(
                    vec![1, 2],
                    vec!["east", "west"],
                    vec![10, 20],
                    vec![Some(12.0), Some(30.0)],
                )],
                vec![sales_batch(
                    vec![3, 4],
                    vec!["east", "north"],
                    vec![10, 30],
                    vec![None, Some(7.0)],
                )],
                vec![sales_batch(
                    vec![5, 6],
                    vec!["west", "east"],
                    vec![20, 99],
                    vec![Some(50.0), Some(18.0)],
                )],
            ],
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
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Ada", "Ben", "Cora", "Drew"])),
        ],
    )
    .unwrap();
    session.register_memory(
        "customers",
        MemoryTable::new(customers_schema, vec![vec![customers]]).unwrap(),
    );
    session
}

fn duckdb_connection() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            r#"
            CREATE TABLE sales (
                id BIGINT NOT NULL,
                region VARCHAR NOT NULL,
                customer_id BIGINT NOT NULL,
                amount DOUBLE
            );
            INSERT INTO sales VALUES
                (1, 'east', 10, 12.0),
                (2, 'west', 20, 30.0),
                (3, 'east', 10, NULL),
                (4, 'north', 30, 7.0),
                (5, 'west', 20, 50.0),
                (6, 'east', 99, 18.0);

            CREATE TABLE customers (id BIGINT NOT NULL, name VARCHAR NOT NULL);
            INSERT INTO customers VALUES
                (10, 'Ada'),
                (20, 'Ben'),
                (30, 'Cora'),
                (40, 'Drew');
            "#,
        )
        .unwrap();
    connection
}

fn normalize_float(value: f64) -> String {
    let value = if value == 0.0 { 0.0 } else { value };
    let mut formatted = format!("{value:.9}");
    while formatted.contains('.') && formatted.ends_with('0') {
        formatted.pop();
    }
    if formatted.ends_with('.') {
        formatted.pop();
    }
    formatted
}

fn sparkx_cell(value: ScalarValue) -> Cell {
    match value {
        ScalarValue::Null => Cell::Null,
        ScalarValue::Boolean(value) => Cell::Boolean(value),
        ScalarValue::Int64(value) => Cell::Integer(value as i128),
        ScalarValue::UInt64(value) => Cell::Integer(value as i128),
        ScalarValue::Float64(value) => Cell::Float(normalize_float(value)),
        ScalarValue::Utf8(value) => Cell::Text(value),
    }
}

fn duckdb_cell(value: ValueRef<'_>) -> Cell {
    match value {
        ValueRef::Null => Cell::Null,
        ValueRef::Boolean(value) => Cell::Boolean(value),
        ValueRef::TinyInt(value) => Cell::Integer(value as i128),
        ValueRef::SmallInt(value) => Cell::Integer(value as i128),
        ValueRef::Int(value) => Cell::Integer(value as i128),
        ValueRef::BigInt(value) => Cell::Integer(value as i128),
        ValueRef::HugeInt(value) => Cell::Integer(value),
        ValueRef::UTinyInt(value) => Cell::Integer(value as i128),
        ValueRef::USmallInt(value) => Cell::Integer(value as i128),
        ValueRef::UInt(value) => Cell::Integer(value as i128),
        ValueRef::UBigInt(value) => Cell::Integer(value as i128),
        ValueRef::UHugeInt(value) => Cell::Integer(
            i128::try_from(value).expect("differential integer exceeds SparkX's scalar domain"),
        ),
        ValueRef::Float(value) => Cell::Float(normalize_float(value as f64)),
        ValueRef::Double(value) => Cell::Float(normalize_float(value)),
        ValueRef::Text(value) => Cell::Text(String::from_utf8_lossy(value).into_owned()),
        other => panic!("unsupported DuckDB differential value: {other:?}"),
    }
}

async fn sparkx_rows(session: &Session, sql: &str) -> Vec<Vec<Cell>> {
    let result = session.execute_sql(sql).await.unwrap();
    let mut rows = result
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| sparkx_cell(value_at(column.as_ref(), row).unwrap()))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn duckdb_rows(connection: &Connection, sql: &str) -> Vec<Vec<Cell>> {
    let mut statement = connection.prepare(sql).unwrap();
    let mut result = statement.query([]).unwrap();
    let column_count = result
        .as_ref()
        .expect("DuckDB query should expose its result columns")
        .column_count();
    let mut rows = Vec::new();
    while let Some(row) = result.next().unwrap() {
        rows.push(
            (0..column_count)
                .map(|column| duckdb_cell(row.get_ref(column).unwrap()))
                .collect::<Vec<_>>(),
        );
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn sql_corpus_matches_duckdb_in_native_and_distributed_modes() {
    let native = sparkx_session(false);
    let distributed = sparkx_session(true);
    let duckdb = duckdb_connection();

    for case in corpus() {
        let expected = duckdb_rows(&duckdb, case.sql);
        assert_eq!(
            sparkx_rows(&native, case.sql).await,
            expected,
            "native result differed from DuckDB for {}",
            case.name
        );
        assert_eq!(
            sparkx_rows(&distributed, case.sql).await,
            expected,
            "distributed result differed from DuckDB for {}",
            case.name
        );
    }
}
