use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use sparkx::catalog::MemoryTable;
use sparkx::expr::{Expr, Operator, ScalarValue, evaluate};
use sparkx::{Session, SessionConfig};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

const ROWS_PER_BATCH: usize = 32_768;
const PARTITIONS: usize = 8;

fn sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn sales_batch(offset: usize, rows: usize) -> RecordBatch {
    let schema = sales_schema();
    let id = Int64Array::from_iter_values((offset..offset + rows).map(|value| value as i64));
    let region = StringArray::from_iter_values(
        (offset..offset + rows).map(|value| ["north", "south", "east", "west"][value % 4]),
    );
    let customer_id =
        Int64Array::from_iter_values((offset..offset + rows).map(|value| (value % 4_096) as i64));
    let amount = Float64Array::from_iter_values(
        (offset..offset + rows).map(|value| ((value * 17) % 10_000) as f64 / 100.0),
    );
    let active = BooleanArray::from(
        (offset..offset + rows)
            .map(|value| value % 5 != 0)
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id) as ArrayRef,
            Arc::new(region),
            Arc::new(customer_id),
            Arc::new(amount),
            Arc::new(active),
        ],
    )
    .expect("benchmark fixture must be valid")
}

fn sales_partitions() -> Vec<Vec<RecordBatch>> {
    (0..PARTITIONS)
        .map(|partition| vec![sales_batch(partition * ROWS_PER_BATCH, ROWS_PER_BATCH)])
        .collect()
}

fn customer_table() -> MemoryTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("tier", DataType::Utf8, false),
    ]));
    let customer_id = Int64Array::from_iter_values(0..4_096_i64);
    let tier = StringArray::from_iter_values(
        (0..4_096).map(|value| ["free", "standard", "premium"][value % 3]),
    );
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(customer_id), Arc::new(tier)])
        .expect("benchmark fixture must be valid");
    MemoryTable::new(schema, vec![vec![batch]]).expect("benchmark fixture must be valid")
}

fn session(distributed: bool) -> Session {
    let session = Session::new(SessionConfig {
        batch_size: ROWS_PER_BATCH,
        channel_capacity: 2,
        workers: 4,
        distributed,
        ..SessionConfig::default()
    });
    session.register_memory(
        "sales",
        MemoryTable::new(sales_schema(), sales_partitions())
            .expect("benchmark fixture must be valid"),
    );
    session.register_memory("customers", customer_table());
    session
}

fn vectorized_expression(c: &mut Criterion) {
    let batch = sales_batch(0, ROWS_PER_BATCH);
    let predicate = Expr::binary(
        Expr::column("amount"),
        Operator::Gt,
        Expr::literal(ScalarValue::Float64(50.0)),
    );
    let mut group = c.benchmark_group("vectorized_expression");
    group.throughput(Throughput::Elements(ROWS_PER_BATCH as u64));
    group.bench_function("amount_gt_50", |bencher| {
        bencher.iter(|| evaluate(black_box(&predicate), black_box(&batch)).unwrap())
    });
    group.finish();
}

fn query_pipeline(c: &mut Criterion) {
    let runtime = Runtime::new().expect("Tokio runtime");
    let native = session(false);
    let distributed = session(true);
    let total_rows = (ROWS_PER_BATCH * PARTITIONS) as u64;
    let cases = [
        (
            "scan_filter_project",
            "SELECT id, amount * 1.2 AS gross FROM sales WHERE active = true AND amount > 50",
        ),
        (
            "hash_aggregate",
            "SELECT region, COUNT(*) AS rows, SUM(amount) AS revenue, AVG(amount) AS average FROM sales GROUP BY region",
        ),
        (
            "topk_sort",
            "SELECT id, amount FROM sales ORDER BY amount DESC LIMIT 100",
        ),
        (
            "hash_join",
            "SELECT sales.id, customers.tier, sales.amount FROM sales JOIN customers ON sales.customer_id = customers.customer_id WHERE sales.amount > 50",
        ),
    ];

    let mut group = c.benchmark_group("native_queries");
    group.sample_size(20);
    group.throughput(Throughput::Elements(total_rows));
    for (name, sql) in cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &sql, |bencher, sql| {
            bencher.iter(|| {
                let result = runtime
                    .block_on(native.execute_sql(black_box(sql)))
                    .expect("benchmark query");
                black_box(result.row_count())
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("local_cluster");
    group.sample_size(20);
    group.throughput(Throughput::Elements(total_rows));
    let sql = "SELECT region, COUNT(*) AS rows, SUM(amount) AS revenue, AVG(amount) AS average FROM sales GROUP BY region";
    group.bench_function("two_stage_hash_aggregate", |bencher| {
        bencher.iter(|| {
            let result = runtime
                .block_on(distributed.execute_sql(black_box(sql)))
                .expect("benchmark query");
            black_box(result.row_count())
        })
    });
    group.finish();
}

criterion_group!(benches, vectorized_expression, query_pipeline);
criterion_main!(benches);
