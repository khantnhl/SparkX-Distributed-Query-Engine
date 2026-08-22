use arrow::datatypes::{DataType, Field, Schema};
use sparkx::catalog::MemoryTable;
use sparkx::{Session, SessionConfig};
use std::sync::Arc;

fn snapshot_session() -> Session {
    let session = Session::new(SessionConfig::default());
    let sales_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let customers_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    session.register_memory("sales", MemoryTable::new(sales_schema, Vec::new()).unwrap());
    session.register_memory(
        "customers",
        MemoryTable::new(customers_schema, Vec::new()).unwrap(),
    );
    session
}

fn assert_snapshot(name: &str, actual: &str, expected: &str) {
    let actual = actual.replace("\r\n", "\n");
    let expected = expected.replace("\r\n", "\n");
    assert_eq!(
        actual.trim_end(),
        expected.trim_end(),
        "explain snapshot {name} changed"
    );
}

#[test]
fn filter_sort_limit_plan_matches_snapshot() {
    let actual = snapshot_session()
        .explain("SELECT id, amount FROM sales WHERE amount >= 22 ORDER BY id DESC LIMIT 2")
        .unwrap();
    assert_snapshot(
        "filter_sort_limit",
        &actual,
        include_str!("snapshots/filter_sort_limit.plan"),
    );
}

#[test]
fn grouped_aggregate_plan_matches_snapshot() {
    let actual = snapshot_session()
        .explain(
            "SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue \
             FROM sales WHERE amount > 10 GROUP BY region",
        )
        .unwrap();
    assert_snapshot(
        "grouped_aggregate",
        &actual,
        include_str!("snapshots/grouped_aggregate.plan"),
    );
}

#[test]
fn inner_join_plan_matches_snapshot() {
    let actual = snapshot_session()
        .explain(
            "SELECT s.id, c.name FROM sales AS s \
             JOIN customers AS c ON s.customer_id = c.id WHERE s.amount > 10",
        )
        .unwrap();
    assert_snapshot(
        "inner_join",
        &actual,
        include_str!("snapshots/inner_join.plan"),
    );
}
