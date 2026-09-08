#[test]
fn registers_an_additional_table_for_join_queries() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sparkx"))
        .current_dir(root)
        .args(["--input", "examples/data/orders.csv", "--table", "orders", "--register", "customers=examples/data/customers.csv", "--sql", "SELECT orders.order_id, customers.name FROM orders LEFT JOIN customers ON orders.customer_id = customers.customer_id"])
        .output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("Ada") && text.contains("Grace"));
}
