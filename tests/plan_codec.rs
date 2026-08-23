use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sparkx::catalog::{Catalog, MemoryTable};
use sparkx::execution::{PhysicalPlan, TaskContext, execute};
use sparkx::metrics::QueryMetrics;
use sparkx::optimizer::Optimizer;
use sparkx::plan_codec::{MAX_PLAN_FRAGMENT_BYTES, PhysicalPlanCodec};
use sparkx::planner::PhysicalPlanner;
use sparkx::protocol::{QueryId, StageId, StagePlan};
use sparkx::{CancellationToken, QueryMemory, Session, SessionConfig, SparkXError};
use std::sync::Arc;

fn test_session() -> Session {
    let session = Session::new(SessionConfig::default());
    let sales_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let sales = RecordBatch::try_new(
        sales_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["east", "west", "east", "west"])),
            Arc::new(Int64Array::from(vec![10, 20, 10, 20])),
            Arc::new(Float64Array::from(vec![12.0, 8.0, 18.0, 72.0])),
        ],
    )
    .unwrap();
    session.register_memory("sales", MemoryTable::from_batches(vec![sales], 2).unwrap());

    let customers_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
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
    session.register_memory(
        "customers",
        MemoryTable::new(customers_schema, vec![vec![customers]]).unwrap(),
    );
    session
}

fn physical_plan(session: &Session, sql: &str) -> Arc<PhysicalPlan> {
    let optimized = Optimizer.optimize(session.sql(sql).unwrap()).unwrap();
    PhysicalPlanner::create_physical_plan(&optimized, session.catalog()).unwrap()
}

async fn execute_plan(plan: Arc<PhysicalPlan>) -> Vec<RecordBatch> {
    execute(
        plan,
        TaskContext {
            batch_size: 1_024,
            channel_capacity: 2,
            partition: None,
            metrics: Arc::new(QueryMetrics::default()),
            memory: QueryMemory::new(16 * 1024 * 1024),
            cancellation: CancellationToken::new(),
        },
    )
    .collect()
    .await
    .unwrap()
}

#[tokio::test]
async fn round_trips_and_executes_every_physical_operator_shape() {
    let session = test_session();
    let queries = [
        "SELECT id, amount + 1 AS adjusted FROM sales WHERE amount > 10 ORDER BY adjusted DESC LIMIT 2",
        "SELECT region, SUM(amount) AS revenue FROM sales GROUP BY region HAVING revenue > 1 ORDER BY revenue DESC",
        "SELECT id FROM sales LIMIT 2",
        "SELECT sales.id, customers.name FROM sales LEFT JOIN customers ON sales.customer_id = customers.customer_id ORDER BY sales.id",
    ];

    for sql in queries {
        let original = physical_plan(&session, sql);
        let bytes = PhysicalPlanCodec::encode(original.as_ref()).unwrap();
        PhysicalPlanCodec::validate_fragment(&bytes).unwrap();
        let decoded = PhysicalPlanCodec::decode(&bytes, session.catalog()).unwrap();

        assert_eq!(decoded.explain(), original.explain(), "query: {sql}");
        assert_eq!(decoded.schema(), original.schema(), "query: {sql}");
        assert_eq!(
            execute_plan(decoded).await,
            execute_plan(original).await,
            "query: {sql}"
        );
    }
}

#[test]
fn stage_plan_owns_a_valid_decodable_fragment() {
    let session = test_session();
    let physical = physical_plan(&session, "SELECT id FROM sales WHERE amount > 10");
    let stage = StagePlan::from_physical_plan(
        QueryId::new("query-codec").unwrap(),
        StageId(1),
        Vec::new(),
        2,
        physical.as_ref(),
    )
    .unwrap();

    stage.validate().unwrap();
    let json = serde_json::to_string(&stage).unwrap();
    let decoded_stage: StagePlan = serde_json::from_str(&json).unwrap();
    let decoded_plan = decoded_stage
        .decode_physical_plan(session.catalog())
        .unwrap();

    assert_eq!(decoded_plan.explain(), physical.explain());
}

#[test]
fn rejects_corrupt_versions_unknown_tables_and_schema_drift() {
    let session = test_session();
    let physical = physical_plan(&session, "SELECT id FROM sales");
    let bytes = PhysicalPlanCodec::encode(physical.as_ref()).unwrap();

    assert!(matches!(
        PhysicalPlanCodec::validate_fragment(&[]),
        Err(SparkXError::Protocol(_))
    ));
    assert!(matches!(
        PhysicalPlanCodec::validate_fragment(&bytes[..bytes.len() / 2]),
        Err(SparkXError::Protocol(_))
    ));

    let mut wrong_version = bytes.clone();
    assert_eq!(wrong_version[0], 8);
    wrong_version[1] = wrong_version[1].saturating_add(1);
    assert!(matches!(
        PhysicalPlanCodec::validate_fragment(&wrong_version),
        Err(SparkXError::Protocol(_))
    ));

    assert!(matches!(
        PhysicalPlanCodec::decode(&bytes, &Catalog::default()),
        Err(SparkXError::Protocol(_))
    ));

    let drifted_catalog = Catalog::default();
    let drifted_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    drifted_catalog.register(
        "sales",
        Arc::new(MemoryTable::new(drifted_schema, vec![Vec::new()]).unwrap()),
    );
    assert!(matches!(
        PhysicalPlanCodec::decode(&bytes, &drifted_catalog),
        Err(SparkXError::Protocol(_))
    ));
}

#[test]
fn enforces_fragment_size_depth_and_supported_types() {
    assert!(matches!(
        PhysicalPlanCodec::validate_fragment(&vec![0; MAX_PLAN_FRAGMENT_BYTES + 1]),
        Err(SparkXError::Protocol(_))
    ));

    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Binary,
        false,
    )]));
    let provider = Arc::new(MemoryTable::new(schema.clone(), vec![Vec::new()]).unwrap());
    let unsupported = PhysicalPlan::Scan {
        id: 0,
        table_name: "binary_input".to_owned(),
        provider,
        projection: None,
        filters: Vec::new(),
        schema: schema.clone(),
    };
    assert!(matches!(
        PhysicalPlanCodec::encode(&unsupported),
        Err(SparkXError::Protocol(_))
    ));

    let base_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let base_provider = Arc::new(MemoryTable::new(base_schema.clone(), vec![Vec::new()]).unwrap());
    let mut deep = Arc::new(PhysicalPlan::Scan {
        id: 0,
        table_name: "deep".to_owned(),
        provider: base_provider,
        projection: None,
        filters: Vec::new(),
        schema: base_schema.clone(),
    });
    for id in 1..=128 {
        deep = Arc::new(PhysicalPlan::Limit {
            id,
            input: deep,
            limit: 1,
            schema: base_schema.clone(),
        });
    }
    assert!(matches!(
        PhysicalPlanCodec::encode(deep.as_ref()),
        Err(SparkXError::Protocol(_))
    ));
}
