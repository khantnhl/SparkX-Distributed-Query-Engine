use arrow::datatypes::{DataType, Field, Schema};
use sparkx::catalog::{Catalog, MemoryTable};
use sparkx::cluster::join::plan_remote_join;
use sparkx::execution::PhysicalPlan;
use sparkx::expr::Expr;
use sparkx::logical::JoinType;
use sparkx::protocol::{QueryId, StageId};
use std::sync::Arc;

fn scan(name: &str, partitions: usize, data_type: DataType) -> Arc<PhysicalPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        format!("{name}.id"),
        data_type,
        true,
    )]));
    Arc::new(PhysicalPlan::Scan {
        id: if name == "a" { 1 } else { 2 },
        table_name: name.into(),
        provider: Arc::new(MemoryTable::new(schema.clone(), vec![vec![]; partitions]).unwrap()),
        projection: None,
        filters: vec![],
        schema,
    })
}
fn join(right_type: DataType) -> PhysicalPlan {
    let left = scan("a", 2, DataType::Int64);
    let right = scan("b", 3, right_type);
    PhysicalPlan::HashJoin {
        id: 0,
        schema: Arc::new(Schema::new(vec![
            left.schema().field(0).clone(),
            right.schema().field(0).clone(),
        ])),
        left,
        right,
        join_type: JoinType::Inner,
        left_on: vec![Expr::column("a.id")],
        right_on: vec![Expr::column("b.id")],
    }
}
#[test]
fn plans_deterministic_three_stage_join_and_decodes_both_inputs() {
    let plan = join(DataType::Int64);
    let query = QueryId::new("join-plan").unwrap();
    let stages = plan_remote_join(&plan, query.clone()).unwrap().unwrap();
    assert_eq!(stages, plan_remote_join(&plan, query).unwrap().unwrap());
    assert_eq!(
        stages.iter().map(|s| s.partition_count).collect::<Vec<_>>(),
        vec![2, 3, 3]
    );
    assert_eq!(stages[2].input_stages, vec![StageId(0), StageId(1)]);
    assert!(stages[2].partitioned_input);
    for stage in &stages[..2] {
        let exchange = stage.output_exchange.as_ref().unwrap();
        assert_eq!(exchange.partition_count, 3);
        assert_eq!(exchange.columns, vec![0]);
    }
    let PhysicalPlan::HashJoin { left, right, .. } = &plan else {
        unreachable!()
    };
    let catalog = Catalog::default();
    for (index, input) in [left, right].iter().enumerate() {
        catalog.register(
            format!("__sparkx_stage_input_{index}"),
            Arc::new(MemoryTable::new(input.schema(), vec![vec![]]).unwrap()),
        );
    }
    let decoded = stages[2].decode_physical_plan(&catalog).unwrap();
    assert_eq!(decoded.schema(), plan.schema());
    assert!(decoded.explain().contains("__sparkx_stage_input_0"));
    assert!(decoded.explain().contains("__sparkx_stage_input_1"));
}
#[test]
fn rejects_incompatible_keys_and_global_or_chained_join_shapes() {
    let query = || QueryId::new("bad-join").unwrap();
    assert!(plan_remote_join(&join(DataType::Utf8), query()).is_err());
    let input = Arc::new(join(DataType::Int64));
    let limit = PhysicalPlan::Limit {
        id: 9,
        input: input.clone(),
        limit: 1,
        schema: input.schema(),
    };
    assert!(plan_remote_join(&limit, query()).is_err());
    let mut chained = join(DataType::Int64);
    if let PhysicalPlan::HashJoin { left, .. } = &mut chained {
        *left = input;
    }
    assert!(plan_remote_join(&chained, query()).is_err());
    let mut expression = join(DataType::Int64);
    if let PhysicalPlan::HashJoin { left_on, .. } = &mut expression {
        *left_on = vec![Expr::cast(Expr::column("a.id"), DataType::Float64)];
    }
    assert!(plan_remote_join(&expression, query()).is_err());
}
