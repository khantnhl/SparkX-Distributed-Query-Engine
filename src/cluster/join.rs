//! Three-stage remote equi-join planning. Both producers use the same hash contract.
use crate::catalog::MemoryTable;
use crate::execution::PhysicalPlan;
use crate::expr::{Expr, find_column};
use crate::protocol::{HashExchange, QueryId, StageId, StagePlan, stage_input_table_name};
use crate::{Result, SparkXError};
use std::sync::Arc;

/// Returns no graph for a partition-local query or aggregate. Unsupported join shapes fail here,
/// before the caller submits any stages to the coordinator.
pub fn plan_remote_join(plan: &PhysicalPlan, query_id: QueryId) -> Result<Option<Vec<StagePlan>>> {
    if !contains_join(plan) {
        return Ok(None);
    }
    let mut producers = Vec::new();
    let downstream = rewrite(plan, &query_id, &mut producers)?;
    let partitions = producers[0]
        .output_exchange
        .as_ref()
        .expect("join producer exchange")
        .partition_count;
    let mut final_stage = StagePlan::from_physical_plan(
        query_id,
        StageId(2),
        vec![StageId(0), StageId(1)],
        partitions,
        &downstream,
    )?;
    final_stage.partitioned_input = true;
    producers.push(final_stage);
    Ok(Some(producers))
}

fn rewrite(
    plan: &PhysicalPlan,
    query: &QueryId,
    stages: &mut Vec<StagePlan>,
) -> Result<PhysicalPlan> {
    Ok(match plan {
        PhysicalPlan::Projection {
            id,
            input,
            exprs,
            schema,
        } => PhysicalPlan::Projection {
            id: *id,
            input: Arc::new(rewrite(input, query, stages)?),
            exprs: exprs.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::Filter {
            id,
            input,
            predicate,
            schema,
        } => PhysicalPlan::Filter {
            id: *id,
            input: Arc::new(rewrite(input, query, stages)?),
            predicate: predicate.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::HashJoin {
            id,
            left,
            right,
            join_type,
            left_on,
            right_on,
            schema,
        } => {
            let left_count = local_partitions(left)?;
            let right_count = local_partitions(right)?;
            let partitions = left_count.max(right_count);
            if left_on.is_empty() || left_on.len() != right_on.len() {
                return Err(SparkXError::planning(
                    "remote join requires matching nonempty equi-join keys",
                ));
            }
            let mut left_columns = Vec::new();
            let mut right_columns = Vec::new();
            for (left_key, right_key) in left_on.iter().zip(right_on) {
                let (Expr::Column(left_name), Expr::Column(right_name)) = (left_key, right_key)
                else {
                    return Err(SparkXError::unsupported(
                        "remote join keys must be columns; key expressions are not supported",
                    ));
                };
                let l = find_column(&left.schema(), left_name)?;
                let r = find_column(&right.schema(), right_name)?;
                if left.schema().field(l).data_type() != right.schema().field(r).data_type() {
                    return Err(SparkXError::planning(
                        "remote join key types must match on both inputs",
                    ));
                }
                left_columns.push(l);
                right_columns.push(r);
            }
            for (stage_id, input, count, columns) in [
                (StageId(0), left, left_count, left_columns),
                (StageId(1), right, right_count, right_columns),
            ] {
                let mut stage =
                    StagePlan::from_physical_plan(query.clone(), stage_id, vec![], count, input)?;
                stage.output_exchange = Some(HashExchange {
                    columns,
                    partition_count: partitions,
                });
                stages.push(stage);
            }
            PhysicalPlan::HashJoin {
                id: *id,
                left: dependency_scan(left, StageId(0))?,
                right: dependency_scan(right, StageId(1))?,
                join_type: *join_type,
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                schema: schema.clone(),
            }
        }
        _ => {
            return Err(SparkXError::unsupported(
                "remote joins support one inner/left equi-join with scan/filter/projection inputs and optional filter/projection output; chained joins, aggregation, sorting, and limits are not supported",
            ));
        }
    })
}

fn dependency_scan(input: &PhysicalPlan, stage: StageId) -> Result<Arc<PhysicalPlan>> {
    let schema = input.schema();
    Ok(Arc::new(PhysicalPlan::Scan {
        id: input.id(),
        table_name: stage_input_table_name(stage),
        provider: Arc::new(MemoryTable::new(schema.clone(), vec![vec![]])?),
        projection: None,
        filters: vec![],
        schema,
    }))
}

fn local_partitions(plan: &PhysicalPlan) -> Result<u32> {
    match plan {
        PhysicalPlan::Scan { provider, .. } => u32::try_from(provider.partition_count())
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(|| SparkXError::planning("remote join scan partition count is invalid")),
        PhysicalPlan::Projection { input, .. } | PhysicalPlan::Filter { input, .. } => {
            local_partitions(input)
        }
        _ => Err(SparkXError::unsupported(
            "remote join inputs must be partition-local scans, filters, or projections",
        )),
    }
}

fn contains_join(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::HashJoin { .. } => true,
        PhysicalPlan::Scan { .. } => false,
        PhysicalPlan::Projection { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopK { input, .. }
        | PhysicalPlan::Limit { input, .. } => contains_join(input),
    }
}
