use crate::catalog::Catalog;
use crate::error::Result;
use crate::execution::{OperatorId, PhysicalPlan};
use crate::expr::find_column;
use crate::logical::LogicalPlan;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn create_physical_plan(
        logical: &LogicalPlan,
        catalog: &Catalog,
    ) -> Result<Arc<PhysicalPlan>> {
        let mut next_id = 0;
        Self::build(logical, catalog, &mut next_id)
    }

    fn build(
        logical: &LogicalPlan,
        catalog: &Catalog,
        next_id: &mut OperatorId,
    ) -> Result<Arc<PhysicalPlan>> {
        let id = *next_id;
        *next_id += 1;
        let plan = match logical {
            LogicalPlan::Scan {
                table,
                projection,
                filters,
                schema,
            } => {
                let provider = catalog.table(table)?;
                let projection = projection
                    .as_ref()
                    .map(|columns| {
                        columns
                            .iter()
                            .map(|column| find_column(provider.schema().as_ref(), column))
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                PhysicalPlan::Scan {
                    id,
                    table_name: table.clone(),
                    provider,
                    projection,
                    filters: filters.clone(),
                    schema: schema.clone(),
                }
            }
            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => PhysicalPlan::Projection {
                id,
                input: Self::build(input, catalog, next_id)?,
                exprs: exprs.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Filter {
                input,
                predicate,
                schema,
            } => PhysicalPlan::Filter {
                id,
                input: Self::build(input, catalog, next_id)?,
                predicate: predicate.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                schema,
            } => PhysicalPlan::HashAggregate {
                id,
                input: Self::build(input, catalog, next_id)?,
                group_exprs: group_exprs.clone(),
                aggregate_exprs: aggregate_exprs.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Sort {
                input,
                exprs,
                schema,
            } => PhysicalPlan::Sort {
                id,
                input: Self::build(input, catalog, next_id)?,
                exprs: exprs.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Limit {
                input,
                limit,
                schema,
            } => PhysicalPlan::Limit {
                id,
                input: Self::build(input, catalog, next_id)?,
                limit: *limit,
                schema: schema.clone(),
            },
            LogicalPlan::Join {
                left,
                right,
                join_type,
                left_on,
                right_on,
                schema,
            } => PhysicalPlan::HashJoin {
                id,
                left: Self::build(left, catalog, next_id)?,
                right: Self::build(right, catalog, next_id)?,
                join_type: *join_type,
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                schema: schema.clone(),
            },
        };
        Ok(Arc::new(plan))
    }
}
