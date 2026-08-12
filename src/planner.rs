use crate::catalog::Catalog;
use crate::error::Result;
use crate::execution::PhysicalPlan;
use crate::expr::find_column;
use crate::logical::LogicalPlan;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn create_physical_plan(
        &self,
        logical: &LogicalPlan,
        catalog: &Catalog,
    ) -> Result<Arc<PhysicalPlan>> {
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
                input: self.create_physical_plan(input, catalog)?,
                exprs: exprs.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Filter {
                input,
                predicate,
                schema,
            } => PhysicalPlan::Filter {
                input: self.create_physical_plan(input, catalog)?,
                predicate: predicate.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                schema,
            } => PhysicalPlan::HashAggregate {
                input: self.create_physical_plan(input, catalog)?,
                group_exprs: group_exprs.clone(),
                aggregate_exprs: aggregate_exprs.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Sort {
                input,
                exprs,
                schema,
            } => PhysicalPlan::Sort {
                input: self.create_physical_plan(input, catalog)?,
                exprs: exprs.clone(),
                schema: schema.clone(),
            },
            LogicalPlan::Limit {
                input,
                limit,
                schema,
            } => PhysicalPlan::Limit {
                input: self.create_physical_plan(input, catalog)?,
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
                left: self.create_physical_plan(left, catalog)?,
                right: self.create_physical_plan(right, catalog)?,
                join_type: *join_type,
                left_on: left_on.clone(),
                right_on: right_on.clone(),
                schema: schema.clone(),
            },
        };
        Ok(Arc::new(plan))
    }
}
