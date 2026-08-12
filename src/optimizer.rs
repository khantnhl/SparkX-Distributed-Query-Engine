use crate::error::Result;
use crate::expr::{Expr, find_column};
use crate::logical::LogicalPlan;
use arrow::datatypes::Schema;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct Optimizer;

impl Optimizer {
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = self.optimize_children(plan)?;
        let plan = push_filter_into_scan(plan)?;
        let plan = push_projection_into_scan(plan)?;
        eliminate_identity_projection(plan)
    }

    fn optimize_children(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan { .. } => Ok(plan),
            LogicalPlan::Projection { input, exprs, .. } => {
                LogicalPlan::project(self.optimize((*input).clone())?, exprs)
            }
            LogicalPlan::Filter {
                input, predicate, ..
            } => LogicalPlan::filter(self.optimize((*input).clone())?, predicate),
            LogicalPlan::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                ..
            } => LogicalPlan::aggregate(
                self.optimize((*input).clone())?,
                group_exprs,
                aggregate_exprs,
            ),
            LogicalPlan::Sort { input, exprs, .. } => {
                LogicalPlan::sort(self.optimize((*input).clone())?, exprs)
            }
            LogicalPlan::Limit { input, limit, .. } => {
                Ok(LogicalPlan::limit(self.optimize((*input).clone())?, limit))
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                left_on,
                right_on,
                ..
            } => LogicalPlan::join(
                self.optimize((*left).clone())?,
                self.optimize((*right).clone())?,
                join_type,
                left_on,
                right_on,
            ),
        }
    }
}

fn eliminate_identity_projection(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Projection { input, exprs, .. } = plan else {
        return Ok(plan);
    };
    let schema = input.schema();
    let identity = exprs.len() == schema.fields().len()
        && exprs
            .iter()
            .zip(schema.fields())
            .all(|(expr, field)| match expr.unalias() {
                Expr::Column(name) => {
                    expr.name() == field.name() && crate::expr::unqualified(name) == field.name()
                }
                _ => false,
            });
    if identity {
        Ok((*input).clone())
    } else {
        LogicalPlan::project((*input).clone(), exprs)
    }
}

fn push_filter_into_scan(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Filter {
        input, predicate, ..
    } = plan
    else {
        return Ok(plan);
    };

    match input.as_ref() {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            schema,
        } => {
            let mut filters = filters.clone();
            filters.push(predicate);
            Ok(LogicalPlan::Scan {
                table: table.clone(),
                projection: projection.clone(),
                filters,
                schema: schema.clone(),
            })
        }
        _ => LogicalPlan::filter((*input).clone(), predicate),
    }
}

fn push_projection_into_scan(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Projection { input, exprs, .. } = plan else {
        return Ok(plan);
    };

    let LogicalPlan::Scan {
        table,
        projection: _,
        filters,
        schema,
    } = input.as_ref()
    else {
        return LogicalPlan::project((*input).clone(), exprs);
    };

    let mut required = BTreeSet::new();
    for expr in exprs.iter().chain(filters.iter()) {
        required.extend(expr.columns());
    }
    if required.is_empty() {
        return LogicalPlan::project((*input).clone(), exprs);
    }

    let indices = projection_indices(schema.as_ref(), &required)?;
    let projected_fields = indices
        .iter()
        .map(|index| schema.field(*index).clone())
        .collect::<Vec<_>>();
    let scan = LogicalPlan::Scan {
        table: table.clone(),
        projection: Some(
            indices
                .iter()
                .map(|index| schema.field(*index).name().clone())
                .collect(),
        ),
        filters: filters.clone(),
        schema: Arc::new(Schema::new(projected_fields)),
    };
    LogicalPlan::project(scan, exprs)
}

fn projection_indices(schema: &Schema, required: &BTreeSet<String>) -> Result<Vec<usize>> {
    required
        .iter()
        .map(|name| find_column(schema, name))
        .collect()
}
