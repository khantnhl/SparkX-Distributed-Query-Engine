use crate::error::Result;
use crate::expr::{Expr, find_column};
use crate::logical::{LogicalPlan, SortExpr};
use arrow::datatypes::Schema;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct Optimizer;

impl Optimizer {
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = self.optimize_children(plan)?;
        let plan = simplify_expressions(plan)?;
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

fn simplify_expressions(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            schema,
        } => Ok(LogicalPlan::Scan {
            table,
            projection,
            filters: filters
                .iter()
                .map(|expr| expr.simplify(schema.as_ref()))
                .collect::<Result<Vec<_>>>()?,
            schema,
        }),
        LogicalPlan::Projection { input, exprs, .. } => {
            let schema = input.schema();
            LogicalPlan::project(
                (*input).clone(),
                exprs
                    .iter()
                    .map(|expr| expr.simplify(schema.as_ref()))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        LogicalPlan::Filter {
            input, predicate, ..
        } => {
            let predicate = predicate.simplify(input.schema().as_ref())?;
            LogicalPlan::filter((*input).clone(), predicate)
        }
        LogicalPlan::Aggregate {
            input,
            group_exprs,
            aggregate_exprs,
            ..
        } => {
            let schema = input.schema();
            LogicalPlan::aggregate(
                (*input).clone(),
                group_exprs
                    .iter()
                    .map(|expr| expr.simplify(schema.as_ref()))
                    .collect::<Result<Vec<_>>>()?,
                aggregate_exprs
                    .iter()
                    .map(|expr| expr.simplify(schema.as_ref()))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        LogicalPlan::Sort { input, exprs, .. } => {
            let schema = input.schema();
            LogicalPlan::sort(
                (*input).clone(),
                exprs
                    .iter()
                    .map(|sort| {
                        Ok(SortExpr {
                            expr: sort.expr.simplify(schema.as_ref())?,
                            ascending: sort.ascending,
                            nulls_first: sort.nulls_first,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        LogicalPlan::Limit { input, limit, .. } => Ok(LogicalPlan::limit((*input).clone(), limit)),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            left_on,
            right_on,
            ..
        } => LogicalPlan::join(
            (*left).clone(),
            (*right).clone(),
            join_type,
            left_on
                .iter()
                .map(|expr| expr.simplify(left.schema().as_ref()))
                .collect::<Result<Vec<_>>>()?,
            right_on
                .iter()
                .map(|expr| expr.simplify(right.schema().as_ref()))
                .collect::<Result<Vec<_>>>()?,
        ),
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
                    expr.name().as_str() == field.name().as_str()
                        && crate::expr::unqualified(name) == field.name().as_str()
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
