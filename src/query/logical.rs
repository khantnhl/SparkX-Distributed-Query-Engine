use crate::error::{Result, SparkXError};
use crate::expr::Expr;
use arrow::datatypes::{Field, Schema, SchemaRef};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inner => write!(f, "Inner"),
            Self::Left => write!(f, "Left"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        projection: Option<Vec<String>>,
        filters: Vec<Expr>,
        schema: SchemaRef,
    },
    Projection {
        input: Arc<LogicalPlan>,
        exprs: Vec<Expr>,
        schema: SchemaRef,
    },
    Filter {
        input: Arc<LogicalPlan>,
        predicate: Expr,
        schema: SchemaRef,
    },
    Aggregate {
        input: Arc<LogicalPlan>,
        group_exprs: Vec<Expr>,
        aggregate_exprs: Vec<Expr>,
        schema: SchemaRef,
    },
    Sort {
        input: Arc<LogicalPlan>,
        exprs: Vec<SortExpr>,
        schema: SchemaRef,
    },
    Limit {
        input: Arc<LogicalPlan>,
        limit: usize,
        schema: SchemaRef,
    },
    Join {
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        join_type: JoinType,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        schema: SchemaRef,
    },
}

impl LogicalPlan {
    pub fn scan(table: impl Into<String>, schema: SchemaRef) -> Self {
        Self::Scan {
            table: table.into(),
            projection: None,
            filters: Vec::new(),
            schema,
        }
    }

    pub fn project(input: LogicalPlan, exprs: Vec<Expr>) -> Result<Self> {
        let expanded = expand_projection(&exprs, input.schema().as_ref())?;
        let fields = expanded
            .iter()
            .map(|expr| expr.field(input.schema().as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::Projection {
            input: Arc::new(input),
            exprs: expanded,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    pub fn filter(input: LogicalPlan, predicate: Expr) -> Result<Self> {
        let data_type = predicate.data_type(input.schema().as_ref())?;
        if data_type != arrow::datatypes::DataType::Boolean {
            return Err(SparkXError::planning(format!(
                "filter predicate must be Boolean, got {data_type}"
            )));
        }
        let schema = input.schema();
        Ok(Self::Filter {
            input: Arc::new(input),
            predicate,
            schema,
        })
    }

    pub fn aggregate(
        input: LogicalPlan,
        group_exprs: Vec<Expr>,
        aggregate_exprs: Vec<Expr>,
    ) -> Result<Self> {
        let mut fields = Vec::with_capacity(group_exprs.len() + aggregate_exprs.len());
        for expr in group_exprs.iter().chain(aggregate_exprs.iter()) {
            fields.push(expr.field(input.schema().as_ref())?);
        }
        Ok(Self::Aggregate {
            input: Arc::new(input),
            group_exprs,
            aggregate_exprs,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    pub fn sort(input: LogicalPlan, exprs: Vec<SortExpr>) -> Result<Self> {
        for sort in &exprs {
            sort.expr.data_type(input.schema().as_ref())?;
        }
        let schema = input.schema();
        Ok(Self::Sort {
            input: Arc::new(input),
            exprs,
            schema,
        })
    }

    pub fn limit(input: LogicalPlan, limit: usize) -> Self {
        let schema = input.schema();
        Self::Limit {
            input: Arc::new(input),
            limit,
            schema,
        }
    }

    pub fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
    ) -> Result<Self> {
        if left_on.len() != right_on.len() || left_on.is_empty() {
            return Err(SparkXError::planning(
                "joins require the same non-zero number of left and right keys",
            ));
        }
        for (left_key, right_key) in left_on.iter().zip(&right_on) {
            let left_type = left_key.data_type(left.schema().as_ref())?;
            let right_type = right_key.data_type(right.schema().as_ref())?;
            if left_type != right_type {
                return Err(SparkXError::planning(format!(
                    "join key types differ: {left_type} versus {right_type}"
                )));
            }
        }
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        for field in right.schema().fields() {
            let mut output = field.as_ref().clone();
            if join_type == JoinType::Left {
                output = output.with_nullable(true);
            }
            if fields
                .iter()
                .any(|existing| existing.name() == output.name())
            {
                let output_name = format!("right.{}", output.name());
                output = output.with_name(output_name);
            }
            fields.push(output);
        }
        Ok(Self::Join {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type,
            left_on,
            right_on,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Scan { schema, .. }
            | Self::Projection { schema, .. }
            | Self::Filter { schema, .. }
            | Self::Aggregate { schema, .. }
            | Self::Sort { schema, .. }
            | Self::Limit { schema, .. }
            | Self::Join { schema, .. } => schema.clone(),
        }
    }

    pub fn explain(&self) -> String {
        let mut output = String::new();
        self.format_into(0, &mut output);
        output
    }

    fn format_into(&self, indent: usize, output: &mut String) {
        output.push_str(&"  ".repeat(indent));
        match self {
            Self::Scan {
                table,
                projection,
                filters,
                ..
            } => {
                output.push_str(&format!("Scan: {table}"));
                if let Some(projection) = projection {
                    output.push_str(&format!(" projection={projection:?}"));
                }
                if !filters.is_empty() {
                    output.push_str(&format!(
                        " filters=[{}]",
                        filters
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                output.push('\n');
            }
            Self::Projection { input, exprs, .. } => {
                output.push_str(&format_exprs("Project", exprs));
                input.format_into(indent + 1, output);
            }
            Self::Filter {
                input, predicate, ..
            } => {
                output.push_str(&format!("Filter: {predicate}\n"));
                input.format_into(indent + 1, output);
            }
            Self::Aggregate {
                input,
                group_exprs,
                aggregate_exprs,
                ..
            } => {
                output.push_str(&format!(
                    "Aggregate: group=[{}] aggr=[{}]\n",
                    display_exprs(group_exprs),
                    display_exprs(aggregate_exprs)
                ));
                input.format_into(indent + 1, output);
            }
            Self::Sort { input, exprs, .. } => {
                output.push_str(&format!(
                    "Sort: [{}]\n",
                    exprs
                        .iter()
                        .map(|sort| format!(
                            "{} {}",
                            sort.expr,
                            if sort.ascending { "ASC" } else { "DESC" }
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                input.format_into(indent + 1, output);
            }
            Self::Limit { input, limit, .. } => {
                output.push_str(&format!("Limit: {limit}\n"));
                input.format_into(indent + 1, output);
            }
            Self::Join {
                left,
                right,
                join_type,
                left_on,
                right_on,
                ..
            } => {
                output.push_str(&format!(
                    "HashJoin: {join_type} on [{} = {}]\n",
                    display_exprs(left_on),
                    display_exprs(right_on)
                ));
                left.format_into(indent + 1, output);
                right.format_into(indent + 1, output);
            }
        }
    }
}

fn expand_projection(exprs: &[Expr], schema: &Schema) -> Result<Vec<Expr>> {
    let mut expanded = Vec::new();
    for expr in exprs {
        match expr {
            Expr::Wildcard => expanded.extend(
                schema
                    .fields()
                    .iter()
                    .map(|field| Expr::column(field.name())),
            ),
            other => expanded.push(other.clone()),
        }
    }
    Ok(expanded)
}

fn display_exprs(exprs: &[Expr]) -> String {
    exprs
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_exprs(label: &str, exprs: &[Expr]) -> String {
    format!("{label}: [{}]\n", display_exprs(exprs))
}
