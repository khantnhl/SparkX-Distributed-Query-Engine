use crate::catalog::{Catalog, CsvTable, MemoryTable, ParquetTable, TableRef};
use crate::distributed::LocalCluster;
use crate::error::{Result, SparkXError};
use crate::execution::{TaskContext, execute};
use crate::expr::{AggregateFunction, Expr, Operator, ScalarValue};
use crate::logical::{JoinType, LogicalPlan, SortExpr};
use crate::metrics::{MetricsSnapshot, QueryMetrics};
use crate::optimizer::Optimizer;
use crate::planner::PhysicalPlanner;
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, LimitClause, OrderByKind, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub batch_size: usize,
    pub channel_capacity: usize,
    pub workers: usize,
    pub distributed: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            batch_size: 8_192,
            channel_capacity: 2,
            workers: num_cpus::get().max(1),
            distributed: false,
        }
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub batches: Vec<RecordBatch>,
    pub metrics: MetricsSnapshot,
    pub logical_plan: String,
    pub optimized_plan: String,
    pub physical_plan: String,
    pub distributed: bool,
    pub stages: usize,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn pretty(&self) -> Result<String> {
        Ok(arrow::util::pretty::pretty_format_batches(&self.batches)?.to_string())
    }
}

#[derive(Debug)]
pub struct Session {
    config: SessionConfig,
    catalog: Arc<Catalog>,
    optimizer: Optimizer,
    physical_planner: PhysicalPlanner,
}

impl Session {
    pub fn new(config: SessionConfig) -> Self {
        let config = SessionConfig {
            batch_size: config.batch_size.max(1),
            channel_capacity: config.channel_capacity.max(1),
            workers: config.workers.max(1),
            distributed: config.distributed,
        };
        Self {
            config,
            catalog: Arc::new(Catalog::default()),
            optimizer: Optimizer,
            physical_planner: PhysicalPlanner,
        }
    }

    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub fn register_table(&self, name: impl Into<String>, table: TableRef) {
        self.catalog.register(name, table);
    }

    pub fn register_memory(&self, name: impl Into<String>, table: MemoryTable) {
        self.register_table(name, Arc::new(table));
    }

    pub fn register_csv(&self, name: impl Into<String>, path: impl AsRef<Path>) -> Result<()> {
        self.register_table(name, Arc::new(CsvTable::try_new(path)?));
        Ok(())
    }

    pub fn register_parquet(&self, name: impl Into<String>, path: impl AsRef<Path>) -> Result<()> {
        self.register_table(name, Arc::new(ParquetTable::try_new(path)?));
        Ok(())
    }

    pub fn sql(&self, sql: &str) -> Result<LogicalPlan> {
        let statements = Parser::parse_sql(&GenericDialect {}, sql)?;
        if statements.len() != 1 {
            return Err(SparkXError::planning(
                "exactly one SQL statement is required",
            ));
        }
        match &statements[0] {
            Statement::Query(query) => self.plan_query(query),
            statement => Err(SparkXError::unsupported(format!(
                "only SELECT queries are supported, got {statement}"
            ))),
        }
    }

    pub fn explain(&self, sql: &str) -> Result<String> {
        let logical = self.sql(sql)?;
        let optimized = self.optimizer.optimize(logical.clone())?;
        let physical = self
            .physical_planner
            .create_physical_plan(&optimized, &self.catalog)?;
        Ok(format!(
            "== Logical Plan ==\n{}\n== Optimized Logical Plan ==\n{}\n== Physical Plan ==\n{}",
            logical.explain(),
            optimized.explain(),
            physical.explain()
        ))
    }

    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        self.execute_plan(self.sql(sql)?).await
    }

    pub async fn execute_plan(&self, logical: LogicalPlan) -> Result<QueryResult> {
        let logical_text = logical.explain();
        let optimized = self.optimizer.optimize(logical)?;
        let optimized_text = optimized.explain();
        let physical = self
            .physical_planner
            .create_physical_plan(&optimized, &self.catalog)?;
        let physical_text = physical.explain();
        let metrics = Arc::new(QueryMetrics::default());
        let context = TaskContext {
            batch_size: self.config.batch_size,
            channel_capacity: self.config.channel_capacity,
            partition: None,
            metrics: metrics.clone(),
        };
        let started = Instant::now();
        let (batches, distributed, stages) = if self.config.distributed {
            let result = LocalCluster::new(self.config.workers)
                .execute(physical, context)
                .await?;
            (result.batches, result.distributed, result.stages)
        } else {
            (execute(physical, context).collect().await?, false, 1)
        };
        for batch in &batches {
            metrics.record_output(batch.num_rows());
        }
        metrics.set_elapsed(started.elapsed());
        Ok(QueryResult {
            batches,
            metrics: metrics.snapshot(),
            logical_plan: logical_text,
            optimized_plan: optimized_text,
            physical_plan: physical_text,
            distributed,
            stages,
        })
    }

    fn plan_query(&self, query: &Query) -> Result<LogicalPlan> {
        if query.with.is_some() {
            return Err(SparkXError::unsupported("CTEs are not implemented yet"));
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Err(SparkXError::unsupported(
                "UNION, INTERSECT, and EXCEPT are not implemented yet",
            ));
        };
        let mut plan = self.plan_select(select)?;

        if let Some(order_by) = &query.order_by {
            if order_by.interpolate.is_some() {
                return Err(SparkXError::unsupported("ORDER BY INTERPOLATE"));
            }
            let OrderByKind::Expressions(order_exprs) = &order_by.kind else {
                return Err(SparkXError::unsupported("ORDER BY ALL"));
            };
            let exprs = order_exprs
                .iter()
                .map(|order| {
                    if order.with_fill.is_some() {
                        return Err(SparkXError::unsupported("ORDER BY WITH FILL"));
                    }
                    Ok(SortExpr {
                        expr: Self::sql_expr(&order.expr)?,
                        ascending: order.options.asc.unwrap_or(true),
                        nulls_first: order
                            .options
                            .nulls_first
                            .unwrap_or(!order.options.asc.unwrap_or(true)),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            plan = LogicalPlan::sort(plan, exprs)?;
        }
        if let Some(limit) = parse_limit(query.limit_clause.as_ref())? {
            plan = LogicalPlan::limit(plan, limit);
        }
        Ok(plan)
    }

    fn plan_select(&self, select: &Select) -> Result<LogicalPlan> {
        if select.distinct.is_some() {
            return Err(SparkXError::unsupported("SELECT DISTINCT"));
        }
        if select.from.len() != 1 {
            return Err(SparkXError::planning(
                "a SELECT must contain exactly one FROM relation",
            ));
        }
        let mut plan = self.plan_from(&select.from[0])?;
        if let Some(selection) = &select.selection {
            plan = LogicalPlan::filter(plan, Self::sql_expr(selection)?)?;
        }

        let projection = select
            .projection
            .iter()
            .map(|item| self.select_item(item))
            .collect::<Result<Vec<_>>>()?;
        let group_exprs = match &select.group_by {
            GroupByExpr::Expressions(exprs, modifiers) => {
                if !modifiers.is_empty() {
                    return Err(SparkXError::unsupported("GROUP BY modifiers"));
                }
                exprs
                    .iter()
                    .map(Self::sql_expr)
                    .collect::<Result<Vec<_>>>()?
            }
            GroupByExpr::All(_) => projection
                .iter()
                .filter(|expr| !expr.contains_aggregate())
                .cloned()
                .collect(),
        };
        let aggregate_exprs = projection
            .iter()
            .filter(|expr| expr.contains_aggregate())
            .cloned()
            .collect::<Vec<_>>();
        if let Some(expression) = aggregate_exprs
            .iter()
            .find(|expr| !matches!(expr.unalias(), Expr::Aggregate { .. }))
        {
            return Err(SparkXError::unsupported(format!(
                "aggregate expressions must be directly projected; got {expression}"
            )));
        }

        if !group_exprs.is_empty() || !aggregate_exprs.is_empty() {
            plan = LogicalPlan::aggregate(plan, group_exprs, aggregate_exprs)?;
            let output = projection
                .iter()
                .map(|expr| Expr::column(expr.name()))
                .collect::<Vec<_>>();
            plan = LogicalPlan::project(plan, output)?;
            if let Some(having) = &select.having {
                plan = LogicalPlan::filter(plan, Self::sql_expr(having)?)?;
            }
        } else {
            plan = LogicalPlan::project(plan, projection)?;
        }
        Ok(plan)
    }

    fn plan_from(&self, from: &TableWithJoins) -> Result<LogicalPlan> {
        if from.joins.len() > 1 {
            return Err(SparkXError::unsupported(
                "more than one JOIN in a SELECT",
            ));
        }
        let (mut plan, left_qualifier) = self.table_factor(&from.relation)?;
        for join in &from.joins {
            let (right, right_qualifier) = self.table_factor(&join.relation)?;
            let (join_type, constraint) = match &join.join_operator {
                JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
                    (JoinType::Inner, constraint)
                }
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (JoinType::Left, constraint)
                }
                other => return Err(SparkXError::unsupported(format!("join operator {other}"))),
            };
            let JoinConstraint::On(condition) = constraint else {
                return Err(SparkXError::unsupported(
                    "only JOIN ... ON equality constraints are supported",
                ));
            };
            let mut pairs = Vec::new();
            extract_join_pairs(condition, &mut pairs)?;
            let mut left_on = Vec::new();
            let mut right_on = Vec::new();
            for (left, right_expr) in pairs {
                let left_is_right = expression_qualifier(&left)
                    .is_some_and(|qualifier| qualifier == right_qualifier);
                let right_is_left = expression_qualifier(&right_expr)
                    .is_some_and(|qualifier| qualifier == left_qualifier);
                if left_is_right || right_is_left {
                    left_on.push(Self::sql_expr(&right_expr)?);
                    right_on.push(Self::sql_expr(&left)?);
                } else {
                    left_on.push(Self::sql_expr(&left)?);
                    right_on.push(Self::sql_expr(&right_expr)?);
                }
            }
            plan = LogicalPlan::join(plan, right, join_type, left_on, right_on)?;
        }
        Ok(plan)
    }

    fn table_factor(&self, factor: &TableFactor) -> Result<(LogicalPlan, String)> {
        let TableFactor::Table {
            name, alias, args, ..
        } = factor
        else {
            return Err(SparkXError::unsupported(format!("table factor {factor}")));
        };
        if args.is_some() {
            return Err(SparkXError::unsupported("table-valued functions"));
        }
        let table = name.to_string();
        let qualifier = alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| table.clone());
        let provider = self.catalog.table(&table)?;
        Ok((LogicalPlan::scan(table, provider.schema()), qualifier))
    }

    fn select_item(&self, item: &SelectItem) -> Result<Expr> {
        match item {
            SelectItem::UnnamedExpr(expr) => Self::sql_expr(expr),
            SelectItem::ExprWithAlias { expr, alias } => {
                Ok(Self::sql_expr(expr)?.alias(alias.value.clone()))
            }
            SelectItem::Wildcard(_) => Ok(Expr::Wildcard),
            SelectItem::QualifiedWildcard(_, _) => {
                Err(SparkXError::unsupported("qualified wildcard projection"))
            }
            SelectItem::ExprWithAliases { .. } => {
                Err(SparkXError::unsupported("multi-alias projection"))
            }
        }
    }

    fn sql_expr(expr: &SqlExpr) -> Result<Expr> {
        match expr {
            SqlExpr::Identifier(identifier) => Ok(Expr::column(identifier.value.clone())),
            SqlExpr::CompoundIdentifier(identifiers) => Ok(Expr::column(
                identifiers
                    .iter()
                    .map(|identifier| identifier.value.as_str())
                    .collect::<Vec<_>>()
                    .join("."),
            )),
            SqlExpr::Value(value) => sql_value(&value.value),
            SqlExpr::Nested(expr) => Self::sql_expr(expr),
            SqlExpr::BinaryOp { left, op, right } => Ok(Expr::binary(
                Self::sql_expr(left)?,
                sql_operator(op)?,
                Self::sql_expr(right)?,
            )),
            SqlExpr::Function(function) => {
                if function.over.is_some()
                    || function.filter.is_some()
                    || function.null_treatment.is_some()
                    || !function.within_group.is_empty()
                    || !matches!(&function.parameters, FunctionArguments::None)
                {
                    return Err(SparkXError::unsupported(
                        "aggregate parameters, windows, filters, null treatment, or WITHIN GROUP",
                    ));
                }
                let function_name = function.name.to_string().to_ascii_uppercase();
                let aggregate = match function_name.as_str() {
                    "COUNT" => AggregateFunction::Count,
                    "SUM" => AggregateFunction::Sum,
                    "MIN" => AggregateFunction::Min,
                    "MAX" => AggregateFunction::Max,
                    "AVG" => AggregateFunction::Avg,
                    _ => {
                        return Err(SparkXError::unsupported(format!(
                            "function {function_name}"
                        )));
                    }
                };
                let FunctionArguments::List(arguments) = &function.args else {
                    return Err(SparkXError::planning(format!(
                        "{function_name} requires arguments"
                    )));
                };
                if arguments.args.len() != 1 {
                    return Err(SparkXError::planning(format!(
                        "{function_name} requires exactly one argument"
                    )));
                }
                if !arguments.clauses.is_empty() {
                    return Err(SparkXError::unsupported(
                        "aggregate argument ORDER BY/LIMIT clauses",
                    ));
                }
                let argument = match &arguments.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Self::sql_expr(expr)?,
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Expr::Wildcard,
                    other => {
                        return Err(SparkXError::unsupported(format!(
                            "aggregate argument {other}"
                        )));
                    }
                };
                let distinct = matches!(
                    arguments.duplicate_treatment,
                    Some(DuplicateTreatment::Distinct)
                );
                if distinct && argument == Expr::Wildcard {
                    return Err(SparkXError::unsupported("DISTINCT wildcard aggregate"));
                }
                Ok(Expr::Aggregate {
                    function: aggregate,
                    expr: Box::new(argument),
                    distinct,
                })
            }
            other => Err(SparkXError::unsupported(format!("SQL expression {other}"))),
        }
    }
}

fn sql_operator(operator: &BinaryOperator) -> Result<Operator> {
    Ok(match operator {
        BinaryOperator::Eq => Operator::Eq,
        BinaryOperator::NotEq => Operator::NotEq,
        BinaryOperator::Lt => Operator::Lt,
        BinaryOperator::LtEq => Operator::LtEq,
        BinaryOperator::Gt => Operator::Gt,
        BinaryOperator::GtEq => Operator::GtEq,
        BinaryOperator::And => Operator::And,
        BinaryOperator::Or => Operator::Or,
        BinaryOperator::Plus => Operator::Add,
        BinaryOperator::Minus => Operator::Subtract,
        BinaryOperator::Multiply => Operator::Multiply,
        BinaryOperator::Divide => Operator::Divide,
        other => return Err(SparkXError::unsupported(format!("operator {other}"))),
    })
}

fn sql_value(value: &Value) -> Result<Expr> {
    let scalar = match value {
        Value::Number(value, _) if value.contains(['.', 'e', 'E']) => {
            ScalarValue::Float64(value.parse().map_err(|_| {
                SparkXError::planning(format!("invalid floating-point literal {value}"))
            })?)
        }
        Value::Number(value, _) => ScalarValue::Int64(
            value
                .parse()
                .map_err(|_| SparkXError::planning(format!("invalid integer literal {value}")))?,
        ),
        Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
            ScalarValue::Utf8(value.clone())
        }
        Value::Boolean(value) => ScalarValue::Boolean(*value),
        Value::Null => ScalarValue::Null,
        other => return Err(SparkXError::unsupported(format!("literal {other}"))),
    };
    Ok(Expr::literal(scalar))
}

fn parse_limit(limit: Option<&LimitClause>) -> Result<Option<usize>> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let expression = match limit {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if offset.is_some() || !limit_by.is_empty() {
                return Err(SparkXError::unsupported("LIMIT OFFSET/BY"));
            }
            limit.as_ref()
        }
        LimitClause::OffsetCommaLimit { .. } => {
            return Err(SparkXError::unsupported("LIMIT offset, count"));
        }
    };
    let Some(SqlExpr::Value(value)) = expression else {
        return Err(SparkXError::planning("LIMIT must be an integer literal"));
    };
    let Value::Number(value, _) = &value.value else {
        return Err(SparkXError::planning("LIMIT must be an integer literal"));
    };
    Ok(Some(value.parse().map_err(|_| {
        SparkXError::planning(format!("invalid LIMIT {value}"))
    })?))
}

fn extract_join_pairs(expr: &SqlExpr, output: &mut Vec<(SqlExpr, SqlExpr)>) -> Result<()> {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            extract_join_pairs(left, output)?;
            extract_join_pairs(right, output)
        }
        SqlExpr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            output.push((left.as_ref().clone(), right.as_ref().clone()));
            Ok(())
        }
        other => Err(SparkXError::unsupported(format!(
            "join condition {other}; only equality joined with AND is supported"
        ))),
    }
}

fn expression_qualifier(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::CompoundIdentifier(parts) if parts.len() > 1 => {
            Some(parts[parts.len() - 2].value.clone())
        }
        _ => None,
    }
}
