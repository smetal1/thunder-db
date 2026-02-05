//! # Thunder SQL
//!
//! SQL layer for ThunderDB providing:
//! - SQL parser (PostgreSQL dialect)
//! - Multi-dialect support (MySQL, MongoDB, Redis)
//! - Natural language query interface (AI/LLM-native)
//! - LLM integration via llama.cpp
//! - ML-native data operations
//! - Query analyzer and semantic validation
//! - Query planner and optimizer
//! - User-defined functions (UDFs)

pub mod analyzer;
pub mod catalog;
pub mod dialect;
pub mod llm;
pub mod ml;
pub mod nlp;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod types;
pub mod udf;

use async_trait::async_trait;
use thunder_common::error::SqlError;
use thunder_common::prelude::*;

pub use sqlparser::ast::Statement;

// Re-export key types from modules
pub use analyzer::SqlAnalyzer;
pub use catalog::{
    Catalog, CatalogProvider, ColumnInfo, ColumnStats, IndexColumn, IndexInfo, IndexType,
    TableInfo, TableStats,
};
pub use optimizer::{OptimizationRule, RuleBasedOptimizer};
pub use parser::{
    convert_data_type, convert_expr, convert_join_type, convert_order_by, extract_table_name,
    is_aggregate_function, try_convert_aggregate,
};
pub use planner::{
    format_plan, walk_plan, ColumnCollector, CostEstimator, PlanCost, PlanTransformer,
    PlanVisitor, TableCollector,
};
pub use types::{TypeCoercion, TypeInference, TypeValidator};
pub use udf::{Accumulator, AggregateUdf, ScalarUdf, UdfRegistry};

// Multi-dialect and NLP support
pub use dialect::{
    Dialect, DialectDetector, MySQLTranslator, MongoDBTranslator, RedisTranslator, QueryTranslator,
};
pub use nlp::{NLProcessor, NLQuery, NLQueryResult, QueryContext, QueryIntent, VariableProcessor};

// LLM integration
pub use llm::{
    LlmConfig, LlmEngine, LlmResponse, SchemaInfo, TableSchema, ColumnSchema,
    ModelRegistry, ModelInfo, PromptTemplates, TokenUsage,
};

// ML-native features
pub use ml::{
    MLDataType, TensorInfo, FeatureColumn, DatasetInfo, ModelMetadata,
    TrainingConfig, DistributedConfig, AgentMemory, AgentContext,
};

/// Parse SQL statements
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| Error::Sql(SqlError::ParseError(e.to_string())))
}

/// Logical plan representation
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a table
    Scan {
        table: String,
        schema: Schema,
        filter: Option<Box<Expr>>,
        projection: Option<Vec<usize>>,
    },
    /// Project columns
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<Expr>,
        schema: Schema,
    },
    /// Filter rows
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Join two inputs
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expr,
        join_type: JoinType,
    },
    /// Aggregate
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: Schema,
    },
    /// Sort
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<SortExpr>,
    },
    /// Limit
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },
    /// Insert
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    },
    /// Update
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    /// Delete
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    /// Create table
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    /// Drop table
    DropTable {
        name: String,
        if_exists: bool,
    },
    /// Create index
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    /// Drop index
    DropIndex {
        name: String,
        if_exists: bool,
    },
    /// Alter table
    AlterTable {
        table: String,
        operations: Vec<AlterTableOp>,
    },
    /// Explain a query plan
    Explain {
        plan: Box<LogicalPlan>,
        analyze: bool,
        verbose: bool,
    },
    /// Analyze table (collect statistics)
    AnalyzeTable {
        table: Option<String>,
    },
    /// Empty result
    Empty,
}

/// Operations for ALTER TABLE
#[derive(Debug, Clone)]
pub enum AlterTableOp {
    /// Add a new column
    AddColumn { column: ColumnDef },
    /// Drop an existing column
    DropColumn { name: String },
    /// Rename a column
    RenameColumn { old_name: String, new_name: String },
    /// Rename the table
    RenameTable { new_name: String },
}

/// Expression types
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference
    Column { name: String, index: Option<usize> },
    /// Literal value
    Literal(Value),
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// Function call
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    /// IS NULL
    IsNull(Box<Expr>),
    /// IS NOT NULL
    IsNotNull(Box<Expr>),
    /// IN list
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// BETWEEN
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// Subquery
    Subquery(Box<LogicalPlan>),
    /// Cast
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// Wildcard (*)
    Wildcard,
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
    // String
    Like,
    ILike,
    // Other
    Concat,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    Not,
    Neg,
    Plus,
}

/// Join types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Aggregate expression
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunction,
    pub args: Vec<Expr>,
    pub distinct: bool,
}

/// Built-in aggregate functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    ArrayAgg,
    StringAgg,
}

/// Sort expression
#[derive(Debug, Clone)]
pub struct SortExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

/// Query analyzer trait
#[async_trait]
pub trait Analyzer: Send + Sync {
    /// Analyze a SQL statement and produce a logical plan
    async fn analyze(&self, stmt: Statement) -> Result<LogicalPlan>;
}

/// Query optimizer trait
pub trait Optimizer: Send + Sync {
    /// Optimize a logical plan
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let sql = "SELECT id, name FROM users WHERE age > 18";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_invalid() {
        let sql = "SELECTT * FROM users";
        assert!(parse_sql(sql).is_err());
    }
}
