# SQL and type support

This matrix describes the behavior covered by SparkX's checked-in correctness tests. Features not
listed here should be treated as unsupported rather than assumed to follow a particular database.

## Query features

| Feature | Status | Current boundary |
|---|---|---|
| `SELECT` expressions and aliases | Supported | One `FROM` relation |
| `WHERE` | Supported | Boolean predicates with SQL three-valued `NULL` logic |
| `GROUP BY` | Supported | Ordinary grouping expressions; no grouping sets/modifiers |
| `HAVING` | Supported | Primarily aggregate output aliases |
| `ORDER BY` and `LIMIT` | Supported | No offset, fill, interpolate, or `ORDER BY ALL` |
| `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | Supported | `DISTINCT` is supported inside aggregates except `COUNT(DISTINCT *)` |
| Inner and left joins | Supported | One equi-join; multiple keys may be joined with `AND` |
| Qualified columns and table aliases | Supported | Ambiguous unqualified names are planning errors |
| `IS NULL` and `IS NOT NULL` | Supported | Produce non-null Boolean results |
| `CAST` and PostgreSQL `::` | Partially supported | Primitive targets listed below; no `TRY_CAST`, `SAFE_CAST`, format, or array options |
| `SELECT DISTINCT` | Unsupported | Distinct aggregate arguments are separate and supported |
| CTEs and subqueries | Unsupported | — |
| `UNION`, `INTERSECT`, `EXCEPT` | Unsupported | — |
| Window functions | Unsupported | — |
| Right, full, cross, semi, and anti joins | Unsupported | — |

## Primitive types

SparkX expression execution currently supports these Arrow types:

| Family | Arrow types | SQL `CAST` spellings |
|---|---|---|
| Boolean | `Boolean` | `BOOLEAN`, `BOOL` |
| Signed integer | `Int32`, `Int64` | Standard tiny/small/integer names map to `Int32`; bigint names map to `Int64` |
| Unsigned integer | `UInt32`, `UInt64` | Unsigned integer names map by width family |
| Floating point | `Float32`, `Float64` | `REAL`/`FLOAT32` map to `Float32`; `FLOAT`/`DOUBLE` map to `Float64` |
| String | `Utf8` | `CHAR`, `VARCHAR`, `TEXT`, `STRING` |
| Untyped null | `Null` | `CAST(NULL AS <supported type>)` creates a typed null |

Decimal, date, timestamp, binary, dictionary, list, and struct expression semantics remain future
work. A provider may expose additional Arrow types, but operators that extract scalar values will
reject types outside the matrix above.

## Coercion rules

Implicit coercion is intentionally narrow:

- `Int32` and `Int64` arithmetic is evaluated as `Int64`.
- `UInt32` and `UInt64` arithmetic is evaluated as `UInt64`.
- Arithmetic containing `Float32` or `Float64` is evaluated as `Float64` when the other operand is
  also numeric.
- Signed/unsigned mixing is rejected unless the query uses an explicit cast.
- Strings and Booleans are never implicitly converted to numbers.
- A `NULL` operand inherits the other numeric operand's normalized type and remains null.
- Comparisons with `NULL` produce unknown (`NULL`), not true or false.
- Boolean `AND`/`OR` use Kleene logic; for example, `FALSE AND NULL` is false and `TRUE OR NULL` is
  true.
- A null filter result is treated as not selected, matching SQL `WHERE` behavior.

The optimizer folds column-free expressions, removes safe Boolean identities such as
`TRUE AND predicate`, and replaces comparisons/arithmetic involving a guaranteed null with a typed
null. The optimized plan text exposes these rewrites.

## Distributed eligibility

The current local-cluster runner uses two-stage execution only for a non-distinct top-level hash
aggregate over a join-free input with more than one scan partition. Other query shapes execute
natively and report `distributed = false` rather than pretending to be distributed. Eligible
worker inputs round-trip through a versioned Protobuf physical-plan fragment and resolve scans from
the worker catalog. Their partial batches then cross a query-scoped loopback Arrow Flight/gRPC
exchange before the final merge.

CSV files expose one partition. Memory tables can contain multiple explicit partitions, and each
Parquet row group is a partition.

## Verification

`tests/sql/differential.sql` is executed against SparkX in both native and local-distributed modes
and against an embedded DuckDB reference engine. The checked-in corpus covers filtering,
projection, `NULL` predicates and Boolean behavior, primitive casts, grouped aggregates, and inner
and left joins. Golden files under `tests/snapshots` separately lock the logical, optimized, and
physical explain-plan shapes.
