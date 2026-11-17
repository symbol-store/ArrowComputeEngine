# BOSS Operator Catalog (as implemented by ArrowComputeEngine)

This catalog describes the operators that ArrowComputeEngine implements. It is **descriptive**, not normative: it documents the syntax, semantics, and edge cases of one engine's vocabulary, so that new engines targeting similar relational workloads can reuse the conventions where they fit.

There is no central BOSS specification of "the" operator set. Each engine is free to define its own. Interoperability is much easier when engines reuse the same operator names and shapes for the same semantics — reuse when it fits, deviate explicitly when it does not, and document any deviation in your engine's `GetEngineDescription` output.

For implementation guidance, see [`EngineImplementationGuide.md`](EngineImplementationGuide.md).

---

## Conventions

- **Operator heads** are written in PascalCase (`Filter`, `OrderBy`, `GroupBy`, `LeftJoin`). Internally, ArrowComputeEngine lowercases them and applies aliases (see `toArrowName` in the source).
- **Symbols inside operator arguments** are column references if they match a column name in the input schema, and symbol-valued literals otherwise. This is what lets `(Equal sport Running)` parse as "the `sport` column equals the symbol `Running`" without needing an explicit quote.
- **String literals** are always values: `(Filter t (Equal name "Alice"))`.
- **Integer literals** are int64, both in expressions and in `Table`. A `Table` column that mixes integer and floating literals is float64.
- **Errors** are returned as a string result (e.g. `"Mean expects exactly 1 argument, got 2"`); the engine and its named tables stay alive.
- **Malformed operators stay unevaluated.** Operator shapes are matched by pattern (e.g. `Slice(table Integer Integer)`); an expression that matches no pattern is returned unevaluated, as with any expression the engine does not handle.
- **Auto-cast for date literals**: bare `YYYY-MM-DD` string literals appearing as the right-hand side of a comparison are auto-cast to Arrow `date32`.
- **Output column names** for aggregates follow the form `fn(col)`, e.g. `sum(quantity)`. They become BOSS symbols that, because they contain parentheses, must be quoted with `|…|` syntax in Scheme: `|sum(quantity)|`.
- **Pipeline composition**: every operator that consumes a "table" accepts any other operator that produces a table. There is no separate plan-builder mode.
- **Lazy execution**: most operators build a plan fragment and defer execution. `Materialize`, `ToStatus`, and the final `convertResult` at the C ABI boundary are the points at which a plan is actually run.

---

## Ingestion

### `Load(path [col …])`

Reads a CSV file from `path` into an in-memory table.

- `path` is a string.
- Files ending in `.tbl` are parsed with `|` delimiters (TPC-H convention), with an extra trailing-empty-column workaround.
- An optional list of column-name symbols restricts the load to those columns: `(Load "file.csv" id name email)`.
- Returns a handle into the engine's registry.

**Directory mode.** If `path` is a directory, every `.csv` and `.tbl` file at the top level is loaded and unioned:

- The result schema is the union of all per-file schemas; columns absent in a given file are filled with nulls (which surface as `NULL` symbols when materialized).
- A `"file name"` column of `boss_type=symbol` is appended to every row with the basename of the file it came from (extension included).
- Optional column-name arguments are applied to every file (same `include_columns` list per CSV).
- A directory with no `.csv`/`.tbl` files produces an empty table.

**Example:**

```scheme
(Load "owid-covid-data.csv")
(Load "lineitem.tbl" l_orderkey l_partkey l_quantity)
(Load "TPCHData/")  ; loads every .csv/.tbl in the directory, tagged by file name
```

### `Table((col v1 v2 …) …)`

Constructs a table from inline literal data. Each child is a column expression whose head is the column name and whose values are the column data.

- Column type is inferred from the first non-symbol value (int64, float64, string, bool).
- A `Symbol` value inside a typed column is treated as `NULL` (the "named-null" idiom: `(A 1 NULL 3)` parses with `NULL` as a Symbol since it is unquoted).
- A column whose values are *all* symbols becomes a string-typed column with `boss_type=symbol` Arrow field metadata, so it round-trips as symbols rather than strings.
- Span arguments (typed columnar slices) are accepted in addition to dynamic arguments; the Arrow engine's `Table` handler walks both.

**Example:**

```scheme
(Table (A 1 2 3) (B 4.0 5.0 6.0))
(Table (sport Running Swimming) (distance 5 10))
(Table (flags #t NULL #f))
```

---

## Reshape

### `Project(table col …)`

Selects and renames columns. Each subsequent argument is either:

- A bare column symbol — keep the column as-is.
- `(As expr name)` — compute `expr` and bind the result to `name`.
- A compute expression — keep the result, naming it after the expression's text form (often unwieldy; prefer `As`).

Supports type casts via `(Int col)`, `(Bool col)`, `(Date col)`, `(Timestamp col)`, and any Arrow compute function as a head (`Add`, `Subtract`, `Multiply`, `IfElse`, `Year`, `Month`, …).

**Example:**

```scheme
(Project orders custkey (As (Multiply price (Subtract 1.0 discount)) revenue))
```

### `Filter(table predicate)`

Keeps rows where `predicate` evaluates true. `predicate` is built from:

- Comparison heads: `Equal`, `NotEqual`, `Less`, `LessEqual`, `Greater`, `GreaterEqual`.
- Combinators: `And`, `Or`, `Not`.
- `(Between val lo hi)` — desugared to `(And (GreaterEqual val lo) (LessEqual val hi))`.
- `(Like col pattern)` — SQL `%`/`_` wildcard match via Arrow's `match_like`.
- Arbitrary Arrow compute functions used as the head.

**Example:**

```scheme
(Filter lineitem (And (Greater discount 0.05) (Less discount 0.07)))
(Filter customers (Like name "Al%"))
(Filter shipments (Between ship_date "1996-01-01" "1996-12-31"))
```

### `Slice(table offset count)`

Returns rows `[offset, offset+count)`. Both arguments must be integer literals.

**Example:**

```scheme
(Slice (OrderBy parts (keys price)) 0 10)   ; cheapest 10
```

---

## Ordering

### `OrderBy(table (keys col …))`

Sorts by the listed keys in order of precedence. A bare symbol is ascending; `(Desc col)` is descending.

**Example:**

```scheme
(OrderBy sales (keys region (Desc revenue)))
```

---

## Aggregation

### `GroupBy(table (agg col) … [key …])`

Aggregates one or more columns. Each aggregator is a unary expression naming a column to reduce (`(Sum x)`, `(Mean x)`, `(Max x)`, `(Min x)`, `(Count x)`) or the nullary `(CountAll)`.

- Aggregator heads are mapped through `toArrowName`: `Avg` → `mean`, `CountAll` → `count_all`.
- Group keys, if any, follow all aggregators as bare symbols.
- Without keys, it is a global aggregate (single-row output).
- Output columns are named `fn(col)` (e.g. `sum(quantity)`); rename them in a subsequent `Project (As …)` for ergonomic results.
- `(Median col)` and `(Quantile col q [interpolation])` compute exact quantiles as float64, skipping nulls. The interpolation between neighbouring values is `Linear` (default), `Lower`, `Higher`, `Nearest` or `Midpoint`.

**Example:**

```scheme
(GroupBy lineitem (Sum quantity) (CountAll) returnflag linestatus)
(GroupBy laps (Median pace) (Quantile pace 0.99 Lower))
```

**HAVING** is expressed as `Filter` after `GroupBy`:

```scheme
(Filter (GroupBy orders (Sum quantity) orderkey) (Greater |sum(quantity)| 300))
```

---

## Sliding and cumulative

These operators are order-sensitive — the input table must already be in the order you want the window to walk. They are also ArrowComputeEngine's example of operators that bypass the underlying planner (Acero) because the planner cannot express them, and use scalar compute kernels directly. See §12 "escape hatch" in the implementation guide.

### `Cumulate(table (agg col))`

Appends a column containing the running prefix aggregate (e.g. cumulative sum) of the named column. Output column is named `fn(col)`.

**Example:**

```scheme
(Cumulate sales (Sum revenue))    ; appends |sum(revenue)| as a running total
```

### `Pairwise(table out-col in-col lag)`

Appends a column `out-col` whose value at row `i` is `in-col[i] − in-col[i − lag]`. The first `lag` rows are `NULL`.

**Example:**

```scheme
(Pairwise (Cumulate sales (Sum revenue)) smoothed_revenue |sum(revenue)| 7)
```

### `Window(table (agg col) … size [Partial] [(OrderBy col)])`

Appends one column per aggregate, holding the aggregate of each row and the `size − 1` rows before it. Supported aggregates are `Sum`, `Mean`, `Min`, `Max` and `Count`; nulls are ignored. The first `size − 1` rows are `NULL`; with `Partial`, they aggregate the rows available instead. Rows are taken in input order, or sorted by the column given in `(OrderBy col)`. Output columns are named `fn(col)`.

**Example:**

```scheme
(Window readings (Mean power) 30 (OrderBy timestamp))    ; appends |mean(power)|
```

### `ExponentialMovingAverage(table [out-col] col (Alpha a))` / `ExponentialMovingAverage(table [out-col] col (Span n))`

Appends a column containing the exponentially weighted moving average of the named column. Starting from 0, each row moves the average towards its value by the fraction `a`; null rows keep the previous average. `(Span n)` sets `a` to `2/(n+1)`. `a` and `n` may be any expression without columns, such as `(Divide 1 42)` or a named one-row, one-column table. The output column is named `out-col`, or `exponential_moving_average(col)` if none is given.

**Example:**

```scheme
(ExponentialMovingAverage daily fitness load (Alpha (Divide 1 42)))
```

### `KernelDensityEstimate(table col (Grid start stop step) (Bandwidth h))`

Returns the Gaussian kernel density of the named column at each point from `start` to `stop` in steps of `step`, as columns `x` and `density`. `h` is the bandwidth, or `Silverman` to derive it from the data (1.06 × standard deviation × count^−1/5). Nulls are ignored.

**Example:**

```scheme
(KernelDensityEstimate readings heart_rate (Grid 50 175 0.5) (Bandwidth Silverman))
```

### `Resample(table key (agg col) … step [(Fill v)])`

Aggregates like `GroupBy`, grouped by `key`, a numeric column of `table`, with one row per `step` from the smallest to the largest key. The output is sorted. Keys without input rows get the fill value (default `0`) in every aggregate column.

**Example:**

```scheme
(Resample sessions day (Sum load) 1)    ; one row per day, 0 on days without sessions
```

---

## Generators

### `Range(col start stop step)`

Constructs a one-column table of evenly spaced values from `start` to `stop` (inclusive). The column is int64 if all three bounds are integers, otherwise float64.

**Example:**

```scheme
(Range x 50.0 175.0 0.5)    ; 50.0, 50.5, …, 175.0
```

---

## Joins

All join variants use the same predicate syntax: one or more `(Equal lCol rCol)` predicates define equi-join keys, and any additional predicates are ANDed together as a residual filter applied after the hash lookup. Colliding output column names get `_l` / `_r` suffixes.

### `Join(left right pred …)`

Inner hash join.

**Warning:** omitting all `Equal` predicates degrades to an O(n²) cross-join. The engine inserts a constant dummy key column on both sides and applies the predicate as a post-join filter. Useful for `(Between val lo hi)`-style joins, but expensive on large inputs.

**Example:**

```scheme
(Join orders lineitems (Equal orderkey orderkey))
(Join lineitems brackets (Equal product product) (Between price low high))
```

### `LeftJoin(left right pred …)`

Left outer hash join. Left rows with no match get `NULL` for right columns.

**Example:**

```scheme
(LeftJoin customers orders (Equal custkey custkey))
```

### `AntiJoin(left right pred …)`

Left anti join. Returns left rows with *no* match in right.

**Example:**

```scheme
(AntiJoin parts disqualified_suppliers (Equal suppkey suppkey))
```

---

## Set operations

### `Union(table table …)`

Concatenates two or more tables (bag union — duplicates are preserved, matching SQL `UNION ALL`). Schemas are unified across inputs: numeric columns are widened to a common type (int32 → int64 → float64), and columns absent from a given input are filled with nulls (which surface as `NULL` symbols when materialized). Rows appear in input order: `(Union a b c)` yields a's rows, then b's, then c's. For set-style union without duplicates, follow with `GroupBy` on all columns.

**Example:**

```scheme
(Union (Filter orders (Equal region "US"))
       (Filter orders (Equal region "EU")))
```

---

## Named bindings (DAG-shaped plans)

### `Name(table sym)`

Stores `table` under the symbol `sym` in the engine's name registry and marks it as live for the engine's garbage collector. Returns the table (so it can be threaded through a larger expression).

### `ByName(sym)`

Retrieves the table previously stored under `sym`. This lets the user express shared subplans as named bindings, turning a DAG edge into a tree reference.

Inside a `Filter`, `Project` or `Join` expression, a table with one row and one column stands for its value, so a named aggregate can be used like a variable:

```scheme
(Name (GroupBy readings (Mean heart_rate)) mean_heart_rate)
(Project readings (As (Subtract heart_rate (ByName mean_heart_rate)) deviation))
```

```scheme
(Name (GroupBy rec (Mean heart_rate)) mean_hr)
(Project rec (As (Subtract heart_rate (ByName mean_hr)) deviation))
(Project zones z (As (Divide (Multiply 100.0 |count_all()|) (GroupBy rec (CountAll))) pct))
```

**Example — shared subplan:**

```scheme
(boss-eval (Name (GroupBy lineitem (Min supplycost)) min_cost))
(boss-eval (Filter partsupp (Equal supplycost (ByName min_cost))))
(boss-eval (Filter parts    (Greater retail   (ByName min_cost))))
```

**Example — multi-statement pipeline (the Covid hotspot query):**

```scheme
(Name (Materialize (OrderBy (Project (Load "data.csv") iso_code date) (keys iso_code))) sorted)
(Name (Pairwise (Cumulate (ByName sorted) (Sum cases)) smoothed sum_cases 7) smoothed)
(Join (GroupBy (ByName smoothed) (Max smoothed) date) (ByName smoothed)
      (Equal date date) (Equal max_smoothed smoothed))
```

---

## Materialization

### `Materialize(table)`

Forces evaluation of any deferred plan into a single contiguous Arrow table. Useful when you intend to repeatedly query a result, so the plan is computed once.

### `ToStatus(table)`

Evaluates a pipeline and returns the literal string `"OK"` instead of the materialized result. Useful during profiling: you measure the plan execution time without paying the result-conversion cost (which can dominate for large outputs).

**Example:**

```scheme
(ToStatus (Filter (Load "big.csv") (Greater age 18)))   ; runs the plan, prints "OK"
```

---

## Introspection

### `Schema(table)`

Returns a one-column table whose column is named `Columns` and whose values are the schema-column names of `table` as symbols.

**Example:**

```scheme
(Schema (Table (A 1) (B 2) (C 3)))
;; => (Table (Columns A B C))
```

### `GetEngineDescription()`

Returns a multi-line string listing every supported operator and its signature. The text in `ArrowComputeEngine.cpp` is the source of truth for what this engine reports; keep this catalog and the in-source string in sync.

---

## Reserved scalar / compute heads

These are not standalone operators but appear inside `Filter` predicates and `Project` expressions. They map to Arrow compute kernels and are listed here for reference; an engine wrapping a different library can substitute equivalents.

| BOSS head                | Maps to (Arrow)         | Notes                                      |
|--------------------------|-------------------------|--------------------------------------------|
| `Add`, `Subtract`, `Multiply` | same               | arithmetic                                 |
| `Divide`                 | `divide` on float64     | floating-point division, also for integer operands |
| `IntDivide`              | `divide`                | keeps the operand type: integers divide as integers |
| `Equal`, `NotEqual`      | `equal`, `not_equal`    |                                            |
| `Less`, `LessEqual`      | `less`, `less_equal`    |                                            |
| `Greater`, `GreaterEqual`| `greater`, `greater_equal` |                                         |
| `And`, `Or`              | same                    |                                            |
| `Not`                    | `invert`                |                                            |
| `IfElse`                 | `if_else`               | `(IfElse cond then else)`                  |
| `IsValid`                | `is_valid`              | null check                                 |
| `Int`, `Int64`, `Bool`, `Date` | `cast`              | type casts (`Int` is int32, `Int64` is int64) |
| `Timestamp`              | `boss_to_timestamp` (engine) | cast to `timestamp[s, UTC]`; numbers are seconds since 1970 |
| `Modulo`                 | `boss_modulo` (engine)  | remainder of `x / n`, with the sign of `n`: `(Modulo -7 5)` is 3 |
| `Like`                   | `match_like`            | `(Like col "pat%")`                        |
| `Between`                | (desugared)             | `(Between v lo hi)` → `(And ≥ ≤)`          |
| `Year`, `Month`, `Day`   | same                    | temporal extractors on `timestamp[s, UTC]` |
| `Hour`, `Minute`, `Second` | same                  | time-of-day extractors on `timestamp[s, UTC]` |
| `DayOfWeek`              | `day_of_week`           | Monday = 0                                 |
| `Round`                  | `round`                 | `(Round x ndigits mode)`: round to `ndigits` decimal places (default 0); mode is `HalfToEven` (default), `HalfUp`, `HalfDown`, `Down`, `Up`, `TowardsZero`, `TowardsInfinity`, `HalfTowardsZero`, `HalfTowardsInfinity` or `HalfToOdd` |
| `Strftime`               | `strftime`              | `(Strftime ts format)`: format a timestamp as a string, e.g. `"%d %b %Y"`; `%-d`, `%-H`, … drop the leading zero |
| `Utf8_Trim`, `Utf8_LTrim`, `Utf8_RTrim` | `utf8_trim`, `utf8_ltrim`, `utf8_rtrim` | `(Utf8_LTrim s "0")`: remove the given characters from both ends, the start or the end |

`Round`, `Strftime` and the trim functions take their settings after the operand, either in order (`(Round x 2 Down)`) or by name (`(Round x (Options (ndigits 2) (round_mode Down)))`).

Aggregator heads inside `GroupBy` / `Cumulate`:

| BOSS head     | Maps to (Arrow)              |
|---------------|------------------------------|
| `Sum`         | `sum` (or `hash_sum`)        |
| `Mean`, `Avg` | `mean` / `hash_mean`         |
| `Min`         | `min` / `hash_min`           |
| `Max`         | `max` / `hash_max`           |
| `Count`       | `count` / `hash_count`       |
| `CountAll`    | `count_all` / `hash_count_all` |
| `Median`, `Quantile` | `quantile` per group (GroupBy only) |

(The `hash_` prefix is applied automatically by ArrowComputeEngine when group keys are present.)

---

## Notes on extensibility

A new engine MAY:

- Add operators not in this catalog (`LoadParquet`, `KMeans`, `BFS`, `EmbedText`, …). Use a head name that does not collide with any documented operator.
- Implement only a subset of these operators if its target workload does not need the rest.
- Re-interpret an operator's semantics so long as the deviation is documented in the engine's own `GetEngineDescription` output.

A new engine SHOULD, even if its vocabulary is domain-specific:

- Implement `GetEngineDescription` — it is the BOSS REPL's introspection contract.
- Implement `Schema` and `Table` — these let the user round-trip data through the engine without external files, which is the basis of the test harness pattern in §11 of the implementation guide.
- Implement `ToStatus` — useful for benchmarking and for compositional pipelines where the engine's output feeds into another engine that just needs an OK signal.

An engine MUST avoid:

- Throwing exceptions across the `extern "C"` boundary. Return error strings instead (see implementation guide §8).
- Reusing an operator head with different semantics than this catalog without also overriding `GetEngineDescription` to make the divergence visible to the user.
