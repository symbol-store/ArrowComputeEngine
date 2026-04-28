# ArrowComputeEngine

A data processing engine plugin for the [BOSS](https://github.com/symbol-store/BOSS) framework, built on top of [Apache Arrow](https://arrow.apache.org/). It exposes a small but meaningful set of relational operators that can be composed into query pipelines and evaluated, e.g., through BOSS's Chibi Scheme REPL.

The engine was originally developed to support a **performance engineering coursework** in which students profile the Arrow-based operator pipeline, extract microbenchmarks, and compare it against a hand-fused loop implementation. The query used throughout is a real analysis: computing per-day global Covid-19 hotspots (the country with the highest population-normalised case load on each date) from data published by [Our World in Data](https://ourworldindata.org/covid-cases).

---

## Supported Operators

| Operator | Description |
|---|---|
| `Load(path)` | Read a CSV file into an in-memory Arrow table |
| `Project(table col ...)` | Select and rename columns; supports `(int col)` for type casts and arbitrary Arrow compute functions |
| `OrderBy(table (keys col ...))` | Sort rows by one or more columns |
| `GroupBy(table (agg col) [key])` | Aggregate a column (`sum`, `mean`, `max`, `count`, …). Without `key` it is a global aggregate; with `key` it is a hash-aggregate |
| `Cumulate(table (agg col))` | Running (prefix) aggregate, e.g. cumulative sum |
| `Pairwise(table out-col in-col lag)` | Sliding-window difference: `out[i] = in[i+lag] − in[i]` |
| `Join(left (keys ...) right (keys ...))` | Inner hash join on matching key columns |
| `Name(table sym)` | Store a table under a named handle for later retrieval |
| `ByName(sym)` | Retrieve a previously named table |
| `Materialize(table)` | Force materialisation of chunked Arrow arrays into a single contiguous buffer |
| `Slice(table offset count)` | Fetch a contiguous slice of rows |
| `ToStatus(table)` | Evaluate a pipeline and return `"OK"` rather than materialising the result (useful during profiling) |

---

## Dependencies

- C++23-capable compiler (clang-18 or later recommended)
- CMake ≥ 3.28
- Internet access at configure time (Arrow and BOSS are fetched automatically)

The build system fetches and compiles:
- **BOSS** – the expression-oriented data management framework that provides the Chibi Scheme REPL and the plugin interface
- **Apache Arrow 23.0.0** – columnar in-memory format and compute kernels (CSV reader, Acero query engine, compute functions)

---

## Building

```bash
git clone https://github.com/symbol-store/ArrowComputeEngine.git
cmake ArrowComputeEngine -BBuild/Debug -DCMAKE_BUILD_TYPE=Debug
cmake --build Build/Debug
```

For a profiling-friendly build with optimisations and debug symbols:

```bash
cmake ArrowComputeEngine -BBuild/RelWithDebInfo -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build Build/RelWithDebInfo -j
```

On systems where clang is not the default compiler, pass the compiler explicitly:

```bash
CC=clang-18 CXX=clang++-18 cmake ArrowComputeEngine -BBuild/RelWithDebInfo -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build Build/RelWithDebInfo -j
```

---

## Trying It Out

### 1. Download the data

```bash
curl -sL 'https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv?raw=true' \
  | cut -d, -f1,4,12 > owid-covid-data.csv
```

To create a larger dataset suitable for benchmarking (replace `$USER` with your username):

```bash
mkdir -p /data/$USER
cat owid-covid-data.csv > /data/$USER/owid-covid-data-large.csv
for i in {1..200}; do tail +2 owid-covid-data.csv >> /data/$USER/owid-covid-data-large.csv; done
```

### 2. Start the BOSS REPL

```bash
  ./Build/RelWithDebInfo/BOSS-prefix/src/BOSS-build/deps/bin/chibi-scheme -mBOSS
```

### 3. Load the engine and data

At the REPL prompt (`>`):

```scheme
(begin
  (boss-eval (SetDefaultEnginePipeline "./Build/RelWithDebInfo/libArrowComputeEngine.so"))
  (boss-eval (ToStatus
    (Name
      (Materialize
        (OrderBy
          (Project (Load "owid-covid-data.csv")
            iso_code date (int date) new_cases_per_million)
          (keys iso_code |int(date)|)))
      sorted)))
  (boss-eval (GroupBy (ByName sorted) (count date))))
```

### 4. Run the hotspot query

```scheme
(boss-eval
  (Join
    (GroupBy
      (Name
        (Pairwise
          (Cumulate (ByName sorted) (sum new_cases_per_million))
          smoothed_new_cases_per_million sum_new_cases_per_million 7)
        smoothed)
      (max smoothed_new_cases_per_million) date)
    (keys date max_smoothed_new_cases_per_million)
    (ByName smoothed)
    (keys date smoothed_new_cases_per_million)))
```

This pipeline:
1. Computes a cumulative sum of `new_cases_per_million` per country
2. Applies a 7-day pairwise difference to get a smoothed daily figure
3. Finds the maximum smoothed value across all countries for each date
4. Joins back to find which country held that maximum on each date

### 5. One-liner version

The same query can be run as a single shell command:

```bash
./Build/Debug/deps/bin/chibi-scheme -mBOSS \
  -p'(boss-eval (SetDefaultEnginePipeline "Build/Debug/libArrowComputeEngine.so"))' \
  -p'(boss-eval (ToStatus (Name (Materialize (OrderBy (Project (Load "owid-covid-data.csv") iso_code date (int date) new_cases_per_million) (keys iso_code |int(date)|))) sorted)))' \
  -p'(boss-eval (Join (GroupBy (Name (Pairwise (Cumulate (ByName sorted) (sum new_cases_per_million)) smoothed_new_cases_per_million sum_new_cases_per_million 7) smoothed) (max smoothed_new_cases_per_million) date) (keys date max_smoothed_new_cases_per_million) (ByName smoothed) (keys date smoothed_new_cases_per_million)))'
```

---

## Architecture

The engine is a single shared library (`libArrowComputeEngine.so`) that exposes one entry point: `evaluate(BOSSExpression*)`. BOSS dispatches expressions to this function via its plugin mechanism.

Internally, the engine uses a **pattern-matching dispatch table** (from BOSS's expression utilities) to map each operator head to a handler. Most operators translate directly to [Arrow Acero](https://arrow.apache.org/docs/cpp/streaming_execution.html) query plan nodes (`Declaration` objects) and store the resulting plan fragment in an in-memory registry keyed by a random `int64_t` handle. Final results are materialised on demand when `convertResult` is called.

`Cumulate` and `Pairwise` are exceptions: they call Arrow scalar compute functions directly and re-wrap the result as an `Acero` `table_source` node, since Acero does not natively support stateful row-order-dependent operations.

---

## Coding Conventions

- **No abbreviations in identifiers.** Write `functionName`, not `fnName`; `arguments`, not `args`; `columnName`, not `colName`. Full words make the code self-documenting and easier to search.

---

## Project Structure

```
ArrowComputeEngine/
├── CMakeLists.txt              # build configuration; fetches BOSS and Arrow
└── Source/
    └── ArrowComputeEngine.cpp  # all engine logic (~250 lines)
```
