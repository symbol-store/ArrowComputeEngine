# ArrowComputeEngine — Claude project notes

A BOSS engine plugin built on Apache Arrow Acero. Single-file implementation; serves as a template for other BOSS engines.

## Documentation map

- `Readme.md` — user-facing build & usage.
- `EngineImplementationGuide.md` — how to implement a new BOSS engine (library-agnostic; covers wrapper engines and purpose-built engines).
- `OperatorCatalog.md` — descriptive per-operator spec for the relational vocabulary this engine implements.
- `TPCHPlan.md` — TPC-H query coverage notes.

**Two-place sync:** when operators are added, renamed, or change semantics, update *all* of:
1. `Source/ArrowComputeEngine.cpp` — the implementation AND the `GetEngineDescription` string at the bottom of the file (this is what users see via `(GetEngineDescription)` at the REPL).
2. `OperatorCatalog.md` — the catalog entry.

## Layout

- `Source/ArrowComputeEngine.cpp` — the entire engine (~400 lines). Dispatch chain at the bottom; helpers (ColumnConverter, intermediates registry, toArrowName, toComputeExpression, buildJoin, withBuilder) above it.
- `Tests/repl-tests.scm` — operator + composition tests on inline data. Fast, no external data needed.
- `Tests/tpch-queries.scm` — TPC-H query plans as `define-syntax` macros, shared between unit tests and benchmarks.
- `Tests/tpch-bench.scm`, `Tests/tpch-sf10-correctness.scm` — require `TPCHData/`.
- `expected_results/*.stable.out` — reference outputs for the TPC-H correctness suite.
- BOSS headers (after build): `Build/deps/include/{BOSS.h,BOSS.hpp,Expression.hpp,ExpressionUtilities.hpp,Engine.hpp}`. `ExpressionUtilities.hpp` contains the pattern-matching DSL (`Transformer`, `Recurse`, `Any_`, `AnySequence_`, the `<` / `>=` / `>` operator chain).

## Build & test

- Build: `cmake --build Build` (or `Build/Debug`, `Build/RelWithDebInfo`).
- Build artifact: `Build/libArrowComputeEngine.so` — loaded via `(SetDefaultEnginePipeline "…")` at the REPL.
- Run all unit tests: `Build/deps/bin/boss Tests/repl-tests.scm`.
- Run TPC-H correctness: `Build/deps/bin/boss Tests/tpch-sf10-correctness.scm` (needs `TPCHData/`).
- Sanitize build: configure with `-DCMAKE_BUILD_TYPE=Sanitize`. Use it after dispatch-chain edits — `std::variant` misuse is the most common source of memory bugs in this codebase.

## Engine conventions worth keeping in mind

- **No abbreviations in identifiers**: `columnName`, not `colName`; `arguments`, not `args`. Full words make grep-driven review tractable on a single-file codebase.
- **Errors propagate as `std::string`** return values from handler lambdas. Never throw across the `extern "C"` boundary.
- **Intermediates are GC'd per call** based on which handles are referenced from `intermediates.names`. If a result "disappears" between calls, the user forgot to wrap it in `(Name … sym)`.
- **The escape hatch**: `Cumulate` and `Pairwise` bypass Acero and call Arrow scalar compute directly because Acero cannot express stateful row-ordered operations. When adding an operator the underlying library cannot plan, follow the same pattern — compute eagerly, re-wrap as a `table_source` Declaration, put it in the registry.
- **Pattern-DSL details** (`<` / `>=` / `>`, sentinels, `Recurse`) live in `EngineImplementationGuide.md` §6. Reach for that before deviating from the existing handler shape.

## Testing harness in a sentence

Chibi Scheme via `Build/deps/bin/boss`; the `test` form takes `(test "name" expected-quoted-expr actual-boss-eval)` and compares with `equal?`. The expected form is a quoted BOSS expression — exactly what would be printed at the REPL.
