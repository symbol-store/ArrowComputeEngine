#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <arrow/api.h>
#include <filesystem>

#include <arrow/acero/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <vector>
using namespace boss::utilities::experimental;
using namespace arrow;
using namespace acero;
using arrow::Table;
using arrow::acero::Declaration;
using boss::ComplexExpression;
using boss::Expression;
using boss::Symbol;

#include "EngineDocumentation.hpp"

class ColumnConverter {
  boss::ExpressionArguments columnValues;

  template <typename T, typename = void> struct has_value : std::false_type {};
  template <typename T>
  struct has_value<T, std::void_t<decltype(std::declval<T>().value)>> : std::true_type {};

public:
  template <typename T> arrow::Status Visit(T& value) {
    if(!value.is_valid)
      columnValues.push_back("NULL"_);
    else if constexpr(has_value<T>::value) {
      if constexpr(std::is_convertible_v<decltype(value.value),
                                         boss::expressions::AtomicExpression>)
        columnValues.push_back(value.value);
      else if constexpr(std::is_same_v<std::remove_reference_t<T>, arrow::StringScalar const>)
        columnValues.push_back(std::string(value.view()));
      else
        columnValues.push_back("ArrowType"_(value.ToString()));
    } else
      columnValues.push_back("ArrowType"_(value.ToString()));
    return arrow::Status::OK();
  }
  boss::ExpressionArguments getColumnValues() && { return std::move(columnValues); };
};

static struct {
  std::unordered_map<size_t, Declaration> intermediates;
  std::unordered_map<boss::Symbol, size_t> names;

  int64_t generateID() {
    static std::default_random_engine generator(std::random_device {}());
    return std::uniform_int_distribution<int64_t>(std::numeric_limits<int64_t>::min(),
                                                  std::numeric_limits<int64_t>::max())(generator);
  };

  boss::Expression name(boss::Expression&& key, boss::Symbol name) {
    names[name] = std::get<int64_t>(key);
    return std::move(key);
  }
  int64_t byName(boss::Symbol name) { return names.at(name); }

  int64_t put(Declaration&& table) {
    auto id = generateID();
    intermediates[id] = std::move(table);
    return id;
  };
  int64_t putTable(std::shared_ptr<arrow::Table> table) {
    return put({"table_source", TableSourceNodeOptions(std::move(table))});
  };
  Declaration const& at(boss::Expression const& key) {
    return std::visit(boss::utilities::overload(
                          [this](int64_t k) -> Declaration const& { return intermediates.at(k); },
                          [](auto const& v) -> Declaration const& {
                            auto ss = std::ostringstream {};
                            ss << "no intermediate found for key " << v
                               << " (input was not produced by the arrow engine)";
                            throw std::runtime_error(ss.str());
                          }),
                      key);
  }

  boss::Expression convertResult(boss::Expression const& key) {
    if(std::holds_alternative<ComplexExpression>(key)) {
      auto const& ce = std::get<ComplexExpression>(key);
      boss::ExpressionArguments newDynamics;
      newDynamics.reserve(ce.getDynamicArguments().size());
      for(auto const& arg : ce.getDynamicArguments())
        newDynamics.push_back(convertResult(arg));
      boss::expressions::ExpressionSpanArguments clonedSpans;
      clonedSpans.reserve(ce.getSpanArguments().size());
      for(auto const& span : ce.getSpanArguments())
        clonedSpans.push_back(std::visit(
            [](auto const& v) -> boss::expressions::ExpressionSpanArgument {
              return v.clone(boss::expressions::CloneReason::EVALUATE_CONST_EXPRESSION);
            },
            span));
      return ComplexExpression(boss::Symbol(ce.getHead()), {}, std::move(newDynamics),
                               std::move(clonedSpans));
    }
    if(!std::holds_alternative<int64_t>(key))
      return key.clone(boss::expressions::CloneReason::EVALUATE_CONST_EXPRESSION);
    auto maybeTable = arrow::acero::DeclarationToTable(at(key), false);
    if(!maybeTable.ok())
      return maybeTable.status().ToStringWithoutContextLines();
    auto table = *maybeTable;
    boss::ExpressionArguments resultExpression;
    for(auto i = 0u; i < table->num_columns(); i++) {
      auto resultColumn = table->column(i);
      auto f = table->field(i);
      auto md = f->metadata();
      bool isSymbolCol =
          md && md->FindKey("boss_type") >= 0 && md->value(md->FindKey("boss_type")) == "symbol";
      boss::ExpressionArguments colValues;
      if(isSymbolCol) {
        for(auto j = 0u; j < resultColumn->length(); j++) {
          auto scalar = resultColumn->GetScalar(j).ValueUnsafe();
          if(!scalar->is_valid)
            colValues.push_back("NULL"_);
          else
            colValues.push_back(boss::Symbol(
                std::string(std::static_pointer_cast<arrow::StringScalar>(scalar)->view())));
        }
      } else {
        auto visitor = ColumnConverter();
        for(auto j = 0u; j < resultColumn->length(); j++)
          (void)arrow::VisitScalarInline(*resultColumn->GetScalar(j).ValueUnsafe(), &visitor);
        colValues = std::move(visitor).getColumnValues();
      }
      resultExpression.push_back(ComplexExpression(boss::Symbol(f->name()), std::move(colValues)));
    }
    return ComplexExpression("Table"_, {}, std::move(resultExpression));
  };
  std::shared_ptr<arrow::Table> getTable(Declaration d) {
    if(d.factory_name == "table_source")
      return dynamic_cast<arrow::acero::TableSourceNodeOptions*>(d.options.get())->table;
    auto maybeTable = DeclarationToTable(d, false);
    if(!maybeTable.ok())
      throw std::runtime_error(maybeTable.status().ToStringWithoutContextLines());
    return *maybeTable;
  };
  std::set<std::string> columnNames(boss::Expression const& key) {
    auto fieldNames = getTable(at(key))->schema()->field_names();
    return {fieldNames.begin(), fieldNames.end()};
  };

} intermediates;

static std::string toArrowName(std::string name) {
  std::transform(name.begin(), name.end(), name.begin(), ::tolower);
  static std::unordered_map<std::string, std::string> const aliases = {
      {"countall", "count_all"},
      {"ifelse", "if_else"},
      {"lessequal", "less_equal"},
      {"greaterequal", "greater_equal"},
      {"notequal", "not_equal"},
      {"not", "invert"},
      {"avg", "mean"},
      {"isvalid", "is_valid"},
  };
  auto it = aliases.find(name);
  return it != aliases.end() ? it->second : name;
}

// Predicate, cast, and value functions accepted inside Filter / Project / Join
// expressions. The catalog is shown by `(GetEngineDescription)`. Keep in sync
// with the special-case branches in `toComputeExpression` below.
struct ExpressionFunctionDescription {
  std::string_view signature;
  std::string_view text;
};
static std::vector<ExpressionFunctionDescription> const& expressionFunctions() {
  static std::vector<ExpressionFunctionDescription> const functions = {
      {"Equal(col val)", "Equality; YYYY-MM-DD literals auto-cast to date32 against date columns"},
      {"NotEqual(col val)", "Inequality (with the same date auto-cast)"},
      {"Less(col val)", "Less-than (with date auto-cast)"},
      {"LessEqual(col val)", "Less-or-equal (with date auto-cast)"},
      {"Greater(col val)", "Greater-than (with date auto-cast)"},
      {"GreaterEqual(col val)", "Greater-or-equal (with date auto-cast)"},
      {"And(pred ...)", "Logical conjunction; n-ary, folded left into binary Arrow calls"},
      {"Or(pred ...)", "Logical disjunction; n-ary, folded left"},
      {"Not(pred)", "Logical negation (Arrow's invert)"},
      {"Add(val ...)", "Arithmetic sum; n-ary, folded left"},
      {"Multiply(val ...)", "Arithmetic product; n-ary, folded left"},
      {"Like(col pattern)", "SQL LIKE; % matches any sequence, _ matches a single character"},
      {"Match_Substring(col str)", "Substring match (case-sensitive)"},
      {"Between(col low high)", "Inclusive range: low <= col <= high"},
      {"IfElse(cond then else)", "Conditional expression"},
      {"IsValid(col)", "True where col is non-null"},
      {"Int(expr)", "Cast expression to int32"},
      {"Bool(expr)", "Cast expression to boolean"},
      {"Date(expr)", "Cast expression to date32"},
      {"Timestamp(expr)", "Cast expression to UTC second-resolution timestamp"},
      {"<any other name>(args ...)",
       "Any Arrow compute function: name is lowercased and dispatched directly"},
  };
  return functions;
}

static compute::Expression toComputeExpression(boss::Expression const& e,
                                               std::set<std::string> const& columns = {}) {
  return std::visit(
      boss::utilities::overload(
          // symbol: column ref if the name exists in the schema, otherwise a symbol-value literal
          [&columns](Symbol const& s) {
            return columns.count(s.getName())
                       ? compute::field_ref(s.getName())
                       : compute::literal(std::make_shared<arrow::StringScalar>(s.getName()));
          },
          [](std::string const& s) {
            return compute::literal(std::make_shared<arrow::StringScalar>(s));
          },
          [&columns](ComplexExpression const& ce) {
            auto name = toArrowName(ce.getHead().getName());
            auto operands = std::vector<compute::Expression>();
            for(auto const& arg : ce.getDynamicArguments()) {
              auto expr = toComputeExpression(arg, columns);
              // auto-cast bare YYYY-MM-DD string literals to date32 in comparisons
              auto* s = std::set<std::string_view> {"less",          "less_equal", "greater",
                                                    "greater_equal", "equal",      "not_equal"}
                                .count(name)
                            ? std::get_if<std::string>(&arg)
                            : nullptr;
              operands.push_back(s && s->size() == 10 && (*s)[4] == '-' && (*s)[7] == '-'
                                     ? compute::call("cast", {std::move(expr)},
                                                     compute::CastOptions::Unsafe(arrow::date32()))
                                     : std::move(expr));
            }
            if(name == "int")
              return compute::call("cast", operands, compute::CastOptions::Unsafe(arrow::int32()));
            else if(name == "date")
              return compute::call("cast", operands, compute::CastOptions::Unsafe(arrow::date32()));
            else if(name == "timestamp")
              return compute::call(
                  "cast", operands,
                  compute::CastOptions::Unsafe(arrow::timestamp(arrow::TimeUnit::SECOND, "UTC")));
            else if(name == "like")
              return compute::call("match_like", {operands[0]},
                                   compute::MatchSubstringOptions {
                                       std::get<std::string>(ce.getDynamicArguments().at(1))});
            else if(name == "match_substring")
              return compute::call("match_substring", {operands[0]},
                                   compute::MatchSubstringOptions {
                                       std::get<std::string>(ce.getDynamicArguments().at(1))});
            else if(name == "bool")
              return compute::call("cast", operands,
                                   compute::CastOptions::Unsafe(arrow::boolean()));
            else if(name == "between")
              return compute::call("and",
                                   {compute::call("greater_equal", {operands[0], operands[1]}),
                                    compute::call("less_equal", {operands[0], operands[2]})});
            // Arrow's and/or/add/multiply are strictly binary. Fold left so users
            // can write (And a b c) or (Add x y z) without hand-nesting.
            else if(operands.size() > 2 &&
                    (name == "and" || name == "or" || name == "add" || name == "multiply")) {
              auto result = compute::call(name, {operands[0], operands[1]});
              for(size_t i = 2; i < operands.size(); ++i)
                result = compute::call(name, {std::move(result), operands[i]});
              return result;
            } else
              return compute::call(name, operands);
          },
          [](auto v) { return compute::literal(v); }),
      e);
}

template <typename T, typename F> static void withBuilder(F&& use) {
  if constexpr(std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>)
    use(arrow::Int64Builder {}, arrow::int64());
  else if constexpr(std::is_same_v<T, double_t>)
    use(arrow::DoubleBuilder {}, arrow::float64());
  else if constexpr(std::is_same_v<T, float_t>)
    use(arrow::FloatBuilder {}, arrow::float32());
  else if constexpr(std::is_same_v<T, std::string>)
    use(arrow::StringBuilder {}, arrow::utf8());
  else if constexpr(std::is_same_v<T, bool>)
    use(arrow::BooleanBuilder {}, arrow::boolean());
  else
    throw std::runtime_error("unsupported column type: " + std::string(typeid(T).name()));
}

static boss::Expression buildJoin(JoinType joinType,
                                  boss::expressions::ExpressionArguments& dynamics) {
  auto leftKeys = std::vector<FieldRef>(), rightKeys = std::vector<FieldRef>();
  auto& left = dynamics.at(0);
  auto& right = dynamics.at(1);
  auto cols = intermediates.columnNames(left);
  auto rightCols = intermediates.columnNames(right);
  cols.insert(rightCols.begin(), rightCols.end());
  auto filterExprs = std::vector<compute::Expression>();
  for(auto i = 2u; i < dynamics.size(); ++i) {
    auto const& ce = get<ComplexExpression>(dynamics.at(i));
    if(ce.getHead().getName() == "Equal") {
      auto const& args = ce.getDynamicArguments();
      leftKeys.push_back(get<Symbol>(args.at(0)).getName());
      rightKeys.push_back(get<Symbol>(args.at(1)).getName());
    } else {
      filterExprs.push_back(toComputeExpression(dynamics.at(i), cols));
    }
  }
  compute::Expression filter = filterExprs.empty() ? literal(true) : filterExprs[0];
  for(auto i = 1u; i < filterExprs.size(); ++i)
    filter = compute::call("and", {filter, filterExprs[i]});
  if(leftKeys.empty()) {
    const std::string dummyKey = "__cross_join_key__";
    auto addDummy = [&](Declaration src, std::vector<std::string> const& fields) {
      auto exprs = std::vector<compute::Expression>();
      auto names = std::vector<std::string>(fields);
      for(auto const& f : fields)
        exprs.push_back(compute::field_ref(f));
      exprs.push_back(compute::literal(0));
      names.push_back(dummyKey);
      return Declaration::Sequence({std::move(src), {"project", ProjectNodeOptions(exprs, names)}});
    };
    auto leftFields = intermediates.getTable(intermediates.at(left))->schema()->field_names();
    auto rightFields = intermediates.getTable(intermediates.at(right))->schema()->field_names();
    auto leftOutput = std::vector<FieldRef>(leftFields.begin(), leftFields.end());
    auto rightOutput = std::vector<FieldRef>(rightFields.begin(), rightFields.end());
    return boss::Expression {intermediates.put(
        {"hashjoin",
         {addDummy(intermediates.at(left), leftFields),
          addDummy(intermediates.at(right), rightFields)},
         HashJoinNodeOptions(joinType, {FieldRef(dummyKey)}, {FieldRef(dummyKey)}, leftOutput,
                             rightOutput, filter, "_l", "_r", true)})};
  }
  return boss::Expression {intermediates.put(
      {"hashjoin",
       {intermediates.at(left), intermediates.at(right)},
       HashJoinNodeOptions(joinType, leftKeys, rightKeys, filter, "_l", "_r", true)})};
}

static boss::Expression evaluate(boss::Expression&& e) {
  using boss::utilities::experimental::sentinel::Any_;
  using boss::utilities::experimental::sentinel::AnySequence_;
  static auto _ = compute::Initialize();
  return std::move(e) //
         <"Slice"_(AnySequence_) >= Description("Fetch a contiguous slice of rows")>
             Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           return intermediates.put(Declaration::Sequence(
               {intermediates.at(dynamics.at(0)),
                {"fetch", FetchNodeOptions(get<int>(dynamics.at(1)), get<int>(dynamics.at(2)))}}));
         } < "OrderBy"_(AnySequence_) >=
         Description("Sort rows by one or more columns; wrap a column in (Desc "
                     "col) to sort descending") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto orderKeys = std::vector<compute::SortKey>();
           for(auto& it : get<ComplexExpression>(dynamics.at(1)).getDynamicArguments()) {
             auto* sym = std::get_if<Symbol>(&it);
             if(sym) {
               orderKeys.push_back(compute::SortKey(sym->getName()));
             } else {
               auto const& ce = get<ComplexExpression>(it);
               orderKeys.push_back(compute::SortKey(
                   get<Symbol>(ce.getDynamicArguments().at(0)).getName(),
                   ce.getHead().getName() == "Desc" ? compute::SortOrder::Descending
                                                    : compute::SortOrder::Ascending));
             }
           }
           return intermediates.put(
               Declaration::Sequence({intermediates.at(dynamics.at(0)),
                                      {"order_by", OrderByNodeOptions(Ordering(orderKeys))}}));
         } < "Join"_(AnySequence_) >=
         Description("Hash join; (Equal lCol rCol) defines equi-join keys, "
                     "other predicates "
                     "become residual filters; colliding names get _l/_r "
                     "suffixes; no Equal "
                     "predicates degrades to O(n^2) cross-join") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) { return buildJoin(JoinType::INNER, dynamics); } <
         "LeftJoin"_(AnySequence_) >=
         Description("Left outer hash join; same predicate syntax as Join") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) { return buildJoin(JoinType::LEFT_OUTER, dynamics); } <
         "AntiJoin"_(AnySequence_) >= Description("Left anti join: rows in left with no match in "
                                                  "right; same predicate syntax as Join") >
         Recurse(evaluate) >
         [](auto, auto dynamics, auto) { return buildJoin(JoinType::LEFT_ANTI, dynamics); } <
         "Union"_(AnySequence_) >=
         Description("Concatenate two or more tables (bag union, like SQL UNION ALL); "
                     "schemas are unified across inputs, with columns absent from a "
                     "given input filled with nulls; rows appear in input order") >
         Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto tables = std::vector<std::shared_ptr<arrow::Table>>();
           for(auto& key : dynamics)
             tables.push_back(intermediates.getTable(intermediates.at(key)));
           arrow::ConcatenateTablesOptions concatOptions;
           concatOptions.unify_schemas = true;
           auto maybeTable = arrow::ConcatenateTables(tables, concatOptions);
           if(!maybeTable.ok())
             return boss::Expression {maybeTable.status().ToStringWithoutContextLines()};
           return boss::Expression {intermediates.putTable(*maybeTable)};
         } < "Name"_(AnySequence_) >=
         Description("Store a table under a named handle for later retrieval") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           return intermediates.name(std::move(dynamics.at(0)), get<boss::Symbol>(dynamics.at(1)));
         } < "ByName"_(AnySequence_) >= Description("Retrieve a previously named table") >
         Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           return intermediates.byName(get<boss::Symbol>(dynamics.at(0)));
         } < "Filter"_(Any_, Any_) >=
         Description("Keep only rows where pred holds; supports And, Or, Not "
                     "and all Arrow "
                     "comparison/compute functions") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto columns = intermediates.columnNames(dynamics.at(0));
           return intermediates.put(Declaration::Sequence(
               {intermediates.at(dynamics.at(0)),
                {"filter", FilterNodeOptions(toComputeExpression(dynamics.at(1), columns))}}));
         } < "Project"_(AnySequence_) >=
         Description("Select and rename columns; supports (As expr newName) "
                     "aliasing, (As (Int "
                     "col) newName)/(As (Bool col) newName) for type casts, "
                     "and arbitrary Arrow "
                     "compute functions in the expression") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto columns = intermediates.columnNames(dynamics.at(0));
           auto projections = std::vector<compute::Expression>();
           auto names = std::vector<std::string>();
           for(auto i = 1; i < dynamics.size(); i++)
             visit(boss::utilities::overload(
                       [&](Symbol&& s) {
                         projections.push_back(compute::field_ref(s.getName()));
                         names.push_back(s.getName());
                       },
                       [&](ComplexExpression&& s) {
                         if(s.getHead().getName() == "As") {
                           auto const& args = s.getDynamicArguments();
                           projections.push_back(toComputeExpression(args.at(0), columns));
                           names.push_back(get<Symbol>(args.at(1)).getName());
                         } else {
                           auto expr =
                               toComputeExpression(boss::Expression {std::move(s)}, columns);
                           projections.push_back(expr);
                           names.push_back(expr.ToString());
                         }
                       },
                       [](auto&&) {}),
                   std::move(dynamics.at(i)));
           return intermediates.put(
               Declaration::Sequence({intermediates.at(dynamics.at(0)),
                                      {"project", ProjectNodeOptions(projections, names)}}));
         } < "GroupBy"_(AnySequence_) >=
         Description("Aggregate a column (Sum, Mean, Max, CountAll, ...). "
                     "Without keys: global "
                     "aggregate; with keys: hash-aggregate") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto aggregates = std::vector<compute::Aggregate>();
           auto keys = std::vector<FieldRef>();
           auto i = 1u;
           for(; i < dynamics.size() && std::holds_alternative<ComplexExpression>(dynamics.at(i));
               ++i) {
             auto const& fn = get<ComplexExpression>(dynamics.at(i));
             auto const& args = fn.getDynamicArguments();
             auto const functionName = toArrowName(fn.getHead().getName());
             if(args.empty()) {
               aggregates.emplace_back(functionName, functionName + "()");
             } else {
               auto const col = get<Symbol>(args.at(0));
               aggregates.push_back(
                   {functionName, col.getName(), functionName + "(" + col.getName() + ")"});
             }
           }
           for(; i < dynamics.size(); ++i)
             keys.push_back(get<Symbol>(dynamics.at(i)).getName());
           if(!keys.empty())
             for(auto& agg : aggregates)
               agg.function = "hash_" + agg.function;
           auto aggDecl =
               Declaration::Sequence({intermediates.at(dynamics.at(0)),
                                      {"aggregate", AggregateNodeOptions(aggregates, keys)}});
           if(!keys.empty()) {
             auto maybeTable = DeclarationToTable(aggDecl, false);
             if(!maybeTable.ok())
               return boss::Expression {maybeTable.status().ToStringWithoutContextLines()};
             return boss::Expression {intermediates.putTable(*maybeTable)};
           }
           return boss::Expression {intermediates.put(std::move(aggDecl))};
         } < "Cumulate"_(AnySequence_) >=
         Description("Running (prefix) aggregate, e.g. cumulative sum") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto const& aggregationFunction = get<ComplexExpression>(dynamics.at(1));
           auto const aggregationAttribute =
               get<Symbol>(aggregationFunction.getDynamicArguments().at(0));
           auto const functionName = toArrowName(aggregationFunction.getHead().getName());
           auto input = intermediates.getTable(intermediates.at(dynamics.at(0)));
           auto maybeResult =
               compute::CallFunction("cumulative_" + functionName,
                                     {input->GetColumnByName(aggregationAttribute.getName())});
           if(!maybeResult.ok())
             return boss::Expression {maybeResult.status().ToStringWithoutContextLines()};
           auto result = maybeResult->chunked_array();
           auto maybeTable = input->AddColumn(
               input->num_columns(),
               field(functionName + "(" + aggregationAttribute.getName() + ")", result->type()),
               result);
           if(!maybeTable.ok())
             return boss::Expression {maybeTable.status().ToStringWithoutContextLines()};
           return boss::Expression {intermediates.putTable(*maybeTable)};
         } < "Pairwise"_(AnySequence_) >=
         Description("Sliding-window difference: out[i] = in[i+lag] - in[i]") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto options = compute::PairwiseOptions(get<int>(dynamics.at(3)));
           auto input = intermediates.getTable(intermediates.at(dynamics.at(0)));
           auto column = get<Symbol>(dynamics.at(2)).getName();
           auto combinedInput = input;
           if(input->GetColumnByName(column)->num_chunks() != 1) {
             auto maybeCombined = input->CombineChunks();
             if(!maybeCombined.ok())
               return boss::Expression {maybeCombined.status().ToStringWithoutContextLines()};
             combinedInput = *maybeCombined;
           }
           auto inputArray = combinedInput->GetColumnByName(column);
           auto maybeResult =
               compute::CallFunction("pairwise_diff", {inputArray->chunk(0)}, &options);
           if(!maybeResult.ok())
             return boss::Expression {maybeResult.status().ToStringWithoutContextLines()};
           auto maybeChunked = ChunkedArray::Make({maybeResult->make_array()});
           if(!maybeChunked.ok())
             return boss::Expression {maybeChunked.status().ToStringWithoutContextLines()};
           auto maybeTable = input->AddColumn(
               input->num_columns(),
               field(get<Symbol>(dynamics.at(1)).getName(), maybeResult->type()), *maybeChunked);
           if(!maybeTable.ok())
             return boss::Expression {maybeTable.status().ToStringWithoutContextLines()};
           return boss::Expression {intermediates.putTable(*maybeTable)};
         } < "ToStatus"_(AnySequence_) >=
         Description("Evaluate a pipeline and return OK rather than "
                     "materialising the result") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           return DeclarationToStatus(intermediates.at(dynamics.at(0)), false).CodeAsString();
         } < "Materialize"_(AnySequence_) >=
         Description("Force materialisation of chunked Arrow arrays into a "
                     "single contiguous buffer") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto maybeTable =
               intermediates.getTable(intermediates.at(dynamics.at(0)))->CombineChunks();
           if(!maybeTable.ok())
             return boss::Expression {maybeTable.status().ToStringWithoutContextLines()};
           return boss::Expression {intermediates.putTable(*maybeTable)};
         } < "Schema"_(AnySequence_) >=
         Description("Return a one-column table listing the column names of "
                     "table as symbols") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto input = intermediates.getTable(intermediates.at(dynamics.at(0)));
           auto builder = arrow::StringBuilder {};
           for(auto i = 0; i < input->num_columns(); i++)
             (void)builder.Append(input->field(i)->name());
           return intermediates.putTable(arrow::Table::Make(
               arrow::schema({arrow::field("Columns", arrow::utf8(), true,
                                           arrow::key_value_metadata({"boss_type"}, {"symbol"}))}),
               {*builder.Finish()}));
         } < "Table"_(AnySequence_) >=
         Description("Construct an in-memory table from literal column "
                     "data; symbol values are stored as named nulls") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto fields = std::vector<std::shared_ptr<arrow::Field>>();
           auto arrays = std::vector<std::shared_ptr<arrow::Array>>();
           for(auto& columnExpr : dynamics) {
             auto& column = get<ComplexExpression>(columnExpr);
             auto columnName = column.getHead().getName();
             auto& spanArguments = column.getSpanArguments();
             if(!spanArguments.empty()) {
               auto firstTyped =
                   std::find_if(spanArguments.begin(), spanArguments.end(), [](auto const& span) {
                     return std::visit(
                         [](auto const& s) {
                           return !std::is_same_v<Symbol, std::remove_const_t<typename std::decay_t<
                                                              decltype(s)>::element_type>>;
                         },
                         span);
                   });
               if(firstTyped != spanArguments.end())
                 std::visit(
                     [&](auto& typedSource) {
                       using Scalar = std::remove_const_t<
                           typename std::decay_t<decltype(typedSource)>::element_type>;
                       withBuilder<Scalar>([&](auto&& builder, auto type) {
                         for(auto& spanArg : spanArguments)
                           std::visit(
                               [&](auto& source) {
                                 using SourceScalar = std::remove_const_t<
                                     typename std::decay_t<decltype(source)>::element_type>;
                                 if constexpr(std::is_same_v<SourceScalar, Symbol>)
                                   for(auto i = 0u; i < source.size(); ++i)
                                     (void)builder.AppendNull();
                                 else if constexpr(std::is_same_v<SourceScalar, Scalar>)
                                   for(auto const& value : source)
                                     (void)builder.Append(value);
                               },
                               spanArg);
                         arrays.push_back(*builder.Finish());
                         fields.push_back(arrow::field(columnName, type));
                       });
                     },
                     *firstTyped);
               else {
                 auto builder = arrow::StringBuilder {};
                 for(auto& span : spanArguments)
                   std::visit(
                       [&](auto& s) {
                         using Scalar =
                             std::remove_const_t<typename std::decay_t<decltype(s)>::element_type>;
                         if constexpr(std::is_same_v<Scalar, Symbol>)
                           for(auto& sym : s)
                             (void)builder.Append(sym.getName());
                       },
                       span);
                 arrays.push_back(*builder.Finish());
                 fields.push_back(
                     arrow::field(columnName, arrow::utf8(), true,
                                  arrow::key_value_metadata({"boss_type"}, {"symbol"})));
               }
             } else {
               auto& dynamicArguments = column.getDynamicArguments();
               auto firstNonNull =
                   std::find_if(dynamicArguments.begin(), dynamicArguments.end(),
                                [](auto const& v) { return !std::holds_alternative<Symbol>(v); });
               if(firstNonNull != dynamicArguments.end())
                 visit(boss::utilities::overload(
                           [&]<typename T>(T const&)
                               requires(std::is_arithmetic_v<T> || std::is_same_v<T, std::string>) {
                                 withBuilder<T>([&](auto&& builder, auto type) {
                                   for(auto& argument : dynamicArguments)
                                     if(std::holds_alternative<Symbol>(argument))
                                       (void)builder.AppendNull();
                                     else
                                       (void)builder.Append(get<T>(argument));
                                   arrays.push_back(*builder.Finish());
                                   fields.push_back(arrow::field(columnName, type));
                                 });
                               },
                           [](auto&&) {}),
                       *firstNonNull);
               else {
                 auto builder = arrow::StringBuilder {};
                 for(auto& argument : dynamicArguments)
                   (void)builder.Append(get<Symbol>(argument).getName());
                 arrays.push_back(*builder.Finish());
                 fields.push_back(
                     arrow::field(columnName, arrow::utf8(), true,
                                  arrow::key_value_metadata({"boss_type"}, {"symbol"})));
               }
             }
           }
           return intermediates.putTable(arrow::Table::Make(arrow::schema(fields), arrays));
         } < "Load"_(AnySequence_) >=
         Description("Read a CSV file, or every .csv/.tbl in a directory unioned "
                     "with a 'file name' column") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           return std::visit(
               boss::utilities::overload(
                   [&dynamics](std::string&& path) -> boss::Expression {
                     std::vector<std::string> columnNames;
                     for(auto i = 1u; i < dynamics.size(); ++i)
                       columnNames.push_back(get<Symbol>(dynamics.at(i)).getName());

                     auto loadOne = [&](std::string const& filePath)
                         -> arrow::Result<std::shared_ptr<arrow::Table>> {
                       auto readOptions = csv::ReadOptions::Defaults();
                       auto parseOptions = csv::ParseOptions::Defaults();
                       auto convertOptions = csv::ConvertOptions::Defaults();
                       readOptions.use_threads = false;
                       readOptions.block_size = 1 << 26;
                       bool isTbl = filePath.ends_with(".tbl");
                       if(isTbl)
                         parseOptions.delimiter = '|';
                       if(!columnNames.empty()) {
                         auto withTrailing = columnNames;
                         if(isTbl)
                           withTrailing.push_back("_trailing");
                         readOptions.column_names = withTrailing;
                         convertOptions.include_columns = columnNames;
                       }
                       ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(filePath));
                       ARROW_ASSIGN_OR_RAISE(auto reader,
                                             csv::TableReader::Make(io::default_io_context(), file,
                                                                    readOptions, parseOptions,
                                                                    convertOptions));
                       return reader->Read();
                     };

                     namespace fs = std::filesystem;
                     std::error_code errorCode;
                     bool isDirectory = fs::is_directory(path, errorCode);
                     if(errorCode)
                       return std::string("cannot stat ") + path + ": " + errorCode.message();

                     auto build = [&]() -> arrow::Result<std::shared_ptr<arrow::Table>> {
                       if(!isDirectory) {
                         ARROW_ASSIGN_OR_RAISE(auto table, loadOne(path));
                         return table->CombineChunks();
                       }

                       std::vector<std::string> filePaths;
                       for(auto const& entry : fs::directory_iterator(path)) {
                         if(!entry.is_regular_file())
                           continue;
                         auto extension = entry.path().extension().string();
                         if(extension == ".csv" || extension == ".tbl")
                           filePaths.push_back(entry.path().string());
                       }
                       if(filePaths.empty())
                         return arrow::Table::MakeEmpty(arrow::schema({}));

                       auto fileNameField =
                           arrow::field("file name", arrow::utf8(), true,
                                        arrow::key_value_metadata({"boss_type"}, {"symbol"}));

                       std::vector<std::shared_ptr<arrow::Table>> tables;
                       tables.reserve(filePaths.size());
                       for(auto const& filePath : filePaths) {
                         ARROW_ASSIGN_OR_RAISE(auto table, loadOne(filePath));
                         auto baseName = fs::path(filePath).filename().string();
                         ARROW_ASSIGN_OR_RAISE(
                             auto fileNameArray,
                             arrow::MakeArrayFromScalar(arrow::StringScalar(baseName),
                                                        table->num_rows()));
                         ARROW_ASSIGN_OR_RAISE(
                             table, table->AddColumn(
                                        table->num_columns(), fileNameField,
                                        std::make_shared<arrow::ChunkedArray>(fileNameArray)));
                         tables.push_back(table);
                       }

                       arrow::ConcatenateTablesOptions concatOptions;
                       concatOptions.unify_schemas = true;
                       ARROW_ASSIGN_OR_RAISE(auto concatenated,
                                             arrow::ConcatenateTables(tables, concatOptions));
                       return concatenated->CombineChunks();
                     };

                     auto maybeTable = build();
                     if(!maybeTable.ok())
                       return maybeTable.status().ToStringWithoutContextLines();
                     return intermediates.putTable(*maybeTable);
                   },
                   [](auto&& e) -> boss::Expression { return e; }),
               std::move(dynamics.at(0)));
         } < "GetEngineDescription"_(AnySequence_) >=
         Description("Return this operator description string") > Recurse(evaluate) >
         [](auto, auto, auto) {
           auto const& operators = operatorDescriptions();
           auto const& functions = expressionFunctions();
           size_t maxWidth = 0;
           for(auto const& entry : operators) {
             auto width = entry.head.size() + entry.signature.size() + 2;
             if(width > maxWidth)
               maxWidth = width;
           }
           for(auto const& entry : functions) {
             if(entry.signature.size() > maxWidth)
               maxWidth = entry.signature.size();
           }
           std::string output = "\nOperators:\n";
           for(auto const& entry : operators) {
             std::string signature = entry.head + "(" + entry.signature + ")";
             output += "  " + signature + std::string(maxWidth + 2 - signature.size(), ' ') +
                       entry.text + "\n";
           }
           output += "\nExpression functions (used inside Filter / Project / Join predicates):\n";
           for(auto const& entry : functions) {
             output += "  " + std::string(entry.signature) +
                       std::string(maxWidth + 2 - entry.signature.size(), ' ') +
                       std::string(entry.text) + "\n";
           }
           return output;
         } < Any_ >= Recurse(evaluate);
};

extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
  auto result = new BOSSExpression {
      .delegate = intermediates.convertResult(evaluate(std::move(e->delegate)))};
  auto live = std::set<size_t>();
  for(auto& [name, key] : intermediates.names)
    live.insert(key);
  std::erase_if(intermediates.intermediates, [&](auto& kv) { return !live.count(kv.first); });
  return result;
};
