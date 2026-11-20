#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <arrow/api.h>
#include <cmath>
#include <deque>
#include <filesystem>

#include <arrow/acero/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <ranges>
#include <regex>
#include <set>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <vector>
using namespace boss::utilities::experimental;
using boss::utilities::experimental::sentinel::Any_;
using boss::utilities::experimental::sentinel::AnySequence_;
using boss::utilities::experimental::sentinel::Integer_;
using boss::utilities::experimental::sentinel::String_;
using boss::utilities::experimental::sentinel::Symbol_;
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

  int64_t put(Declaration&& table) {
    auto id = generateID();
    intermediates[id] = std::move(table);
    return id;
  };
  int64_t putTable(std::shared_ptr<arrow::Table> table) {
    return put({"table_source", TableSourceNodeOptions(std::move(table))});
  };
  bool contains(boss::Expression const& key) const {
    return std::holds_alternative<int64_t>(key) && intermediates.contains(std::get<int64_t>(key));
  }
  Declaration const& at(boss::Expression const& key) {
    return intermediates.at(std::get<int64_t>(key));
  }

  // Replaces every table handle in key, at any depth, by its (Table …) expression.
  boss::Expression convertResult(boss::Expression&& key) {
    if(std::holds_alternative<ComplexExpression>(key)) {
      auto [head, statics, dynamics, spans] =
          std::get<ComplexExpression>(std::move(key)).decompose();
      for(auto& argument : dynamics)
        argument = convertResult(std::move(argument));
      return ComplexExpression(std::move(head), std::move(statics), std::move(dynamics),
                               std::move(spans));
    }
    if(!contains(key))
      return std::move(key);
    auto maybeTable = arrow::acero::DeclarationToTable(at(key), false);
    if(!maybeTable.ok())
      return maybeTable.status().ToStringWithoutContextLines();
    auto table = *maybeTable;
    boss::ExpressionArguments resultExpression;
    for(auto i : std::views::iota(0, table->num_columns())) {
      auto resultColumn = table->column(i);
      auto f = table->field(i);
      auto md = f->metadata();
      bool isSymbolCol =
          md && md->FindKey("boss_type") >= 0 && md->value(md->FindKey("boss_type")) == "symbol";
      boss::ExpressionArguments colValues;
      if(isSymbolCol) {
        for(auto j : std::views::iota(int64_t(0), resultColumn->length())) {
          auto scalar = resultColumn->GetScalar(j).ValueUnsafe();
          if(!scalar->is_valid)
            colValues.push_back("NULL"_);
          else
            colValues.push_back(boss::Symbol(
                std::string(std::static_pointer_cast<arrow::StringScalar>(scalar)->view())));
        }
      } else {
        auto visitor = ColumnConverter();
        for(auto j : std::views::iota(int64_t(0), resultColumn->length()))
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

enum class TableArguments { First, FirstTwo, All };

// Leaves an operator unevaluated, with its arguments evaluated, when a table argument is not a
// handle in the registry, e.g. because another engine produced it or a previous call released it
template <typename Handler> struct OperatorOnIntermediates {
  TableArguments tableArguments;
  Handler handler;

  boss::Expression operator()(boss::Symbol&& head, auto&& statics,
                              boss::ExpressionArguments&& dynamics, auto&& spans) {
    auto tableArgumentCount = tableArguments == TableArguments::First      ? std::size_t(1)
                              : tableArguments == TableArguments::FirstTwo ? std::size_t(2)
                                                                           : dynamics.size();
    if(std::ranges::all_of(dynamics | std::views::take(tableArgumentCount),
                           [](auto const& argument) { return intermediates.contains(argument); }))
      return handler(std::move(statics), std::move(dynamics), std::move(spans));
    return ComplexExpression(std::move(head), std::move(statics), std::move(dynamics),
                             std::move(spans));
  }
};
template <typename Handler>
OperatorOnIntermediates(TableArguments, Handler) -> OperatorOnIntermediates<Handler>;

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
      {"dayofweek", "day_of_week"},
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
      {"Divide(a b)", "Floating-point division, also for integer operands"},
      {"IntDivide(a b)", "Division that keeps the operand type; integers divide as integers"},
      {"Int(expr)", "Cast expression to int32"},
      {"Int64(expr)", "Cast expression to int64"},
      {"Bool(expr)", "Cast expression to boolean"},
      {"Date(expr)", "Cast expression to date32"},
      {"Timestamp(expr)",
       "Cast expression to UTC second-resolution timestamp; numbers are seconds since 1970"},
      {"Hour(ts) / Minute(ts) / Second(ts)", "Hour, minute or second of a timestamp"},
      {"Year(ts) / Month(ts) / Day(ts)", "Year, month or day of a timestamp or date"},
      {"DayOfWeek(ts)", "Day of the week of a timestamp or date (Monday = 0)"},
      {"Modulo(x n)", "Remainder of x divided by n, with the sign of n"},
      {"Round(x [ndigits] [mode])",
       "Round to ndigits decimal places (default 0) using the rounding mode (default HalfToEven)"},
      {"Strftime(ts format)",
       "Format a timestamp as a string, e.g. \"%d %b %Y\"; \"%-d\", \"%-H\", ... drop the "
       "leading zero"},
      {"Utf8_Trim(str chars) / Utf8_LTrim / Utf8_RTrim",
       "Remove the given characters from both ends, the start or the end of a string"},
      {"<any other name>(args ...)",
       "Any Arrow compute function: name is lowercased and dispatched directly"},
  };
  return functions;
}

// The value of an Arrow result; throws on error (ValueOrDie would abort the process).
template <typename T> static T orThrow(arrow::Result<T>&& result) {
  if(!result.ok())
    throw std::runtime_error(result.status().ToStringWithoutContextLines());
  return std::move(result).ValueUnsafe();
}

// The column of table called columnName; throws "<operator>: no column named …" if there is none.
static std::shared_ptr<arrow::ChunkedArray> columnNamed(arrow::Table const& table,
                                                        std::string const& columnName,
                                                        std::string const& operatorName) {
  auto column = table.GetColumnByName(columnName);
  if(!column)
    throw std::runtime_error(operatorName + ": no column named '" + columnName + "'");
  return column;
}

// Typed access to a user-supplied operator argument. Throws a readable error
// (caught at the extern "C" boundary) instead of std::bad_variant_access.
template <typename T>
static T argumentAs(boss::ExpressionArguments const& arguments, size_t index,
                    std::string const& operatorName, std::string const& expected) {
  if(index >= arguments.size() || !std::holds_alternative<T>(arguments[index]))
    throw std::runtime_error(operatorName + " expects " + expected + " as argument " +
                             std::to_string(index + 1));
  return std::get<T>(arguments[index]);
}

static bool isNumber(boss::Expression const& argument) {
  return std::holds_alternative<int8_t>(argument) || std::holds_alternative<int32_t>(argument) ||
         std::holds_alternative<int64_t>(argument) || std::holds_alternative<double_t>(argument);
}

static double_t numberArgument(boss::ExpressionArguments const& arguments, size_t index,
                               std::string const& operatorName, std::string const& expected) {
  if(index >= arguments.size() || !isNumber(arguments[index]))
    throw std::runtime_error(operatorName + " expects " + expected + " as argument " +
                             std::to_string(index + 1));
  return std::visit(
      [](auto const& value) -> double_t {
        if constexpr(std::is_arithmetic_v<std::decay_t<decltype(value)>>)
          return static_cast<double_t>(value);
        else
          return 0;
      },
      arguments[index]);
}

// The column an aggregate like (Sum col) reduces.
static std::string aggregateColumn(Symbol const& head, boss::ExpressionArguments const& arguments) {
  if(arguments.size() != (head.getName() == "CountAll" ? 0u : 1u))
    throw std::runtime_error(head.getName() + " expects exactly " +
                             (head.getName() == "CountAll" ? "0 arguments" : "1 argument") +
                             ", got " + std::to_string(arguments.size()));
  return argumentAs<Symbol>(arguments, 0, head.getName(), "a column name").getName();
}

static compute::RoundMode roundMode(std::string const& name) {
  static std::unordered_map<std::string, compute::RoundMode> const modes = {
      {"Down", compute::RoundMode::DOWN},
      {"Up", compute::RoundMode::UP},
      {"TowardsZero", compute::RoundMode::TOWARDS_ZERO},
      {"TowardsInfinity", compute::RoundMode::TOWARDS_INFINITY},
      {"HalfDown", compute::RoundMode::HALF_DOWN},
      {"HalfUp", compute::RoundMode::HALF_UP},
      {"HalfTowardsZero", compute::RoundMode::HALF_TOWARDS_ZERO},
      {"HalfTowardsInfinity", compute::RoundMode::HALF_TOWARDS_INFINITY},
      {"HalfToEven", compute::RoundMode::HALF_TO_EVEN},
      {"HalfToOdd", compute::RoundMode::HALF_TO_ODD},
  };
  auto mode = modes.find(name);
  if(mode == modes.end())
    throw std::runtime_error("Round mode must be one of Down, Up, TowardsZero, TowardsInfinity, "
                             "HalfDown, HalfUp, HalfTowardsZero, HalfTowardsInfinity, "
                             "HalfToEven, HalfToOdd");
  return mode->second;
}

static compute::QuantileOptions::Interpolation interpolation(std::string const& name) {
  static std::unordered_map<std::string, compute::QuantileOptions::Interpolation> const
      interpolations = {
          {"Linear", compute::QuantileOptions::LINEAR},
          {"Lower", compute::QuantileOptions::LOWER},
          {"Higher", compute::QuantileOptions::HIGHER},
          {"Nearest", compute::QuantileOptions::NEAREST},
          {"Midpoint", compute::QuantileOptions::MIDPOINT},
  };
  auto mode = interpolations.find(name);
  if(mode == interpolations.end())
    throw std::runtime_error(
        "Quantile interpolation must be one of Linear, Lower, Higher, Nearest, Midpoint");
  return mode->second;
}

static bool isTrim(std::string const& name) {
  return name == "utf8_trim" || name == "utf8_ltrim" || name == "utf8_rtrim" ||
         name == "ascii_trim" || name == "ascii_ltrim" || name == "ascii_rtrim";
}

static bool takesOptions(std::string const& name) {
  return name == "round" || name == "strftime" || isTrim(name);
}

// Passes each option that follows a function's operand to set(key, value).
// Options are either positional, in the order of positionalKeys, or given as
// (Options (key value) …).
template <typename Set>
static void readOptions(std::string const& functionName, boss::ExpressionArguments&& arguments,
                        std::vector<std::string> const& positionalKeys, Set&& set) {
  auto position = 0u;
  for(auto& argument : arguments) {
    if(!std::holds_alternative<ComplexExpression>(argument)) {
      if(position == positionalKeys.size())
        throw std::runtime_error(functionName + " takes at most " +
                                 std::to_string(positionalKeys.size()) + " option arguments");
      set(positionalKeys[position++], std::move(argument));
    } else {
      boss::Expression read =
          std::move(argument)<"Options"_(AnySequence_)>[&](auto, auto settings, auto) {
            for(auto& setting : settings) {
              if(!std::holds_alternative<ComplexExpression>(setting))
                throw std::runtime_error("Options expects (key value) pairs");
              auto [key, statics, values, spans] =
                  std::get<ComplexExpression>(std::move(setting)).decompose();
              if(values.size() != 1)
                throw std::runtime_error("Options expects (key value) pairs");
              set(key.getName(), std::move(values[0]));
            }
            return true;
          };
      if(!std::holds_alternative<bool>(read))
        throw std::runtime_error(functionName +
                                 " expects option literals or (Options (key value) …)");
    }
  }
}

// The Arrow options of Round, Strftime and the trim functions, built from their
// trailing arguments.
static std::shared_ptr<compute::FunctionOptions>
functionOptions(std::string const& name, boss::ExpressionArguments&& arguments) {
  if(isTrim(name)) {
    auto options = std::make_shared<compute::TrimOptions>();
    readOptions(name, std::move(arguments), {"characters"},
                [&](std::string const& key, boss::Expression&& value) {
                  if(key != "characters")
                    throw std::runtime_error(name + " has no option '" + key + "'");
                  if(!std::holds_alternative<std::string>(value))
                    throw std::runtime_error(name + " characters must be a string");
                  options->characters = std::get<std::string>(std::move(value));
                });
    return options;
  }
  if(name == "round") {
    auto options = std::make_shared<compute::RoundOptions>();
    readOptions("Round", std::move(arguments), {"ndigits", "round_mode"},
                [&](std::string const& key, boss::Expression&& value) {
                  if(key == "ndigits" && std::holds_alternative<int32_t>(value))
                    options->ndigits = std::get<int32_t>(value);
                  else if(key == "round_mode" && std::holds_alternative<Symbol>(value))
                    options->round_mode = roundMode(std::get<Symbol>(value).getName());
                  else if(key == "ndigits" || key == "round_mode")
                    throw std::runtime_error("Round " + key + " must be " +
                                             (key == "ndigits" ? "an integer" : "a mode symbol"));
                  else
                    throw std::runtime_error("round has no option '" + key + "'");
                });
    return options;
  }
  auto options = std::make_shared<compute::StrftimeOptions>();
  readOptions("Strftime", std::move(arguments), {"format", "locale"},
              [&](std::string const& key, boss::Expression&& value) {
                if(!std::holds_alternative<std::string>(value))
                  throw std::runtime_error("Strftime " + key + " must be a string");
                if(key == "format")
                  options->format = std::get<std::string>(std::move(value));
                else if(key == "locale")
                  options->locale = std::get<std::string>(std::move(value));
                else
                  throw std::runtime_error("strftime has no option '" + key + "'");
              });
  return options;
}

// Formats timestamp with options.format. Arrow's strftime does not support "%-"
// (no zero padding, e.g. "%-d" gives "5" rather than "05"), so each "%-x" is
// formatted as "%x" with a leading zero removed, and the parts are joined.
static compute::Expression strftimeExpression(compute::Expression const& timestamp,
                                              compute::StrftimeOptions const& options) {
  auto format = [&](std::string const& pattern) {
    return compute::call("strftime", {timestamp},
                         compute::StrftimeOptions(pattern, options.locale));
  };
  auto parts = std::vector<compute::Expression>();
  auto const unpadded = std::regex("%-.");
  for(auto const& token :
      std::ranges::subrange(std::sregex_token_iterator(options.format.begin(), options.format.end(),
                                                       unpadded, {-1, 0}),
                            std::sregex_token_iterator())) {
    if(token.str().starts_with("%-"))
      parts.push_back(compute::call("replace_substring_regex",
                                    {format("%" + token.str().substr(2))},
                                    compute::ReplaceSubstringOptions("^0(\\d)", "\\1")));
    else if(token.length() > 0)
      parts.push_back(format(token.str()));
  }
  if(parts.size() == 1)
    return parts.front();
  // binary_join_element_wise takes the separator as its last argument
  parts.push_back(compute::literal(std::make_shared<arrow::StringScalar>("")));
  return compute::call("binary_join_element_wise", parts);
}

static compute::Expression toComputeExpression(boss::Expression&& e,
                                               std::set<std::string> const& columns = {}) {
  return std::visit(
      boss::utilities::overload(
          // symbol: column ref if the name exists in the schema, otherwise a symbol-value literal
          [&columns](Symbol&& s) {
            return columns.count(s.getName())
                       ? compute::field_ref(s.getName())
                       : compute::literal(std::make_shared<arrow::StringScalar>(s.getName()));
          },
          [](std::string&& s) {
            return compute::literal(std::make_shared<arrow::StringScalar>(std::move(s)));
          },
          [&columns](ComplexExpression&& ce) {
            auto [head, statics, arguments, spans] = std::move(ce).decompose();
            auto name = toArrowName(head.getName());
            // these functions take their trailing arguments as options, not operands
            if(name == "like" || name == "match_substring") {
              auto operand = toComputeExpression(std::move(arguments.at(0)), columns);
              return compute::call(name == "like" ? "match_like" : name, {std::move(operand)},
                                   compute::MatchSubstringOptions {
                                       std::get<std::string>(std::move(arguments.at(1)))});
            }
            if(takesOptions(name) && arguments.size() > 1) {
              auto operand = toComputeExpression(std::move(arguments[0]), columns);
              arguments.erase(arguments.begin());
              auto options = functionOptions(name, std::move(arguments));
              return name == "strftime"
                         ? strftimeExpression(
                               operand, static_cast<compute::StrftimeOptions const&>(*options))
                         : compute::call(name, {std::move(operand)}, std::move(options));
            }
            // Arrow casts only int32 (not int64) day counts to date32
            if(name == "date" && arguments.size() == 1 &&
               std::holds_alternative<int32_t>(arguments[0]))
              return compute::call("cast", {compute::literal(std::get<int32_t>(arguments[0]))},
                                   compute::CastOptions::Unsafe(arrow::date32()));
            auto operands = std::vector<compute::Expression>();
            for(auto& argument : arguments) {
              // auto-cast bare YYYY-MM-DD string literals to date32 in comparisons
              auto isDateLiteral =
                  std::set<std::string_view> {"less",          "less_equal", "greater",
                                              "greater_equal", "equal",      "not_equal"}
                      .count(name) &&
                  std::holds_alternative<std::string>(argument) &&
                  std::get<std::string>(argument).size() == 10 &&
                  std::get<std::string>(argument)[4] == '-' &&
                  std::get<std::string>(argument)[7] == '-';
              auto operand = toComputeExpression(std::move(argument), columns);
              operands.push_back(isDateLiteral
                                     ? compute::call("cast", {std::move(operand)},
                                                     compute::CastOptions::Unsafe(arrow::date32()))
                                     : std::move(operand));
            }
            if(name == "int")
              return compute::call("cast", operands, compute::CastOptions::Unsafe(arrow::int32()));
            else if(name == "int64")
              return compute::call("cast", operands, compute::CastOptions::Unsafe(arrow::int64()));
            // Divide is always floating-point; IntDivide keeps Arrow's type-preserving divide
            else if(name == "divide") {
              for(auto& operand : operands)
                operand = compute::call("cast", {std::move(operand)},
                                        compute::CastOptions::Safe(arrow::float64()));
              return compute::call("divide", operands);
            } else if(name == "intdivide")
              return compute::call("divide", operands);
            else if(name == "date")
              return compute::call("cast", operands, compute::CastOptions::Unsafe(arrow::date32()));
            else if(name == "timestamp")
              return compute::call("boss_to_timestamp", operands);
            else if(name == "modulo")
              return compute::call("boss_modulo", operands);
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
              for(auto const& operand : operands | std::views::drop(2))
                result = compute::call(name, {std::move(result), operand});
              return result;
            } else
              return compute::call(name, operands);
          },
          // integer literals are int64, matching the column type Table builds for them
          [](int32_t v) { return compute::literal(static_cast<int64_t>(v)); },
          // a table used as an operand, e.g. (ByName m), must have one row and one
          // column; it stands for that value
          [](int64_t v) {
            if(!intermediates.intermediates.count(v))
              return compute::literal(v);
            auto table = intermediates.getTable(intermediates.at(v));
            if(table->num_rows() != 1 || table->num_columns() != 1)
              throw std::runtime_error(
                  "a table used as a value inside an expression must have exactly one row and "
                  "one column, but it has " +
                  std::to_string(table->num_rows()) + " rows and " +
                  std::to_string(table->num_columns()) + " columns");
            return compute::literal(*table->column(0)->GetScalar(0));
          },
          [](auto v) { return compute::literal(v); }),
      std::move(e));
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

// Compute functions the engine adds to Arrow's function registry. They cannot
// be rewritten in toComputeExpression because what they do depends on the
// operand types, which are only known when Acero binds the expression.
namespace engineComputeFunctions {

// Promotes numeric operands to a common type: float64 if any is floating,
// otherwise int64. Non-numeric operands are left alone.
class NumericPromotingFunction : public compute::ScalarFunction {
public:
  using compute::ScalarFunction::ScalarFunction;
  arrow::Result<compute::Kernel const*>
  DispatchBest(std::vector<arrow::TypeHolder>* values) const override {
    auto anyFloating = std::any_of(values->begin(), values->end(), [](auto const& value) {
      return arrow::is_floating(value.id());
    });
    for(auto& value : *values)
      if(arrow::is_integer(value.id()) || arrow::is_floating(value.id()))
        value = anyFloating ? arrow::float64() : arrow::int64();
    return DispatchExact(*values);
  }
};

static arrow::Status setKernelResult(arrow::Datum const& result, compute::ExecSpan const& batch,
                                     compute::ExecResult* out) {
  if(result.is_scalar()) {
    ARROW_ASSIGN_OR_RAISE(auto array, arrow::MakeArrayFromScalar(*result.scalar(), batch.length));
    out->value = array->data();
  } else
    out->value = result.array();
  return arrow::Status::OK();
}

// Floored modulo: the result has the sign of the divisor, so (Modulo -7 5) is 3.
template <bool isFloating>
static arrow::Status moduloExec(compute::KernelContext*, compute::ExecSpan const& batch,
                                compute::ExecResult* out) {
  using compute::CallFunction;
  auto arguments = batch.ToExecBatch().values;
  auto const& x = arguments[0];
  auto const& n = arguments[1];
  if constexpr(isFloating) {
    ARROW_ASSIGN_OR_RAISE(auto quotient, CallFunction("divide", {x, n}));
    ARROW_ASSIGN_OR_RAISE(quotient, CallFunction("floor", {quotient}));
    ARROW_ASSIGN_OR_RAISE(auto product, CallFunction("multiply", {n, quotient}));
    ARROW_ASSIGN_OR_RAISE(auto remainder, CallFunction("subtract", {x, product}));
    return setKernelResult(remainder, batch, out);
  } else {
    // integer divide truncates; shift remainders whose sign differs from n's
    ARROW_ASSIGN_OR_RAISE(auto quotient, CallFunction("divide", {x, n}));
    ARROW_ASSIGN_OR_RAISE(auto product, CallFunction("multiply", {n, quotient}));
    ARROW_ASSIGN_OR_RAISE(auto remainder, CallFunction("subtract", {x, product}));
    auto zero = arrow::Datum(int64_t(0));
    ARROW_ASSIGN_OR_RAISE(auto nonZero, CallFunction("not_equal", {remainder, zero}));
    ARROW_ASSIGN_OR_RAISE(auto remainderNegative, CallFunction("less", {remainder, zero}));
    ARROW_ASSIGN_OR_RAISE(auto divisorNegative, CallFunction("less", {n, zero}));
    ARROW_ASSIGN_OR_RAISE(auto signsDiffer,
                          CallFunction("not_equal", {remainderNegative, divisorNegative}));
    ARROW_ASSIGN_OR_RAISE(auto needsShift, CallFunction("and", {nonZero, signsDiffer}));
    ARROW_ASSIGN_OR_RAISE(auto shifted, CallFunction("add", {remainder, n}));
    ARROW_ASSIGN_OR_RAISE(auto result, CallFunction("if_else", {needsShift, shifted, remainder}));
    return setKernelResult(result, batch, out);
  }
}

// Casts to a UTC timestamp; numbers are seconds since 1970 (fractions are dropped).
static arrow::Status toTimestampExec(compute::KernelContext*, compute::ExecSpan const& batch,
                                     compute::ExecResult* out) {
  auto input = batch.ToExecBatch().values[0];
  if(arrow::is_floating(input.type()->id())) {
    ARROW_ASSIGN_OR_RAISE(input,
                          compute::Cast(input, compute::CastOptions::Unsafe(arrow::int64())));
  }
  ARROW_ASSIGN_OR_RAISE(
      auto result, compute::Cast(input, compute::CastOptions::Unsafe(
                                            arrow::timestamp(arrow::TimeUnit::SECOND, "UTC"))));
  return setKernelResult(result, batch, out);
}

// The quantile of each list in a list column, as float64. Which quantile is
// set by the function's QuantileOptions.
struct QuantileState : compute::KernelState {
  compute::QuantileOptions options;
  explicit QuantileState(compute::QuantileOptions options) : options(std::move(options)) {}
};

static arrow::Status listQuantileExec(compute::KernelContext* context,
                                      compute::ExecSpan const& batch, compute::ExecResult* out) {
  auto input = batch.ToExecBatch().values[0];
  ARROW_ASSIGN_OR_RAISE(auto lists,
                        input.is_scalar()
                            ? arrow::MakeArrayFromScalar(*input.scalar(), batch.length)
                            : arrow::Result<std::shared_ptr<arrow::Array>>(input.make_array()));
  auto const& listArray = static_cast<arrow::ListArray const&>(*lists);
  auto builder = arrow::DoubleBuilder();
  for(auto row : std::views::iota(int64_t(0), listArray.length())) {
    if(listArray.IsNull(row)) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto quantiles,
          compute::CallFunction("quantile", {listArray.value_slice(row)},
                                &static_cast<QuantileState*>(context->state())->options));
      ARROW_ASSIGN_OR_RAISE(quantiles, compute::Cast(quantiles, arrow::float64()));
      auto values = std::static_pointer_cast<arrow::DoubleArray>(quantiles.make_array());
      ARROW_RETURN_NOT_OK(values->length() > 0 && values->IsValid(0)
                              ? builder.Append(values->Value(0))
                              : builder.AppendNull());
    }
  }
  ARROW_ASSIGN_OR_RAISE(auto result, builder.Finish());
  out->value = result->data();
  return arrow::Status::OK();
}

// Collects all values of a column into one list. Arrow only provides the
// grouped version (hash_list), so this is registered when "list" is missing.
struct ListState : compute::KernelState {
  std::shared_ptr<arrow::DataType> type;
  arrow::ArrayVector values;
};

static arrow::Status listConsume(compute::KernelContext* context, compute::ExecSpan const& batch) {
  auto input = batch.ToExecBatch().values[0];
  ARROW_ASSIGN_OR_RAISE(auto values,
                        input.is_scalar()
                            ? arrow::MakeArrayFromScalar(*input.scalar(), batch.length)
                            : arrow::Result<std::shared_ptr<arrow::Array>>(input.make_array()));
  static_cast<ListState*>(context->state())->values.push_back(std::move(values));
  return arrow::Status::OK();
}

static arrow::Status listMerge(compute::KernelContext*, compute::KernelState&& source,
                               compute::KernelState* destination) {
  auto& from = static_cast<ListState&>(source).values;
  auto& to = static_cast<ListState*>(destination)->values;
  to.insert(to.end(), std::make_move_iterator(from.begin()), std::make_move_iterator(from.end()));
  return arrow::Status::OK();
}

static arrow::Status listFinalize(compute::KernelContext* context, arrow::Datum* out) {
  auto* state = static_cast<ListState*>(context->state());
  ARROW_ASSIGN_OR_RAISE(auto values, state->values.empty() ? arrow::MakeEmptyArray(state->type)
                                                           : arrow::Concatenate(state->values));
  *out = arrow::Datum(std::make_shared<arrow::ListScalar>(std::move(values)));
  return arrow::Status::OK();
}

static bool registerAll() {
  auto addKernel = [](compute::ScalarFunction& function, std::vector<compute::InputType> inputs,
                      compute::OutputType output, compute::ArrayKernelExec exec,
                      compute::KernelInit init = nullptr) {
    auto kernel =
        compute::ScalarKernel(std::move(inputs), std::move(output), exec, std::move(init));
    kernel.null_handling = compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = compute::MemAllocation::NO_PREALLOCATE;
    if(auto status = function.AddKernel(std::move(kernel)); !status.ok())
      throw std::runtime_error(status.ToString());
  };
  auto modulo = std::make_shared<NumericPromotingFunction>(
      "boss_modulo", compute::Arity::Binary(),
      compute::FunctionDoc {"Floored modulo", "Result takes the sign of the divisor", {"x", "n"}});
  addKernel(*modulo, {arrow::int64(), arrow::int64()}, arrow::int64(), moduloExec<false>);
  addKernel(*modulo, {arrow::float64(), arrow::float64()}, arrow::float64(), moduloExec<true>);

  auto toTimestamp = std::make_shared<NumericPromotingFunction>(
      "boss_to_timestamp", compute::Arity::Unary(),
      compute::FunctionDoc {"Cast to timestamp[s, UTC]",
                            "Numbers are seconds since 1970; dates and strings are cast directly",
                            {"x"}});
  auto timestampType = arrow::timestamp(arrow::TimeUnit::SECOND, "UTC");
  for(auto input :
      {compute::InputType(arrow::int64()), compute::InputType(arrow::float64()),
       compute::InputType(arrow::Type::DATE32), compute::InputType(arrow::Type::DATE64),
       compute::InputType(arrow::Type::STRING), compute::InputType(arrow::Type::TIMESTAMP)})
    addKernel(*toTimestamp, {input}, timestampType, toTimestampExec);

  static auto const defaultQuantileOptions = compute::QuantileOptions();
  auto listQuantile = std::make_shared<compute::ScalarFunction>(
      "boss_list_quantile", compute::Arity::Unary(),
      compute::FunctionDoc {"Exact quantile of each list",
                            "Arrow's quantile kernel per list",
                            {"lists"},
                            "QuantileOptions"},
      &defaultQuantileOptions);
  addKernel(*listQuantile, {compute::InputType(arrow::Type::LIST)}, arrow::float64(),
            listQuantileExec,
            [](compute::KernelContext*, compute::KernelInitArgs const& arguments)
                -> arrow::Result<std::unique_ptr<compute::KernelState>> {
              return std::make_unique<QuantileState>(
                  static_cast<compute::QuantileOptions const&>(*arguments.options));
            });

  auto* registry = compute::GetFunctionRegistry();
  for(auto function :
      std::vector<std::shared_ptr<compute::Function>> {modulo, toTimestamp, listQuantile})
    if(auto status = registry->AddFunction(function); !status.ok())
      throw std::runtime_error(status.ToString());

  if(!registry->GetFunction("list").ok()) {
    auto list = std::make_shared<compute::ScalarAggregateFunction>(
        "list", compute::Arity::Unary(),
        compute::FunctionDoc {"Collect all values into one list", "", {"values"}});
    auto status = list->AddKernel(compute::ScalarAggregateKernel(
        {compute::InputType::Any()},
        compute::OutputType([](compute::KernelContext*, std::vector<arrow::TypeHolder> const& types)
                                -> arrow::Result<arrow::TypeHolder> {
          return arrow::TypeHolder(arrow::list(types[0].GetSharedPtr()));
        }),
        [](compute::KernelContext*, compute::KernelInitArgs const& arguments)
            -> arrow::Result<std::unique_ptr<compute::KernelState>> {
          auto state = std::make_unique<ListState>();
          state->type = arguments.inputs[0].GetSharedPtr();
          return state;
        },
        listConsume, listMerge, listFinalize, /*ordered=*/false));
    if(status.ok())
      status = registry->AddFunction(list);
    if(!status.ok())
      throw std::runtime_error(status.ToString());
  }
  return true;
}

} // namespace engineComputeFunctions

// Plans the aggregation of GroupBy and Resample. The arguments in
// [first, last) are aggregates, such as (Sum col), and key columns. The
// output has the key columns first, then one column per aggregate.
// Median and Quantile have no Acero kernel: they collect each group's values
// into a list, and the projection after the aggregation takes the quantile of
// each list. Inside the plan, aggregates are named by their position so that
// two aggregates with the same name stay apart.
static Declaration aggregation(Declaration input, boss::ExpressionArguments::iterator first,
                               boss::ExpressionArguments::iterator last) {
  auto aggregates = std::vector<compute::Aggregate>();
  auto keys = std::vector<FieldRef>();
  auto projections = std::vector<compute::Expression>();
  auto names = std::vector<std::string>();
  auto add = [&](std::string function, std::string columnName, std::string name,
                 std::shared_ptr<compute::QuantileOptions> quantile = nullptr) {
    auto position = std::to_string(aggregates.size());
    if(columnName.empty())
      aggregates.emplace_back(std::move(function), position);
    else
      aggregates.push_back({std::move(function), std::move(columnName), position});
    projections.push_back(
        quantile ? compute::call("boss_list_quantile", {compute::field_ref(position)}, quantile)
                 : compute::field_ref(position));
    names.push_back(std::move(name));
    return true;
  };
  auto quantile = [&](auto, auto arguments, auto) {
    auto columnName = std::get<Symbol>(arguments[0]).getName();
    auto options = std::make_shared<compute::QuantileOptions>(
        numberArgument(arguments, 1, "Quantile", "a fraction q"));
    if(arguments.size() == 3)
      options->interpolation = interpolation(std::get<Symbol>(arguments[2]).getName());
    return add("list", columnName, "quantile(" + columnName + ")", std::move(options));
  };
  for(auto& argument : std::ranges::subrange(first, last)) {
    if(std::holds_alternative<Symbol>(argument)) {
      auto key = std::get<Symbol>(argument).getName();
      projections.insert(projections.begin() + keys.size(), compute::field_ref(key));
      names.insert(names.begin() + keys.size(), key);
      keys.emplace_back(key);
    } else {
      boss::Expression added =
          std::move(argument)<"Median"_(Symbol_)>[&](auto, auto arguments, auto) {
            auto columnName = std::get<Symbol>(arguments[0]).getName();
            return add("list", columnName, "median(" + columnName + ")",
                       std::make_shared<compute::QuantileOptions>(0.5));
          } < "Quantile"_(Symbol_, Any_) >
          quantile<"Quantile"_(Symbol_, Any_, Symbol_)>
              quantile<"Quantile"_(AnySequence_)>[](auto, auto, auto) -> bool {
        throw std::runtime_error(
            "Quantile expects (Quantile col q [Linear|Lower|Higher|Nearest|Midpoint])");
      } < "CountAll"_() > [&](auto, auto, auto) {
        return add("count_all", "", "count_all()");
      } < Any_ > [&](Symbol head, auto, auto arguments, auto) {
        auto columnName = aggregateColumn(head, arguments);
        auto function = toArrowName(head.getName());
        return add(function, columnName, function + "(" + columnName + ")");
      };
      if(!std::holds_alternative<bool>(added))
        throw std::runtime_error(
            "GroupBy expects aggregates like (Sum col) followed by key columns");
    }
  }
  if(!keys.empty())
    for(auto& aggregate : aggregates)
      aggregate.function = "hash_" + aggregate.function;
  return Declaration::Sequence({std::move(input),
                                {"aggregate", AggregateNodeOptions(aggregates, keys)},
                                {"project", ProjectNodeOptions(projections, names)}});
}

// Range, Window, ExponentialMovingAverage and KernelDensityEstimate. Like
// Cumulate and Pairwise, they are computed directly on materialized columns,
// in row order, instead of being planned with Acero.

// One contiguous float64 array holding the named column.
static std::shared_ptr<arrow::DoubleArray> doubleColumn(arrow::Table const& table,
                                                        std::string const& columnName,
                                                        std::string const& operatorName) {
  auto values = orThrow(arrow::Concatenate(columnNamed(table, columnName, operatorName)->chunks()));
  if(values->type_id() != arrow::Type::DOUBLE)
    values = orThrow(compute::Cast(*values, arrow::float64()));
  return std::static_pointer_cast<arrow::DoubleArray>(values);
}

// The value of an expression that refers to no columns, such as 0.5,
// (Divide 1 42) or a one-row, one-column (ByName m), as a number.
static double_t constantNumber(boss::Expression&& expression, std::string const& operatorName) {
  auto bound = orThrow(toComputeExpression(std::move(expression)).Bind(arrow::Schema({})));
  auto result = orThrow(compute::ExecuteScalarExpression(bound, compute::ExecBatch({}, 1)));
  auto value = result.is_scalar() ? result.scalar() : orThrow(result.make_array()->GetScalar(0));
  if(!value->is_valid ||
     !(arrow::is_integer(value->type->id()) || arrow::is_floating(value->type->id())))
    throw std::runtime_error(operatorName + " expects a number");
  return std::static_pointer_cast<arrow::DoubleScalar>(orThrow(value->CastTo(arrow::float64())))
      ->value;
}

static std::shared_ptr<arrow::Table> appendColumn(std::shared_ptr<arrow::Table> const& table,
                                                  std::string const& name,
                                                  std::shared_ptr<arrow::Array> const& array) {
  return orThrow(table->AddColumn(table->num_columns(), arrow::field(name, array->type()),
                                  std::make_shared<arrow::ChunkedArray>(array)));
}

// Evenly spaced values from start to stop (inclusive when hit exactly).
static std::shared_ptr<arrow::Array> range(double_t start, double_t stop, double_t step,
                                           bool integral) {
  if(step <= 0)
    throw std::runtime_error("Range step must be positive");
  auto count =
      stop < start ? 0 : static_cast<int64_t>(std::floor((stop - start) / step + 1e-9)) + 1;
  if(integral) {
    auto builder = arrow::Int64Builder();
    for(auto i : std::views::iota(int64_t(0), count))
      (void)builder.Append(static_cast<int64_t>(start) + i * static_cast<int64_t>(step));
    return *builder.Finish();
  }
  auto builder = arrow::DoubleBuilder();
  for(auto i : std::views::iota(int64_t(0), count))
    (void)builder.Append(start + i * step);
  return *builder.Finish();
}

// The aggregate of each row and the size - 1 rows before it, ignoring nulls.
// The first size - 1 rows are null, or, if partial is set, the aggregate of
// the rows available. Sum, Mean and Count keep running totals; Min and Max
// keep the candidate rows in a deque ordered by value.
static std::shared_ptr<arrow::Array>
window(arrow::DoubleArray const& values, std::string const& function, int64_t size, bool partial) {
  auto sum = 0.0;
  auto count = int64_t(0);
  auto deque = std::deque<int64_t>(); // rows that can still be the window's Min or Max
  auto better = [&](double_t a, double_t b) { return function == "min" ? a <= b : a >= b; };
  auto counts = arrow::Int64Builder();
  auto results = arrow::DoubleBuilder();
  for(auto i : std::views::iota(int64_t(0), values.length())) {
    if(values.IsValid(i)) {
      sum += values.Value(i);
      ++count;
      while(!deque.empty() && better(values.Value(i), values.Value(deque.back())))
        deque.pop_back();
      deque.push_back(i);
    }
    if(i >= size && values.IsValid(i - size)) {
      sum -= values.Value(i - size);
      --count;
    }
    while(!deque.empty() && deque.front() <= i - size)
      deque.pop_front();
    if(i + 1 < size && !partial) {
      (void)counts.AppendNull();
      (void)results.AppendNull();
    } else if(function == "count")
      (void)counts.Append(count);
    else if(count == 0)
      (void)results.AppendNull();
    else if(function == "sum")
      (void)results.Append(sum);
    else if(function == "mean")
      (void)results.Append(sum / count);
    else
      (void)results.Append(values.Value(deque.front()));
  }
  return function == "count" ? *counts.Finish() : *results.Finish();
}

// The exponential moving average of values: starting from 0, each row moves
// the average towards its value by the fraction alpha. Null rows keep the
// previous average.
static std::shared_ptr<arrow::Array> exponentialMovingAverage(arrow::DoubleArray const& values,
                                                              double_t alpha) {
  auto builder = arrow::DoubleBuilder();
  auto state = 0.0;
  for(auto value : values) {
    if(value)
      state += alpha * (*value - state);
    (void)builder.Append(state);
  }
  return *builder.Finish();
}

// (Window table aggregate… size [Partial]): appends one column per aggregate.
// If orderColumn is given, the rows are sorted by it first.
static boss::Expression windowOperator(boss::ExpressionArguments&& arguments, bool partial,
                                       std::string const& orderColumn = "") {
  auto sizeIndex = arguments.size() - (partial ? 2 : 1);
  auto size = static_cast<int64_t>(numberArgument(arguments, sizeIndex, "Window", "a size"));
  if(size < 1)
    throw std::runtime_error("Window size must be at least 1");
  auto input = intermediates.getTable(intermediates.at(arguments[0]));
  if(!orderColumn.empty())
    input = orThrow(compute::Take(input, orThrow(compute::SortIndices(
                                             *columnNamed(*input, orderColumn, "Window")))))
                .table();
  auto table = input;
  auto windowed = [&](std::string function) {
    return [&, function](auto, auto aggregateArguments, auto) {
      auto columnName = std::get<Symbol>(aggregateArguments[0]).getName();
      table = appendColumn(
          table, function + "(" + columnName + ")",
          window(*doubleColumn(*input, columnName, "Window"), function, size, partial));
      return true;
    };
  };
  for(auto& aggregate : arguments | std::views::drop(1) | std::views::take(sizeIndex - 1)) {
    boss::Expression appended =
        std::move(aggregate)<"Sum"_(Symbol_)> windowed("sum")<"Mean"_(Symbol_)> windowed(
            "mean")<"Min"_(Symbol_)>
            windowed("min")<"Max"_(Symbol_)> windowed("max")<"Count"_(Symbol_)> windowed("count");
    if(!std::holds_alternative<bool>(appended))
      throw std::runtime_error(
          "Window supports (Sum col), (Mean col), (Min col), (Max col) and (Count col)");
  }
  return intermediates.putTable(table);
}

// (Window table aggregate… size [Partial] (OrderBy col)).
static boss::Expression orderedWindowOperator(boss::ExpressionArguments&& arguments, bool partial) {
  auto [head, statics, keys, spans] =
      std::get<ComplexExpression>(std::move(arguments.back())).decompose();
  arguments.pop_back();
  return windowOperator(std::move(arguments), partial, std::get<Symbol>(keys[0]).getName());
}

// (ExponentialMovingAverage table [outCol] col (Alpha a)) or (… (Span n)):
// appends the moving average of col, named outCol or exponential_moving_average(col).
static boss::Expression exponentialMovingAverageOperator(boss::ExpressionArguments&& arguments) {
  auto [head, statics, parameters, spans] =
      std::get<ComplexExpression>(std::move(arguments.back())).decompose();
  auto parameter = constantNumber(std::move(parameters[0]), head.getName());
  auto alpha = head.getName() == "Span" ? 2 / (parameter + 1) : parameter;
  auto named = arguments.size() == 4;
  auto columnName = std::get<Symbol>(arguments[named ? 2 : 1]).getName();
  auto table = intermediates.getTable(intermediates.at(arguments[0]));
  return intermediates.putTable(
      appendColumn(table,
                   named ? std::get<Symbol>(arguments[1]).getName()
                         : "exponential_moving_average(" + columnName + ")",
                   exponentialMovingAverage(
                       *doubleColumn(*table, columnName, "ExponentialMovingAverage"), alpha)));
}

// (KernelDensityEstimate table col (Grid start stop step) (Bandwidth h)): the
// Gaussian kernel density of col at each grid point, as columns x and density.
// (Bandwidth Silverman) sets h to 1.06 times the standard deviation times
// count^(-1/5) (Silverman's rule of thumb). Null values are ignored.
static boss::Expression kernelDensityEstimateOperator(boss::ExpressionArguments&& arguments) {
  auto values = doubleColumn(*intermediates.getTable(intermediates.at(arguments[0])),
                             std::get<Symbol>(arguments[1]).getName(), "KernelDensityEstimate");
  auto [gridHead, gridStatics, grid, gridSpans] =
      std::get<ComplexExpression>(std::move(arguments[2])).decompose();
  auto [bandwidthHead, bandwidthStatics, bandwidth, bandwidthSpans] =
      std::get<ComplexExpression>(std::move(arguments[3])).decompose();
  auto count = values->length() - values->null_count();
  auto standardDeviationOptions = compute::VarianceOptions(1);
  auto width =
      std::holds_alternative<Symbol>(bandwidth[0]) &&
              std::get<Symbol>(bandwidth[0]).getName() == "Silverman"
          ? 1.06 *
                orThrow(compute::CallFunction("stddev", {values}, &standardDeviationOptions))
                    .scalar_as<arrow::DoubleScalar>()
                    .value *
                std::pow(count, -0.2)
          : constantNumber(std::move(bandwidth[0]), "Bandwidth");
  if(count == 0 || !(width > 0))
    throw std::runtime_error(
        "KernelDensityEstimate needs at least one value and a positive bandwidth");
  auto points = std::static_pointer_cast<arrow::DoubleArray>(
      range(constantNumber(std::move(grid[0]), "Grid"), constantNumber(std::move(grid[1]), "Grid"),
            constantNumber(std::move(grid[2]), "Grid"), false));
  auto densities = arrow::DoubleBuilder();
  for(auto point : *points) {
    auto sum = 0.0;
    for(auto value : *values)
      if(value)
        sum += std::exp(-0.5 * std::pow((*point - *value) / width, 2));
    (void)densities.Append(sum / (count * width * std::sqrt(2 * M_PI)));
  }
  return intermediates.putTable(
      arrow::Table::Make(arrow::schema({arrow::field("x", arrow::float64()),
                                        arrow::field("density", arrow::float64())}),
                         {points, *densities.Finish()}));
}

// (Resample table key aggregate… step): aggregates by key, then left-joins
// every key from the smallest to the largest, in steps of step, with the
// result. Aggregates of keys without rows are set to fill.
static boss::Expression resampleOperator(boss::ExpressionArguments&& arguments,
                                         boss::Expression&& fill) {
  auto step = numberArgument(arguments, arguments.size() - 1, "Resample", "a step");
  auto keyName = std::get<Symbol>(arguments[1]).getName();
  // the aggregation's columns: the key (position 0), then the aggregates
  auto aggregated = intermediates.getTable(
      aggregation(intermediates.at(arguments[0]), arguments.begin() + 1, arguments.end() - 1));
  auto extremes = orThrow(compute::CallFunction("min_max", {aggregated->column(0)}))
                      .scalar_as<arrow::StructScalar>();
  auto keyValue = [&](int index) {
    return extremes.value[index]->is_valid
               ? std::static_pointer_cast<arrow::DoubleScalar>(
                     orThrow(extremes.value[index]->CastTo(arrow::float64())))
                     ->value
               : 0.0;
  };
  auto keys =
      range(keyValue(0), extremes.value[0]->is_valid ? keyValue(1) : -1.0, step,
            arrow::is_integer(aggregated->field(0)->type()->id()) && step == std::floor(step));
  auto fillScalar = std::visit(
      [](auto&& value) -> std::shared_ptr<arrow::Scalar> {
        using Value = std::decay_t<decltype(value)>;
        if constexpr(std::is_same_v<Value, int32_t>)
          return arrow::MakeScalar(static_cast<int64_t>(value));
        else if constexpr(std::is_arithmetic_v<Value>)
          return arrow::MakeScalar(value);
        else
          throw std::runtime_error("Resample fill value must be a number");
      },
      std::move(fill));
  auto rightSide = std::vector<compute::Expression> {
      compute::call("cast", {compute::field_ref(0)}, compute::CastOptions::Safe(keys->type()))};
  auto aggregateOutputs = std::vector<FieldRef>();
  auto filled = std::vector<compute::Expression> {compute::field_ref(0)};
  for(auto i : std::views::iota(1, aggregated->num_columns())) {
    rightSide.push_back(compute::field_ref(i));
    aggregateOutputs.emplace_back(i);
    filled.push_back(compute::call(
        "coalesce", {compute::field_ref(i),
                     compute::literal(orThrow(fillScalar->CastTo(aggregated->field(i)->type())))}));
  }
  return intermediates.put(Declaration::Sequence(
      {{"hashjoin",
        {Declaration {"table_source",
                      TableSourceNodeOptions(arrow::Table::Make(
                          arrow::schema({arrow::field(keyName, keys->type())}), {keys}))},
         Declaration::Sequence(
             {{"table_source", TableSourceNodeOptions(aggregated)},
              {"project", ProjectNodeOptions(rightSide, aggregated->ColumnNames())}})},
        HashJoinNodeOptions(JoinType::LEFT_OUTER, {FieldRef(0)}, {FieldRef(0)}, {FieldRef(0)},
                            aggregateOutputs, compute::literal(true), "_l", "_r")},
       {"order_by", OrderByNodeOptions(Ordering({compute::SortKey(FieldRef(0))}))},
       {"project", ProjectNodeOptions(filled, aggregated->ColumnNames())}}));
}

static boss::Expression buildJoin(JoinType joinType,
                                  boss::expressions::ExpressionArguments& dynamics) {
  auto leftKeys = std::vector<FieldRef>(), rightKeys = std::vector<FieldRef>();
  auto cols = intermediates.columnNames(dynamics.at(0));
  auto rightCols = intermediates.columnNames(dynamics.at(1));
  cols.insert(rightCols.begin(), rightCols.end());
  // (Equal lCol rCol) is an equi-join key; any other predicate is a residual filter
  auto filter = compute::Expression();
  for(auto& condition : dynamics | std::views::drop(2)) {
    boss::Expression residual =
        std::move(condition)<"Equal"_(Symbol_, Symbol_)>[&](auto, auto arguments, auto) {
          leftKeys.emplace_back(std::get<Symbol>(arguments[0]).getName());
          rightKeys.emplace_back(std::get<Symbol>(arguments[1]).getName());
          return true;
        };
    if(!std::holds_alternative<bool>(residual)) {
      auto predicate = toComputeExpression(std::move(residual), cols);
      filter = filter.is_valid() ? compute::call("and", {std::move(filter), std::move(predicate)})
                                 : std::move(predicate);
    }
  }
  if(!filter.is_valid())
    filter = literal(true);
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
    auto leftFields =
        intermediates.getTable(intermediates.at(dynamics[0]))->schema()->field_names();
    auto rightFields =
        intermediates.getTable(intermediates.at(dynamics[1]))->schema()->field_names();
    auto leftOutput = std::vector<FieldRef>(leftFields.begin(), leftFields.end());
    auto rightOutput = std::vector<FieldRef>(rightFields.begin(), rightFields.end());
    return boss::Expression {intermediates.put(
        {"hashjoin",
         {addDummy(intermediates.at(dynamics[0]), leftFields),
          addDummy(intermediates.at(dynamics[1]), rightFields)},
         HashJoinNodeOptions(joinType, {FieldRef(dummyKey)}, {FieldRef(dummyKey)}, leftOutput,
                             rightOutput, filter, "_l", "_r", true)})};
  }
  return boss::Expression {intermediates.put(
      {"hashjoin",
       {intermediates.at(dynamics[0]), intermediates.at(dynamics[1])},
       HashJoinNodeOptions(joinType, leftKeys, rightKeys, filter, "_l", "_r", true)})};
}

static boss::Expression evaluate(boss::Expression&& e) {
  static auto _ = compute::Initialize();
  static auto registered = engineComputeFunctions::registerAll();
  return std::move(e) //
         <"Slice"_(Any_, Integer_, Integer_) >= Description("Fetch a contiguous slice of rows")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               return intermediates.put(Declaration::Sequence(
                   {intermediates.at(dynamics.at(0)),
                    {"fetch",
                     FetchNodeOptions(
                         static_cast<int64_t>(numberArgument(dynamics, 1, "Slice", "an offset")),
                         static_cast<int64_t>(numberArgument(dynamics, 2, "Slice", "a count")))}}));
             }} //
         <"OrderBy"_(Any_, Any_) >=
          Description("Sort rows by one or more columns; wrap a column in (Desc "
                      "col) to sort descending")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               auto orderKeys = std::vector<compute::SortKey>();
               auto sortKey = [&](compute::SortOrder order) {
                 return [&, order](auto, auto arguments, auto) {
                   orderKeys.emplace_back(std::get<Symbol>(arguments[0]).getName(), order);
                   return true;
                 };
               };
               if(!std::holds_alternative<ComplexExpression>(dynamics[1]))
                 throw std::runtime_error("OrderBy expects a key list like (keys col …)");
               auto [listHead, listStatics, keys, listSpans] =
                   std::get<ComplexExpression>(std::move(dynamics[1])).decompose();
               for(auto& key : keys) {
                 if(std::holds_alternative<Symbol>(key)) {
                   orderKeys.emplace_back(std::get<Symbol>(key).getName());
                 } else {
                   boss::Expression added = std::move(key)<"Desc"_(Symbol_)>
                       sortKey(compute::SortOrder::Descending)<"Asc"_(Symbol_)> sortKey(
                           compute::SortOrder::Ascending);
                   if(!std::holds_alternative<bool>(added))
                     throw std::runtime_error("OrderBy keys are columns, (Asc col) or (Desc col)");
                 }
               }
               return intermediates.put(
                   Declaration::Sequence({intermediates.at(dynamics.at(0)),
                                          {"order_by", OrderByNodeOptions(Ordering(orderKeys))}}));
             }} //
         <"Join"_(AnySequence_) >=
          Description("Hash join; (Equal lCol rCol) defines equi-join keys, other predicates "
                      "become residual filters; colliding names get _l/_r suffixes; no Equal "
                      "predicates degrades to O(n^2) cross-join")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::FirstTwo,
             [](auto, auto dynamics, auto) { return buildJoin(JoinType::INNER, dynamics); }} //
         <"LeftJoin"_(AnySequence_) >=
          Description("Left outer hash join; same predicate syntax as Join")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::FirstTwo,
             [](auto, auto dynamics, auto) { return buildJoin(JoinType::LEFT_OUTER, dynamics); }} //
         <"AntiJoin"_(AnySequence_) >= Description("Left anti join: rows in left with no match in "
                                                   "right; same predicate syntax as Join")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::FirstTwo,
             [](auto, auto dynamics, auto) { return buildJoin(JoinType::LEFT_ANTI, dynamics); }} //
         <"Union"_(AnySequence_) >=
          Description("Concatenate two or more tables (bag union, like SQL UNION ALL); "
                      "schemas are unified across inputs, numeric columns are widened to a "
                      "common type, and columns absent from a given input are filled with "
                      "nulls; rows appear in input order")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::All,
             [](auto, auto dynamics, auto) {
               auto tables = std::vector<std::shared_ptr<arrow::Table>>();
               for(auto& key : dynamics)
                 tables.push_back(intermediates.getTable(intermediates.at(key)));
               arrow::ConcatenateTablesOptions concatOptions;
               concatOptions.unify_schemas = true;
               concatOptions.field_merge_options = arrow::Field::MergeOptions::Permissive();
               auto maybeTable = arrow::ConcatenateTables(tables, concatOptions);
               if(!maybeTable.ok())
                 return boss::Expression {maybeTable.status().ToStringWithoutContextLines()};
               return boss::Expression {intermediates.putTable(*maybeTable)};
             }} //
         <"Name"_(AnySequence_) >=
          Description("Store a table under a named handle for later retrieval")>
             Recurse(evaluate) >
         OperatorOnIntermediates {TableArguments::First,
                                  [](auto, auto dynamics, auto) {
                                    return intermediates.name(std::move(dynamics.at(0)),
                                                              get<boss::Symbol>(dynamics.at(1)));
                                  }} //
         <"ByName"_(AnySequence_) >=
          Description("Retrieve a previously named table; inside an expression, a one-row, "
                      "one-column table stands for its value; an unknown name stays unevaluated")>
             Recurse(evaluate) >
         [](auto head, auto statics, auto dynamics, auto spans) {
           auto named = intermediates.names.find(get<boss::Symbol>(dynamics.at(0)));
           if(named == intermediates.names.end())
             return (boss::Expression)ComplexExpression(std::move(head), std::move(statics),
                                                        std::move(dynamics), std::move(spans));
           return (boss::Expression) static_cast<int64_t>(named->second);
         } //
         <"Filter"_(Any_, Any_) >=
          Description("Keep only rows where pred holds; supports And, Or, Not and all Arrow "
                      "comparison/compute functions")>
             Recurse(evaluate) >
         OperatorOnIntermediates {TableArguments::First,
                                  [](auto, auto dynamics, auto) {
                                    auto columns = intermediates.columnNames(dynamics.at(0));
                                    return intermediates.put(Declaration::Sequence(
                                        {intermediates.at(dynamics.at(0)),
                                         {"filter", FilterNodeOptions(toComputeExpression(
                                                        std::move(dynamics.at(1)), columns))}}));
                                  }} //
         <"Project"_(AnySequence_) >= Description("Select and rename columns; supports (As expr "
                                                  "newName) aliasing, (As (Int col) newName)/(As "
                                                  "(Bool col) newName) for type casts, and "
                                                  "arbitrary Arrow compute functions in the "
                                                  "expression")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               auto columns = intermediates.columnNames(dynamics.at(0));
               auto projections = std::vector<compute::Expression>();
               auto names = std::vector<std::string>();
               for(auto& projection : dynamics | std::views::drop(1)) {
                 if(std::holds_alternative<Symbol>(projection)) {
                   projections.push_back(
                       compute::field_ref(std::get<Symbol>(projection).getName()));
                   names.push_back(std::get<Symbol>(projection).getName());
                 } else {
                   boss::Expression unaliased = std::move(
                       projection)<"As"_(Any_, Symbol_)>[&](auto, auto arguments, auto) {
                     projections.push_back(toComputeExpression(std::move(arguments[0]), columns));
                     names.push_back(std::get<Symbol>(arguments[1]).getName());
                     return true;
                   };
                   // any other expression is a computed column named after itself
                   if(std::holds_alternative<ComplexExpression>(unaliased)) {
                     projections.push_back(toComputeExpression(std::move(unaliased), columns));
                     names.push_back(projections.back().ToString());
                   }
                 }
               }
               return intermediates.put(
                   Declaration::Sequence({intermediates.at(dynamics.at(0)),
                                          {"project", ProjectNodeOptions(projections, names)}}));
             }} //
         <"GroupBy"_(Any_, AnySequence_) >=
          Description("Aggregate a column (Sum, Mean, Max, CountAll, Median, Quantile, ...). "
                      "Without keys: global aggregate; with keys: hash-aggregate")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               auto keyed =
                   std::any_of(dynamics.begin() + 1, dynamics.end(), [](auto const& argument) {
                     return std::holds_alternative<Symbol>(argument);
                   });
               auto plan =
                   aggregation(intermediates.at(dynamics[0]), dynamics.begin() + 1, dynamics.end());
               // hash aggregates are materialized right away; global ones stay lazy
               return keyed ? intermediates.putTable(intermediates.getTable(std::move(plan)))
                            : intermediates.put(std::move(plan));
             }} //
         <"Cumulate"_(Any_, Any_) >= Description("Running (prefix) aggregate, e.g. cumulative sum")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               if(!std::holds_alternative<ComplexExpression>(dynamics[1]))
                 throw std::runtime_error("Cumulate expects an aggregate like (Sum col)");
               auto [head, statics, arguments, spans] =
                   std::get<ComplexExpression>(std::move(dynamics[1])).decompose();
               auto columnName = aggregateColumn(head, arguments);
               auto function = toArrowName(head.getName());
               auto input = intermediates.getTable(intermediates.at(dynamics[0]));
               auto result =
                   orThrow(compute::CallFunction("cumulative_" + function,
                                                 {columnNamed(*input, columnName, "Cumulate")}))
                       .chunked_array();
               return intermediates.putTable(orThrow(input->AddColumn(
                   input->num_columns(), field(function + "(" + columnName + ")", result->type()),
                   result)));
             }} //
         <"Pairwise"_(Any_, Symbol_, Symbol_, Integer_) >=
          Description("Difference between each row and the row lag rows before it; the first lag "
                      "rows are null")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               auto input = intermediates.getTable(intermediates.at(dynamics[0]));
               auto values = orThrow(arrow::Concatenate(
                   columnNamed(*input, std::get<Symbol>(dynamics[2]).getName(), "Pairwise")
                       ->chunks()));
               auto options = compute::PairwiseOptions(
                   static_cast<int64_t>(numberArgument(dynamics, 3, "Pairwise", "a lag")));
               return intermediates.putTable(
                   appendColumn(input, std::get<Symbol>(dynamics[1]).getName(),
                                orThrow(compute::CallFunction("pairwise_diff", {values}, &options))
                                    .make_array()));
             }} //
         <"Range"_(Symbol_, Any_, Any_, Any_) >=
          Description("Construct a one-column table of evenly spaced values from start to stop "
                      "(inclusive)")>
             Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto values = range(numberArgument(dynamics, 1, "Range", "a start number"),
                               numberArgument(dynamics, 2, "Range", "a stop number"),
                               numberArgument(dynamics, 3, "Range", "a step number"),
                               !std::holds_alternative<double_t>(dynamics[1]) &&
                                   !std::holds_alternative<double_t>(dynamics[2]) &&
                                   !std::holds_alternative<double_t>(dynamics[3]));
           return intermediates.putTable(arrow::Table::Make(
               arrow::schema(
                   {arrow::field(std::get<Symbol>(dynamics[0]).getName(), values->type())}),
               {values}));
         } < "Window"_(Any_, AnySequence_, Integer_, Symbol("Partial"), "OrderBy"_(Symbol_)) >=
         Description("Aggregate a column (Sum, Mean, Min, Max, Count) over each row and the rows "
                     "before it, size rows in total, in input order or sorted by (OrderBy col); "
                     "the first rows are null unless Partial is given") > Recurse(evaluate) >
         OperatorOnIntermediates {TableArguments::First,
                                  [](auto, auto dynamics, auto) {
                                    return orderedWindowOperator(std::move(dynamics), true);
                                  }} //
         <"Window"_(Any_, AnySequence_, Integer_, "OrderBy"_(Symbol_)) >= Recurse(evaluate)>
             OperatorOnIntermediates {TableArguments::First,
                                      [](auto, auto dynamics, auto) {
                                        return orderedWindowOperator(std::move(dynamics), false);
                                      }} //
         <"Window"_(Any_, AnySequence_, Integer_, Symbol("Partial")) >= Recurse(evaluate)>
             OperatorOnIntermediates {TableArguments::First,
                                      [](auto, auto dynamics, auto) {
                                        return windowOperator(std::move(dynamics), true);
                                      }} //
         <"Window"_(Any_, AnySequence_, Integer_) >= Recurse(evaluate)>
             OperatorOnIntermediates {TableArguments::First,
                                      [](auto, auto dynamics, auto) {
                                        return windowOperator(std::move(dynamics), false);
                                      }} <
         "ExponentialMovingAverage"_(Any_, Symbol_, "Alpha"_(Any_)) >=
         Description("Exponentially weighted moving average of a column, optionally named outCol; "
                     "(Alpha a) gives each new row weight a, (Span n) sets a to 2/(n+1); a and n "
                     "may be constant expressions") > Recurse(evaluate) >
         OperatorOnIntermediates {TableArguments::First,
                                  [](auto, auto dynamics, auto) {
                                    return exponentialMovingAverageOperator(std::move(dynamics));
                                  }} < "ExponentialMovingAverage"_(Any_, Symbol_, "Span"_(Any_)) >=
         Recurse(evaluate) >
         OperatorOnIntermediates {TableArguments::First,
                                  [](auto, auto dynamics, auto) {
                                    return exponentialMovingAverageOperator(std::move(dynamics));
                                  }} //
         <"ExponentialMovingAverage"_(Any_, Symbol_, Symbol_, "Alpha"_(Any_)) >= Recurse(evaluate)>
             OperatorOnIntermediates {
                 TableArguments::First,
                 [](auto, auto dynamics, auto) {
                   return exponentialMovingAverageOperator(std::move(dynamics));
                 }} < "ExponentialMovingAverage"_(Any_, Symbol_, Symbol_, "Span"_(Any_)) >=
         Recurse(evaluate) >
         OperatorOnIntermediates {TableArguments::First,
                                  [](auto, auto dynamics, auto) {
                                    return exponentialMovingAverageOperator(std::move(dynamics));
                                  }} <
         "KernelDensityEstimate"_(Any_, Symbol_, "Grid"_(Any_, Any_, Any_), "Bandwidth"_(Any_)) >=
         Description("Gaussian kernel density of a column at each point of a grid, as columns x "
                     "and density; (Bandwidth Silverman) picks the bandwidth from the "
                     "data") > Recurse(evaluate) >
         OperatorOnIntermediates {TableArguments::First,
                                  [](auto, auto dynamics, auto) {
                                    return kernelDensityEstimateOperator(std::move(dynamics));
                                  }} <
         "Resample"_(Any_, Symbol_, AnySequence_, Any_, "Fill"_(Any_)) >= //
         Description("Aggregate grouped by a numeric key column, with one row per step from the "
                     "smallest to the largest key; the output is sorted; keys without input rows "
                     "get the fill value (default 0)") > Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               auto [head, statics, fill, spans] =
                   std::get<ComplexExpression>(std::move(dynamics.back())).decompose();
               dynamics.pop_back();
               return resampleOperator(std::move(dynamics), std::move(fill[0]));
             }} //
         <"Resample"_(Any_, Symbol_, AnySequence_, Any_) >= Recurse(evaluate)>
             OperatorOnIntermediates {TableArguments::First,
                                      [](auto, auto dynamics, auto) {
                                        return resampleOperator(std::move(dynamics),
                                                                boss::Expression(int32_t(0)));
                                      }} //
         <"ToStatus"_(AnySequence_) >= Description("Evaluate a pipeline and return OK rather than "
                                                   "materialising the result")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               return DeclarationToStatus(intermediates.at(dynamics.at(0)), false).CodeAsString();
             }} //
         <"Materialize"_(AnySequence_) >= Description("Force materialisation of chunked Arrow "
                                                      "arrays into a single contiguous buffer")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               auto maybeTable =
                   intermediates.getTable(intermediates.at(dynamics.at(0)))->CombineChunks();
               if(!maybeTable.ok())
                 return boss::Expression {maybeTable.status().ToStringWithoutContextLines()};
               return boss::Expression {intermediates.putTable(*maybeTable)};
             }} //
         <"Schema"_(AnySequence_) >= Description("Return a one-column table listing the column "
                                                 "names of table as symbols")>
             Recurse(evaluate) >
         OperatorOnIntermediates {
             TableArguments::First,
             [](auto, auto dynamics, auto) {
               auto input = intermediates.getTable(intermediates.at(dynamics.at(0)));
               auto builder = arrow::StringBuilder {};
               for(auto const& field : input->schema()->fields())
                 (void)builder.Append(field->name());
               return intermediates.putTable(arrow::Table::Make(
                   arrow::schema(
                       {arrow::field("Columns", arrow::utf8(), true,
                                     arrow::key_value_metadata({"boss_type"}, {"symbol"}))}),
                   {*builder.Finish()}));
             }} //
         <"Table"_(AnySequence_) >= Description("Construct an in-memory table from literal column "
                                                "data; symbol values are stored as named nulls")>
             Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           auto fields = std::vector<std::shared_ptr<arrow::Field>>();
           auto arrays = std::vector<std::shared_ptr<arrow::Array>>();
           for(auto& columnExpression : dynamics) {
             auto [columnHead, statics, dynamicArguments, spanArguments] =
                 get<ComplexExpression>(std::move(columnExpression)).decompose();
             auto columnName = columnHead.getName();
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
               // a column mixing integer and floating literals is promoted to float64
               auto anyFloating =
                   std::any_of(spanArguments.begin(), spanArguments.end(), [](auto const& span) {
                     return std::visit(
                         [](auto const& s) {
                           return std::is_floating_point_v<std::remove_const_t<
                               typename std::decay_t<decltype(s)>::element_type>>;
                         },
                         span);
                   });
               auto buildFromSpans = [&]<typename Scalar>() {
                 withBuilder<Scalar>([&](auto&& builder, auto type) {
                   for(auto& spanArg : spanArguments)
                     std::visit(
                         [&](auto& source) {
                           using SourceScalar = std::remove_const_t<
                               typename std::decay_t<decltype(source)>::element_type>;
                           if constexpr(std::is_same_v<SourceScalar, Symbol>)
                             (void)builder.AppendNulls(source.size());
                           else if constexpr(std::is_same_v<SourceScalar, Scalar>)
                             for(auto const& value : source)
                               (void)builder.Append(value);
                           else if constexpr(std::is_arithmetic_v<SourceScalar> &&
                                             std::is_arithmetic_v<Scalar>)
                             for(auto const& value : source)
                               (void)builder.Append(static_cast<Scalar>(value));
                         },
                         spanArg);
                   arrays.push_back(*builder.Finish());
                   fields.push_back(arrow::field(columnName, type));
                 });
               };
               if(firstTyped != spanArguments.end() && anyFloating)
                 buildFromSpans.template operator()<double_t>();
               else if(firstTyped != spanArguments.end())
                 std::visit(
                     [&](auto& typedSource) {
                       buildFromSpans.template operator()<std::remove_const_t<
                           typename std::decay_t<decltype(typedSource)>::element_type>>();
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
               auto firstNonNull =
                   std::find_if(dynamicArguments.begin(), dynamicArguments.end(),
                                [](auto const& v) { return !std::holds_alternative<Symbol>(v); });
               // a column mixing integer and floating literals is promoted to float64
               auto anyFloating =
                   std::any_of(dynamicArguments.begin(), dynamicArguments.end(),
                               [](auto const& v) { return std::holds_alternative<double_t>(v); });
               auto buildFromArguments = [&]<typename T>() {
                 withBuilder<T>([&](auto&& builder, auto type) {
                   for(auto& argument : dynamicArguments)
                     visit(boss::utilities::overload(
                               [&](Symbol const&) { (void)builder.AppendNull(); },
                               [&]<typename Source>(Source const& value) requires(
                                   std::is_arithmetic_v<Source>&& std::is_arithmetic_v<T>) {
                                 (void)builder.Append(static_cast<T>(value));
                               },
                               [&](T const& value) requires(!std::is_arithmetic_v<T>) {
                                 (void)builder.Append(value);
                               },
                               [](auto const&) {
                                 throw std::runtime_error("Table column mixes incompatible types");
                               }),
                           argument);
                   arrays.push_back(*builder.Finish());
                   fields.push_back(arrow::field(columnName, type));
                 });
               };
               if(firstNonNull != dynamicArguments.end() && anyFloating)
                 buildFromArguments.template operator()<double_t>();
               else if(firstNonNull != dynamicArguments.end())
                 visit(boss::utilities::overload(
                           [&]<typename T>(T const&)
                               requires(std::is_arithmetic_v<T> || std::is_same_v<T, std::string>) {
                                 buildFromArguments.template operator()<T>();
                               },
                           [](auto const&) {}),
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
         } < "Load"_(AnySequence_) >= Description("Read a CSV file, or every .csv/.tbl in a "
                                                  "directory unioned with a 'file name' "
                                                  "column") > Recurse(evaluate) >
         [](auto, auto dynamics, auto) {
           return std::visit(
               boss::utilities::overload(
                   [&dynamics](std::string&& path) -> boss::Expression {
                     std::vector<std::string> columnNames;
                     for(auto const& column : dynamics | std::views::drop(1))
                       columnNames.push_back(get<Symbol>(column).getName());

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

                     if(!isDirectory) {
                       auto extension = fs::path(path).extension().string();
                       if(extension != ".csv" && extension != ".tbl") {
                         boss::ExpressionArguments unchanged;
                         unchanged.push_back(std::move(path));
                         for(auto& argument : dynamics | std::views::drop(1))
                           unchanged.push_back(std::move(argument));
                         return ComplexExpression("Load"_, {}, std::move(unchanged));
                       }
                     }

                     auto build = [&]() -> arrow::Result<std::shared_ptr<arrow::Table>> {
                       if(!isDirectory) {
                         ARROW_ASSIGN_OR_RAISE(auto table, loadOne(path));
                         return table->CombineChunks();
                       }

                       std::vector<std::string> filePaths;
                       for(auto const& entry : fs::directory_iterator(path)) {
                         auto extension = entry.path().extension().string();
                         if(entry.is_regular_file() && (extension == ".csv" || extension == ".tbl"))
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
         } < "GetEngineDescription"_(AnySequence_) >= Description("Return this operator "
                                                                  "description string") >
         Recurse(evaluate) > [](auto, auto, auto) {
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
  auto result = new BOSSExpression {.delegate = [&]() -> boss::Expression {
    try {
      return intermediates.convertResult(evaluate(std::move(e->delegate)));
    } catch(std::exception const& exception) {
      return std::string(exception.what());
    }
  }()};
  auto live = std::set<size_t>();
  for(auto& [name, key] : intermediates.names)
    live.insert(key);
  std::erase_if(intermediates.intermediates, [&](auto& kv) { return !live.count(kv.first); });
  return result;
};
