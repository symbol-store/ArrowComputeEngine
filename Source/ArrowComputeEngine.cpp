#include <BOSS.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <algorithm>
#include <arrow/api.h>

#include <arrow/acero/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <memory>
#include <random>
#include <set>
#include <unordered_map>
using namespace boss::utilities::experimental;
using namespace arrow;
using namespace acero;
using arrow::Table;
using arrow::acero::Declaration;
using boss::Symbol;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Expression;

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
        columnValues.push_back(boss::Expression((char const*)value.data()));
      else
        columnValues.push_back("ArrowType"_(value.ToString()));
    } else
      columnValues.push_back("ArrowType"_(value.ToString()));
    return arrow::Status::OK();
  }
  boss::ExpressionArguments getColumnValues() && { return std::move(columnValues); };
};

static class {
  std::unordered_map<size_t, Declaration> intermediates;
  std::unordered_map<boss::Symbol, size_t> names;

  int64_t generateID() {
    static std::default_random_engine generator(std::random_device{}());
    return std::uniform_int_distribution<int64_t>(LONG_LONG_MIN, LONG_LONG_MAX)(generator);
  };

public:
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
  Declaration const& at(boss::Expression const& key) { return intermediates.at(std::get<int64_t>(key)); }

  boss::Expression convertResult(boss::Expression const& key) {
    if(!std::holds_alternative<int64_t>(key))
      return key.clone(boss::expressions::CloneReason::EVALUATE_CONST_EXPRESSION);
    auto table = *arrow::acero::DeclarationToTable(at(key), false);
    boss::ExpressionArguments resultExpression;
    for(auto i = 0u; i < table->num_columns(); i++) {
      auto resultColumn = table->column(i);
      auto visitor = ColumnConverter();
      for(auto i = 0u; i < resultColumn->length(); i++) {
        auto _ = arrow::VisitScalarInline(*resultColumn->GetScalar(i).ValueUnsafe(), &visitor);
      }
      resultExpression.push_back(ComplexExpression(boss::Symbol(table->field(i)->name()),
                                                   std::move(visitor).getColumnValues()));
    }
    return ComplexExpression("Table"_, {}, std::move(resultExpression));
  };
  std::shared_ptr<arrow::Table> getTable(Declaration d) {
    return d.factory_name == "table_source"
               ? dynamic_cast<arrow::acero::TableSourceNodeOptions*>(d.options.get())->table
               : (*DeclarationToTable(d, false));
  };

  void collectGarbage() {
    auto live = std::set<size_t>();
    for(auto& [name, key] : names)
      live.insert(key);
    std::erase_if(intermediates, [&](auto& kv) { return !live.count(kv.first); });
  }
} intermediates;

static compute::Expression toComputeExpression(boss::Expression const& e) {
  return std::visit(
      boss::utilities::overload([](Symbol const& s) { return compute::field_ref(s.getName()); },
                                [](bool v) { return compute::literal(v); },
                                [](int64_t v) { return compute::literal(v); },
                                [](int32_t v) { return compute::literal(v); },
                                [](double_t v) { return compute::literal(v); },
                                [](float_t v) { return compute::literal(v); },
                                [](std::string const& s) {
                                  return compute::literal(std::make_shared<arrow::StringScalar>(s));
                                },
                                [](ComplexExpression const& ce) {
                                  auto args = std::vector<compute::Expression>();
                                  for(auto const& arg : ce.getDynamicArguments())
                                    args.push_back(toComputeExpression(arg));
                                  auto name = ce.getHead().getName();
                                  std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                                  return compute::call(name, args);
                                },
                                [](auto const&) -> compute::Expression {
                                  throw std::runtime_error("unsupported compute expression");
                                }),
      e);
}

template <typename T, typename F> static void withBuilder(F&& use) {
  if constexpr(std::is_same_v<T, int64_t>)
    use(arrow::Int64Builder {}, arrow::int64());
  else if constexpr(std::is_same_v<T, int32_t>)
    use(arrow::Int32Builder {}, arrow::int32());
  else if constexpr(std::is_same_v<T, double_t>)
    use(arrow::DoubleBuilder {}, arrow::float64());
  else if constexpr(std::is_same_v<T, float_t>)
    use(arrow::FloatBuilder {}, arrow::float32());
  else if constexpr(std::is_same_v<T, std::string>)
    use(arrow::StringBuilder {}, arrow::utf8());
  else
    throw std::runtime_error("unsupported column type: " + std::string(typeid(T).name()));
}

static boss::Expression evaluate(boss::Expression&& e) {
  using boss::utilities::experimental::sentinel::Any_;
  using boss::utilities::experimental::sentinel::AnySequence_;
  static auto _ = compute::Initialize();
  return std::move(e) //
         <"Slice"_(AnySequence_) >= Recurse(evaluate)>[](auto, auto dynamics, auto) {
           return intermediates.put(Declaration::Sequence(
               {intermediates.at(dynamics.at(0)),
                {"fetch", FetchNodeOptions(get<int>(dynamics.at(1)), get<int>(dynamics.at(2)))}}));
         } < "OrderBy"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           auto orderKeys = std::vector<compute::SortKey>();
           for(auto& it : get<ComplexExpression>(dynamics.at(1)).getDynamicArguments())
             orderKeys.push_back(compute::SortKey(get<Symbol>(it).getName()));
           return intermediates.put(
               Declaration::Sequence({intermediates.at(dynamics.at(0)),
                                      {"order_by", OrderByNodeOptions(Ordering(orderKeys))}}));
         } < "Join"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           auto leftKeys = std::vector<FieldRef>(), rightKeys = std::vector<FieldRef>();
           for(auto& it : get<ComplexExpression>(dynamics.at(1)).getDynamicArguments())
             leftKeys.push_back(get<Symbol>(it).getName());
           for(auto& it : get<ComplexExpression>(dynamics.at(3)).getDynamicArguments())
             rightKeys.push_back(get<Symbol>(it).getName());
           return intermediates.put(
               {"hashjoin",
                {intermediates.at(dynamics.at(0)), intermediates.at(dynamics.at(2))},
                HashJoinNodeOptions(JoinType::INNER, leftKeys, rightKeys, literal(true), "_l", "_r",
                                    true)});
         } < "Name"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           return intermediates.name(std::move(dynamics.at(0)), get<boss::Symbol>(dynamics.at(1)));
         } < "ByName"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           return intermediates.byName(get<boss::Symbol>(dynamics.at(0)));
         } < "Filter"_(Any_, Any_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           return intermediates.put(Declaration::Sequence(
               {intermediates.at(dynamics.at(0)),
                {"filter", FilterNodeOptions(toComputeExpression(dynamics.at(1)))}}));
         } < "Project"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           auto projections = std::vector<compute::Expression>();
           auto names = std::vector<std::string>();
           for(auto i = 1; i < dynamics.size(); i++)
             visit(boss::utilities::overload(
                       [&](Symbol&& s) {
                         projections.push_back(compute::field_ref(s.getName()));
                         names.push_back(s.getName());
                       },
                       [&](ComplexExpression&& s) {
                         auto headName = s.getHead().getName();
                         auto arguments = std::vector<compute::Expression>();
                         for(auto const& it : s.getDynamicArguments())
                           arguments.push_back(toComputeExpression(it));
                         // "int"_(x) maps to a cast — Arrow compute doesn't expose cast by name
                         auto expr = headName == "int"
                                         ? compute::call("cast", arguments,
                                                         compute::CastOptions::Unsafe(int32()))
                                         : compute::call(headName, arguments);
                         projections.push_back(expr);
                         names.push_back(headName == "int"
                                             ? "int(" + arguments.back().ToString() + ")"
                                             : expr.ToString());
                       },
                       [](auto&&) {}),
                   std::move(dynamics.at(i)));
           return intermediates.put(
               Declaration::Sequence({intermediates.at(dynamics.at(0)),
                                      {"project", ProjectNodeOptions(projections, names)}}));
         } < "GroupBy"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           auto aggregates = std::vector<compute::Aggregate>();
           auto keys = std::vector<FieldRef>();
           auto i = 1u;
           for(; i < dynamics.size() && std::holds_alternative<ComplexExpression>(dynamics.at(i)); ++i) {
             auto const& fn = get<ComplexExpression>(dynamics.at(i));
             auto const col = get<Symbol>(fn.getDynamicArguments().at(0));
             aggregates.push_back({fn.getHead().getName(),
                                   {col.getName()},
                                   fn.getHead().getName() + "(" + col.getName() + ")"});
           }
           for(; i < dynamics.size(); ++i)
             keys.push_back(get<Symbol>(dynamics.at(i)).getName());
           if(!keys.empty())
             for(auto& agg : aggregates)
               agg.function = "hash_" + agg.function;
           auto aggDecl = Declaration::Sequence(
               {intermediates.at(dynamics.at(0)),
                {"aggregate", AggregateNodeOptions(aggregates, keys)}});
           if(!keys.empty())
             return intermediates.put(
                 {"table_source",
                  TableSourceNodeOptions(*DeclarationToTable(aggDecl, false))});
           return intermediates.put(std::move(aggDecl));
         } < "Cumulate"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           auto const& aggregationFunction = get<ComplexExpression>(dynamics.at(1));
           auto const aggregationAttribute =
               get<Symbol>(aggregationFunction.getDynamicArguments().at(0));
           auto input = (intermediates.getTable(intermediates.at(dynamics.at(0))));
           auto result =
               compute::CallFunction("cumulative_" + aggregationFunction.getHead().getName(),
                                     {input->GetColumnByName(aggregationAttribute.getName())})
                   ->chunked_array();
           return intermediates.put(
               {"table_source", TableSourceNodeOptions(*input->AddColumn(
                                    input->num_columns(),
                                    field(aggregationFunction.getHead().getName() + "(" +
                                              aggregationAttribute.getName() + ")",
                                          result->type()),
                                    result))});
         } < "Pairwise"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           auto options = compute::PairwiseOptions(get<int>(dynamics.at(3)));
           auto input = intermediates.getTable(intermediates.at(dynamics.at(0)));
           auto column = get<Symbol>(dynamics.at(2)).getName();
           auto inputArray =
               (input->GetColumnByName(column)->num_chunks() == 1 ? input : *input->CombineChunks())
                   ->GetColumnByName(column);
           auto result = compute::CallFunction("pairwise_diff", {inputArray->chunk(0)}, &options);
           return intermediates.put(
               {"table_source", TableSourceNodeOptions(*input->AddColumn(
                                    input->num_columns(),
                                    field(get<Symbol>(dynamics.at(1)).getName(), result->type()),
                                    *ChunkedArray::Make({result->make_array()})))});
         } < "ToStatus"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           return DeclarationToStatus(intermediates.at(dynamics.at(0)), false).CodeAsString();
         } < "Materialize"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           return intermediates.put(
               {"table_source",
                TableSourceNodeOptions(
                    *intermediates.getTable(intermediates.at(dynamics.at(0)))->CombineChunks())});
         } < "Table"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
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
                       auto append = [&](auto&& builder, auto type) {
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
                       };
                       withBuilder<Scalar>(append);
                     },
                     *firstTyped);
             } else {
               auto& dynamicArguments = column.getDynamicArguments();
               auto firstNonNull =
                   std::find_if(dynamicArguments.begin(), dynamicArguments.end(),
                                [](auto const& v) { return !std::holds_alternative<Symbol>(v); });
               if(firstNonNull != dynamicArguments.end())
                 visit(boss::utilities::overload(
                           [&]<typename T>(T const&)
                               requires(std::is_arithmetic_v<T> || std::is_same_v<T, std::string>) {
                                 auto append = [&](auto&& builder, auto type) {
                                   for(auto& argument : dynamicArguments)
                                     if(std::holds_alternative<Symbol>(argument))
                                       (void)builder.AppendNull();
                                     else
                                       (void)builder.Append(get<T>(argument));
                                   arrays.push_back(*builder.Finish());
                                   fields.push_back(arrow::field(columnName, type));
                                 };
                                 withBuilder<T>(append);
                               },
                           [](auto&&) {}),
                       *firstNonNull);
             }
           }
           return intermediates.put({"table_source", TableSourceNodeOptions(arrow::Table::Make(
                                                         arrow::schema(fields), arrays))});
         } < "Load"_(AnySequence_) >= Recurse(evaluate) > [](auto, auto dynamics, auto) {
           return std::visit(
               boss::utilities::overload(
                   [](std::string&& path) -> boss::Expression {
                     auto options = csv::ReadOptions::Defaults();
                     options.use_threads = false;
                     options.block_size = 1 << 26;
                     auto maybeTable =
                         (*csv::TableReader::Make(
                              io::default_io_context(), *io::ReadableFile::Open(path), options,
                              csv::ParseOptions::Defaults(), csv::ConvertOptions::Defaults()))
                             ->Read();
                     if(!maybeTable.ok())
                       return maybeTable.status().ToStringWithoutContextLines();
                     return intermediates.put(
                         {"table_source", TableSourceNodeOptions(*(*maybeTable)->CombineChunks())});
                   },
                   [](auto&& e) -> boss::Expression { return e; }),
               std::move(dynamics.at(0)));
         };
};

extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
  auto result = new BOSSExpression {
      .delegate = intermediates.convertResult(evaluate(std::move(e->delegate)))};
  intermediates.collectGarbage();
  return result;
};
