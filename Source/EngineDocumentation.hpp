// Engine-local in-place operator documentation.
//
// Drop this header into a BOSS engine to make each operator self-documenting.
// Each operator's dispatch line reads
//     } < "Op"_(SIG) >= Description("text") > Recurse(evaluate) > [lambda]
// and the engine's `operator""_` defined below (which shadows BOSS's) captures
// the pattern's head and dynamic-arg symbol names into thread-locals so that
// Description's constructor — sitting in the visitor slot after `>=` — can
// register (head, signature, text) into `operatorDescriptions()` without
// repeating them at the call site. `GetEngineDescription`-style operators can
// then format their catalog by iterating the registry.
//
// Usage:
//   1. Include this header in the engine TU after BOSS headers.
//   2. DROP `using boss::utilities::operator""_;` — this file's literal shadows
//      it and adds the capture side-effect.
//   3. Insert `Description("…")` after `>=` in each dispatch entry.
//
// Intended to migrate into boss-core; lives here in the meantime. The
// anonymous namespace is deliberate: each engine TU gets its own thread-local
// capture state and its own registry; do not include this header in more than
// one TU per engine.

#pragma once

#include <Expression.hpp>
#include <ExpressionUtilities.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables): internal linkage via anonymous
// namespace; clang-tidy doesn't see that.
thread_local std::string lastPatternHead;
thread_local std::vector<std::string> lastPatternArgNames;
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

struct EngineSymbolLiteral {
  std::string_view name {};

  // Implicit conversion to boss::Symbol so value-only uses like `"NULL"_` work.
  operator boss::Symbol() const { // NOLINT(hicpp-explicit-conversions)
    return boss::Symbol(std::string(name));
  }

  // captureArgumentName takes args by const&; arguments are forwarded in the return.
  template <typename... Arguments>
  auto operator()(Arguments&&... arguments) const // NOLINT(cppcoreguidelines-missing-std-forward)
  {
    lastPatternHead.assign(name);
    lastPatternArgNames.clear();
    (captureArgumentName(arguments), ...);
    return boss::utilities::ExpressionBuilder(name.data())(std::forward<Arguments>(arguments)...);
  }

private:
  template <typename Argument> static void captureArgumentName(Argument const& argument) {
    if constexpr(std::is_same_v<std::decay_t<Argument>, boss::Symbol>) {
      if(argument == boss::utilities::experimental::sentinel::AnySequence_) {
        lastPatternArgNames.emplace_back("AnySequence_");
      } else if(argument == boss::utilities::experimental::sentinel::Any_) {
        lastPatternArgNames.emplace_back("Any_");
      } else {
        lastPatternArgNames.emplace_back(argument.getName());
      }
    } else {
      lastPatternArgNames.emplace_back("?");
    }
  }
};

// NOLINTNEXTLINE(llvm-prefer-static-over-anonymous-namespace)
EngineSymbolLiteral operator""_(char const* s, size_t length) {
  return EngineSymbolLiteral {std::string_view(s, length)};
}

struct OperatorDescription {
  std::string head {};
  std::string signature {};
  std::string text {};
};

// NOLINTNEXTLINE(llvm-prefer-static-over-anonymous-namespace)
std::vector<OperatorDescription>& operatorDescriptions() {
  static std::vector<OperatorDescription> registry;
  return registry;
}

// In-place operator documentation. Sits in the dispatch chain between `>=` and
// `> Recurse(evaluate)`. The constructor reads the head and argument-symbol
// names that this header's `operator""_` just captured for the preceding
// pattern and registers (head, signature, text). The visitor reconstructs the
// ComplexExpression unchanged so Recurse and the handler see the same payload.
struct Description {
  char const* text;

  explicit Description(char const* description) : text(description) {
    if(lastPatternHead.empty()) {
      return;
    }
    auto& registry = operatorDescriptions();
    if(std::find_if(registry.begin(), registry.end(), [](auto const& entry) {
         return entry.head == lastPatternHead;
       }) != registry.end()) {
      return;
    }
    std::string signature;
    for(size_t i = 0; i < lastPatternArgNames.size(); ++i) {
      if(i > 0) {
        signature += ", ";
      }
      signature += lastPatternArgNames[i];
    }
    registry.push_back({lastPatternHead, std::move(signature), text});
  }

  template <typename StaticArgumentTuple, typename DynamicArgumentContainer,
            typename SpanArgumentContainer>
  // head is moved and statics/dynamics/spans forwarded into the ComplexExpression.
  // NOLINTBEGIN(cppcoreguidelines-rvalue-reference-param-not-moved,cppcoreguidelines-missing-std-forward)
  boss::Expression operator()(boss::Symbol&& head, StaticArgumentTuple&& statics,
                              DynamicArgumentContainer&& dynamics,
                              SpanArgumentContainer&& spans) {
    return boss::ComplexExpression(std::move(head),
                                   std::forward<StaticArgumentTuple>(statics),
                                   std::forward<DynamicArgumentContainer>(dynamics),
                                   std::forward<SpanArgumentContainer>(spans));
  }
  // NOLINTEND(cppcoreguidelines-rvalue-reference-param-not-moved,cppcoreguidelines-missing-std-forward)
};

} // namespace
