# Refactoring Ideas

## ✅ COMPLETED REFACTORINGS

### Expression Matching and Comparison (COMPLETED)

**Problem:** The GROUP BY implementation revealed significant complexity in comparing and matching SQL expressions. The validation logic in `SqlValidator` had become intricate with multiple helper methods handling expression comparison.

**Solution Implemented:** Created `ExpressionMatcher` class that centralizes:
- Expression equality checking with normalization
- Subexpression detection
- Column extraction from expressions
- Semantic equivalence checking

**Benefits Achieved:**
- Simplified `SqlValidator` by removing ~150 lines of expression matching code
- Made expression comparison logic reusable
- Created a clear, testable abstraction for expression operations

### Query Type Polymorphism (COMPLETED)

**Problem:** Throughout the codebase, there was repeated branching based on whether a query has JOINs, making the code harder to follow and maintain.

**Solution Implemented:** Introduced polymorphic query processors:
- `SimpleQueryProcessor` for single-table queries
- `JoinQueryProcessor` for queries with JOINs
- Common `QueryProcessor` base class

**Benefits Achieved:**
- Eliminated conditional branching in core execution paths
- Each processor type has focused, clear responsibilities
- Easier to test each query type in isolation
- Cleaner code with better separation of concerns

### Table Context Management (COMPLETED)

**Problem:** The table context structure was passed around as a hash with unclear access patterns and no encapsulation.

**Solution Implemented:** Created `TableContext` class with proper methods:
- `primary_table` and `primary_table_info` for common access patterns
- `resolve_alias` for alias resolution
- `get_table_info` for unified access
- `validate_column` for validation
- `build_dummy_row_data` for validation setup

**Benefits Achieved:**
- Clear, documented interface for table context operations
- Eliminated direct hash access throughout the codebase
- Centralized context-related logic
- Better discoverability of available operations

## 📋 FUTURE REFACTORING IDEAS

### Validation Context

**Problem:** Validation logic is spread across multiple classes and methods with validation state passed around loosely.

**Proposed Solution:** Create a `ValidationContext` class that:
- Encapsulates validation state (schemas, dummy rows, etc.)
- Provides a consistent interface for all validation needs
- Can build appropriate dummy data for type checking
- Tracks validation errors with better context

**Expected Benefits:**
- Centralized validation logic
- Better error messages with context
- Easier to extend validation rules
- Clearer validation flow

### Expression Normalization

**Problem:** Expressions need to be normalized for comparison but this happens ad-hoc in various places.

**Proposed Solution:** Create an `ExpressionNormalizer` that:
- Converts expressions to a canonical form
- Handles case normalization
- Resolves qualified/unqualified references consistently
- Could be used before storing expressions for easier comparison

**Expected Benefits:**
- Consistent expression handling
- Easier expression comparison
- Reduced duplication of normalization logic
- Better support for future query optimization

### Row Context Builder Expansion

**Problem:** `RowContextBuilder` exists but could do more to simplify row handling across different query types.

**Proposed Solution:** Expand `RowContextBuilder` to:
- Provide a unified row representation
- Handle conversion between formats transparently
- Encapsulate the complexity of multi-table row contexts
- Support row context transformation operations

**Expected Benefits:**
- Simpler row handling code
- Unified approach to row contexts
- Better abstraction of row representation
- Easier to add new row operations

### Query Planner Enhancement

**Problem:** `QueryPlanner` exists but has limited responsibilities. Much planning logic still lives in `SqlExecutor`.

**Proposed Solution:** Expand `QueryPlanner` to:
- Take full responsibility for query planning
- Build complete execution plans
- Determine optimal execution order
- Handle query optimization decisions

**Expected Benefits:**
- Clear separation between planning and execution
- Easier to add query optimizations
- Better testability of planning logic
- Foundation for future optimization features

## 🎯 Benefits of Completed Refactorings

The three completed refactorings have significantly improved the codebase:

1. **Better Testing** - Smaller, focused classes are easier to test in isolation
2. **Clearer Intent** - Each class has a single, well-defined purpose
3. **Easier Extension** - Adding new features like aggregate functions will be simpler
4. **Reduced Complexity** - Less branching logic and clearer data flow
5. **Better Maintainability** - Code is more modular and easier to understand

## 🔍 Insights from Aggregate Functions Implementation

The implementation of aggregate functions (COUNT and SUM) revealed several architectural patterns and potential improvements:

### What Worked Well

1. **Query Processor Polymorphism** - The separation between `SimpleQueryProcessor` and `JoinQueryProcessor` made it easy to add aggregate support to both query types independently.

2. **Expression Type System** - Adding `:aggregate_function` as a new expression type integrated cleanly with the existing expression handling.

3. **Dedicated Evaluator Pattern** - Creating `AggregateEvaluator` as a specialized evaluator worked well, following the pattern of `ExpressionEvaluator`.

### New Refactoring Ideas from Implementation

#### Aggregate Function Registry

**Problem:** Aggregate functions are currently hardcoded in multiple places (parser, evaluator, validator).

**Proposed Solution:** Create an `AggregateFunctionRegistry` that:
- Defines available aggregate functions and their properties
- Centralizes type requirements (e.g., SUM requires integer)
- Provides a single source of truth for aggregate function behavior
- Makes it easy to add new aggregate functions (MIN, MAX, AVG, etc.)

**Expected Benefits:**
- Easier to add new aggregate functions
- Consistent validation across the codebase
- Better separation of concerns

#### Expression Visitor Pattern

**Problem:** Many operations need to traverse expressions (validation, evaluation, aggregate detection), leading to similar recursive code patterns.

**Proposed Solution:** Implement a visitor pattern for expressions:
- `ExpressionVisitor` base class with methods for each expression type
- Specialized visitors for different operations
- Consistent traversal logic

**Expected Benefits:**
- Eliminate duplicate traversal code
- Easier to add new operations on expressions
- Clearer separation of traversal from operation logic

#### Implicit Grouping as Explicit Concept

**Problem:** Implicit grouping (aggregate without GROUP BY) is handled as a special case in multiple places.

**Proposed Solution:** Make implicit grouping an explicit concept:
- Create an `ImplicitGroupBy` marker or transform queries to have explicit single group
- Unified handling of grouped and implicitly grouped queries
- Clearer semantics in the code

**Expected Benefits:**
- Simpler execution logic
- Fewer special cases
- Better code clarity

### Validation Complexity

The aggregate function implementation significantly increased validation complexity:
- Different validation rules for expressions with/without aggregates
- Context-dependent validation (aggregates allowed in SELECT but not WHERE)
- Interaction between GROUP BY and aggregate validation

This reinforces the need for the **Validation Context** refactoring mentioned earlier.

## 📊 Updated Priority for Future Work

Based on the aggregate functions implementation experience:

1. **Validation Context** - The validation logic has become complex enough to warrant extraction
2. **Expression Visitor Pattern** - Would simplify many expression operations
3. **Aggregate Function Registry** - Would make adding MIN, MAX, AVG much easier
4. **Expression Normalization** - Still valuable for consistent expression handling