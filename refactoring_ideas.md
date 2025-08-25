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

## 📊 Priority for Future Work

Based on upcoming features (aggregate functions), the highest priority future refactorings would be:

1. **Validation Context** - Aggregate functions will add new validation requirements
2. **Expression Normalization** - Will help with aggregate function validation
3. **Query Planner Enhancement** - Aggregate functions may need different execution strategies