# Refactoring Ideas

## Expression Matching and Comparison

### Problem
The GROUP BY implementation revealed significant complexity in comparing and matching SQL expressions. The validation logic in `SqlValidator` has become intricate with multiple helper methods (`expressions_equal?`, `expression_contains_subexpression?`, `extract_columns_not_in_expression_helper`) that handle:
- Matching expressions that are semantically equivalent but syntactically different (e.g., `ABS(a)` vs `ABS(t1.a)`)
- Handling qualified vs unqualified column references
- Case-insensitive comparisons
- Determining which columns are "allowed" based on GROUP BY expressions

### Proposed Solution
Extract an `ExpressionMatcher` class that centralizes:
- Expression equality checking with normalization
- Subexpression detection
- Column extraction from expressions
- Semantic equivalence checking

This would simplify `SqlValidator` and make expression comparison logic reusable for future features.

## Query Type Polymorphism

### Problem
Throughout the codebase, there's repeated branching based on whether a query has JOINs:
```ruby
if joins.empty?
  # Simple query logic
else  
  # JOIN query logic
end
```

This pattern appears in:
- `process_grouped_rows`
- `process_rows`
- `validate_query`
- `fetch_row_contexts`
- Multiple other methods in `SqlExecutor`

### Proposed Solution
Introduce a strategy pattern or polymorphic query processors:
- `SimpleQueryProcessor` for single-table queries
- `JoinQueryProcessor` for queries with JOINs
- Common interface that both implement

This would eliminate the branching logic and make each path clearer and more testable.

## Table Context Management

### Problem
The table context structure (`{tables: {}, aliases: {}}`) is:
- Passed through many methods as a hash
- Accessed inconsistently (sometimes `context[:tables].keys.first`, sometimes `context[:aliases].values.first`)
- Makes it unclear what data is available and how to access it
- Lacks encapsulation and validation

### Proposed Solution
Create a `TableContext` class with proper methods:
- `primary_table` method instead of `tables.keys.first`
- `resolve_alias(name)` for alias resolution
- `get_table_info(name_or_alias)` for unified access
- `validate_column(table_ref, column_name)` for validation
- Immutable or controlled mutation

## Validation Context

### Problem
Validation logic is spread across multiple classes and methods:
- `SqlValidator` handles some validation
- `SqlExecutor` handles other validation
- `ExpressionEvaluator` has its own validation
- Validation state (dummy rows, table info) is passed around loosely

### Proposed Solution
Create a `ValidationContext` class that:
- Encapsulates validation state (schemas, dummy rows, etc.)
- Provides a consistent interface for all validation needs
- Can build appropriate dummy data for type checking
- Tracks validation errors with better context

## Expression Normalization

### Problem
Expressions need to be normalized for comparison but this happens ad-hoc:
- Case conversion happens in multiple places
- Qualified/unqualified column resolution is scattered
- No central place for expression transformation

### Proposed Solution
Create an `ExpressionNormalizer` that:
- Converts expressions to a canonical form
- Handles case normalization
- Resolves qualified/unqualified references consistently
- Could be used before storing expressions for easier comparison

## Row Context Builder Expansion

### Problem
`RowContextBuilder` exists but could do more to simplify row handling:
- Different representations for simple vs JOIN queries
- Conversion between representations happens in multiple places

### Proposed Solution
Expand `RowContextBuilder` to:
- Provide a unified row representation
- Handle conversion between formats transparently
- Encapsulate the complexity of multi-table row contexts

## Query Planner Enhancement

### Problem
`QueryPlanner` exists but has limited responsibilities. Much planning logic still lives in `SqlExecutor`.

### Proposed Solution
Expand `QueryPlanner` to:
- Take full responsibility for query planning
- Determine execution strategy (simple vs join)
- Build execution plans that executors can follow
- Separate planning from execution more clearly

## Benefits of These Refactorings

1. **Easier to test** - Smaller, focused classes with single responsibilities
2. **Easier to extend** - Adding aggregate functions or window functions would be simpler
3. **Better error messages** - Centralized validation with context
4. **Performance optimization** - Clearer separation would make it easier to optimize specific paths
5. **Code clarity** - Less branching logic and clearer intent

## Priority

Based on upcoming features (aggregate functions), the highest priority refactorings would be:
1. Expression matching/comparison extraction (will be heavily used for aggregate validation)
2. Query type polymorphism (aggregate functions need different handling for grouped vs non-grouped)
3. Validation context (aggregate functions add new validation requirements)