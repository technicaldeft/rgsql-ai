## Project

Implement SQL database server in Ruby using TDD.

+ The project already has tests to define the behaviour of the database.
+ You should not modify the files in the 'tests' or 'test_runner' directories.

## Implementation choices

+ You can use techniques from real databases and programming languages if it helps to make a test pass and keep the solution simple
+ You do not need to persist table data on disk, store data in memory instead
+ You do not need to implement any database or SQL features that are not tested.

## Workflow

You will be asked to make all the tests in a file pass.

+ You have made the current file pass when the error mentions the next numbered test file.
+ You should run the whole test suite each time so that you catch and fix any regressions.
+ Run the tests with the `./run-tests` command and then write code to make the failing tests pass.
+ Create a commit each time you implement a logical set of functionality. Prefer smaller, atomic commits and you can create multiple commits per test file.

## Code style

+ Use common patterns and features of Ruby (Ruby version 3.3).
+ Write understandable and maintainable code
+ Try to use organize code into smaller methods
+ Prefer self documenting code with descriptive methods and variables rather than using comments.

## Refactoring

You may be asked to refactor the code in order to make it easier to understand and extend.

+ Think about what refactors to implement and the pros and cons of each.
+ Look for refactors that extract concepts or create abstractions that simplify the code.
+ Avoid premature optimization and architecting of code until there is a need to.
+ It's fine not to perform any refactors, or to save refactoring ideas for later.
+ You MUST create a DIFFERENT commit each refactor. Do NOT put multiple refactors in a single commit

## Current Architecture Overview

The database implementation follows a modular architecture with clear separation of concerns:

### Core Components

**Server (`lib/server.rb`)**
- TCP server listening on port 3003
- Maintains persistent SqlParser and SqlExecutor instances
- Handles client connections and message protocol

**SQL Processing Pipeline**

1. **SqlParser (`lib/sql_parser.rb`)**
   - Entry point for SQL statement parsing
   - Delegates to specialized parsers based on statement type
   - Returns structured AST representation or parsing errors

2. **Specialized Parsers**
   - `SelectParser` - Handles SELECT statements with FROM, JOIN, WHERE, ORDER BY, LIMIT/OFFSET
   - `TableParser` - Parses CREATE TABLE and DROP TABLE statements  
   - `InsertParser` - Parses INSERT INTO statements with multiple value sets
   - `ExpressionParser` - Tokenizes and parses SQL expressions into AST

3. **SqlExecutor (`lib/sql_executor.rb`)**
   - Orchestrates query execution
   - Routes parsed statements to appropriate execution methods
   - Handles both simple queries and complex JOINs
   - Manages validation and error handling

### Data Management

**TableManager (`lib/table_manager.rb`)**
- In-memory storage of table schemas and data
- CRUD operations for tables and rows
- Schema validation and constraint checking

### Query Processing

**QueryProcessor (`lib/query_processor.rb`)**
- Abstract base class for query processing strategies
- `SimpleQueryProcessor` - Handles single-table queries
- `JoinQueryProcessor` - Handles queries with JOINs
- Eliminates conditional branching based on query type

**QueryPlanner (`lib/query_planner.rb`)**
- Validates queries against table schemas
- Builds column mappings and alias resolution
- Extracts column names from expressions

**SqlValidator (`lib/sql_validator.rb`)**
- Type checking for expressions
- Column existence validation
- JOIN condition validation
- Context-aware validation for multi-table queries
- Delegates expression matching to ExpressionMatcher

**ExpressionMatcher (`lib/expression_matcher.rb`)**
- Expression equality checking with normalization
- Subexpression detection and matching
- Column extraction from expressions
- Handles qualified/unqualified column matching

**ExpressionEvaluator (`lib/expression_evaluator.rb`)**
- Evaluates SQL expressions against row data
- Type inference and validation
- NULL handling and three-valued logic
- Support for operators, functions, and column references

**GroupByProcessor (`lib/group_by_processor.rb`)**
- Groups rows based on GROUP BY expressions
- Handles NULL values in grouping keys
- Supports both simple and complex grouping expressions

**RowProcessor (`lib/row_processor.rb`)**
- Filters rows based on WHERE conditions
- Projects columns for SELECT expressions
- Applies LIMIT/OFFSET pagination

**RowSorter (`lib/row_sorter.rb`)**
- Implements ORDER BY sorting
- Handles NULL ordering semantics
- Supports alias references in ORDER BY

**RowContextBuilder (`lib/row_context_builder.rb`)**
- Builds row contexts for query processing
- Handles single-table and join contexts
- Manages row data extraction

### Supporting Modules

**TableContext (`lib/table_context.rb`)**
- Encapsulates table context data and operations
- Provides semantic methods for context access
- Manages table aliases and column resolution
- Builds dummy row data for validation

**BooleanConverter (`lib/boolean_converter.rb`)**
- Converts Ruby boolean values to SQL TRUE/FALSE strings

**ErrorHandler (`lib/error_handler.rb`)**
- Centralized error types and handling
- Consistent error response format

**SqlConstants (`lib/sql_constants.rb`)**
- SQL keywords, token types, and patterns
- Shared constants across parsers

**ParsingUtils (`lib/parsing_utils.rb`)**
- Common parsing utilities
- Comma splitting with parenthesis awareness
- Expression extraction helpers

### Key Design Patterns

1. **Separation of Concerns** - Each component has a single, well-defined responsibility
2. **AST-based Processing** - SQL is parsed into an abstract syntax tree for evaluation
3. **Context Objects** - Table and row contexts encapsulated in dedicated classes
4. **Modular Parsers** - Specialized parsers for different SQL constructs
5. **Validator Pattern** - Separate validation from execution logic
6. **Strategy Pattern** - Polymorphic query processors for different query types
7. **Delegation Pattern** - Complex operations delegated to specialized helper classes

### Data Flow

1. Client sends SQL string to Server
2. SqlParser converts string to AST
3. SqlExecutor validates AST against schema
4. Query processor evaluates expressions and filters rows
5. Results formatted and returned to client

This architecture supports the current feature set including:
- Basic CRUD operations
- Complex SELECT queries with multiple clauses
- All types of JOINs (INNER, LEFT, RIGHT, FULL OUTER)
- Expression evaluation with type checking
- NULL handling and three-valued logic
- Table aliasing and qualified column references
