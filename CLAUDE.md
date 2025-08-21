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
