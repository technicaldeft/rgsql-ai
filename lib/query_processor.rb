require_relative 'boolean_converter'
require_relative 'aggregate_evaluator'
require_relative 'validation_context'

class QueryProcessor
  def initialize(evaluator, row_processor, validator, group_by_processor)
    @evaluator = evaluator
    @row_processor = row_processor
    @validator = validator
    @group_by_processor = group_by_processor
    @aggregate_evaluator = AggregateEvaluator.new(evaluator)
  end
  
  def process_rows(row_contexts, expressions, where_clause, table_context)
    raise NotImplementedError, "Subclass must implement process_rows"
  end
  
  def process_grouped_rows(row_contexts, expressions, where_clause, group_by, table_context)
    raise NotImplementedError, "Subclass must implement process_grouped_rows"
  end
  
  def process_implicit_group(row_contexts, expressions, where_clause, table_context)
    raise NotImplementedError, "Subclass must implement process_implicit_group"
  end
  
  def validate_query(parsed_sql, table_context, alias_mapping, dummy_row_data)
    raise NotImplementedError, "Subclass must implement validate_query"
  end
  
  protected
  
  def evaluate_select_expressions(expressions, row_data)
    expressions.map do |expr_info|
      value = @evaluator.evaluate(expr_info[:expression], row_data)
      BooleanConverter.convert(value)
    end
  end
  
  def evaluate_select_expressions_with_context(expressions, row_context, table_context)
    expressions.map do |expr_info|
      value = @evaluator.evaluate_with_context(expr_info[:expression], row_context, table_context)
      BooleanConverter.convert(value)
    end
  end
  
  def evaluate_select_expressions_with_aggregates(expressions, group_rows)
    expressions.map do |expr_info|
      value = @aggregate_evaluator.evaluate_aggregate(expr_info[:expression], group_rows)
      BooleanConverter.convert(value)
    end
  end
  
  def evaluate_select_expressions_with_aggregates_context(expressions, group_contexts, table_context)
    expressions.map do |expr_info|
      value = @aggregate_evaluator.evaluate_aggregate_with_context(expr_info[:expression], group_contexts, table_context)
      BooleanConverter.convert(value)
    end
  end
  
end

class SimpleQueryProcessor < QueryProcessor
  def process_rows(row_contexts, expressions, where_clause, table_context)
    table_info = table_context[:tables].values.first
    
    # Filter rows based on WHERE clause
    filtered_contexts = if where_clause
      row_contexts.select do |row_context|
        row_data = extract_row_data(row_context, table_info)
        @evaluator.evaluate(where_clause, row_data) == true
      end
    else
      row_contexts
    end
    
    # Evaluate SELECT expressions for each row
    filtered_contexts.map do |row_context|
      row_data = extract_row_data(row_context, table_info)
      result = evaluate_select_expressions(expressions, row_data)
      { result: result, row_data: row_data }
    end
  end
  
  def process_grouped_rows(row_contexts, expressions, where_clause, group_by, table_context)
    table_info = table_context[:tables].values.first
    
    # Filter rows based on WHERE clause
    filtered_contexts = if where_clause
      row_contexts.select do |row_context|
        row_data = extract_row_data(row_context, table_info)
        @evaluator.evaluate(where_clause, row_data) == true
      end
    else
      row_contexts
    end
    
    # Convert row contexts to row data
    rows_data = filtered_contexts.map do |row_context|
      extract_row_data(row_context, table_info)
    end
    
    # Return empty if no rows
    return [] if rows_data.empty?
    
    # Group by the GROUP BY expression
    grouped_rows = @group_by_processor.group_rows(rows_data, group_by, table_context)
    
    # For each group, evaluate the SELECT expressions with aggregate support
    grouped_rows.map do |group|
      evaluate_select_expressions_with_aggregates(expressions, group)
    end
  end
  
  def process_implicit_group(row_contexts, expressions, where_clause, table_context)
    table_info = table_context[:tables].values.first
    
    # Filter rows based on WHERE clause
    filtered_contexts = if where_clause
      row_contexts.select do |row_context|
        row_data = extract_row_data(row_context, table_info)
        @evaluator.evaluate(where_clause, row_data) == true
      end
    else
      row_contexts
    end
    
    # Convert row contexts to row data
    rows_data = filtered_contexts.map do |row_context|
      extract_row_data(row_context, table_info)
    end
    
    # If no rows, return a single result with aggregate defaults
    if rows_data.empty?
      # For empty result set, COUNT returns 0, SUM returns NULL
      result = evaluate_select_expressions_with_aggregates(expressions, [])
      return [result]
    end
    
    # Treat all rows as a single group
    [evaluate_select_expressions_with_aggregates(expressions, rows_data)]
  end
  
  def validate_query(parsed_sql, table_context, alias_mapping, dummy_row_data)
    table_name = parsed_sql[:table_name]
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    group_by = parsed_sql[:group_by]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    
    # Create validation context for simple single-table queries
    # Use the existing table_context parameter which already has the table info
    validation_context = ValidationContext.new(table_context, @evaluator, @validator)
                        .with_aliases(alias_mapping)
    
    # Check if we have aggregate functions in SELECT
    has_aggregates = expressions.any? do |expr_info|
      validation_context.has_aggregate_functions?(expr_info[:expression])
    end
    
    # Validate GROUP BY clause first
    if group_by
      @evaluator.validate_types(group_by, dummy_row_data)
      
      # Validate that SELECT expressions are valid with GROUP BY
      expressions.each do |expr_info|
        @validator.validate_group_by_expression(expr_info[:expression], group_by, dummy_row_data)
      end
    elsif has_aggregates
      # Implicit grouping - validate aggregate expressions
      result = validation_context.validate_implicit_grouping(expressions)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    else
      # Validate SELECT expressions normally
      expressions.each do |expr_info|
        @validator.validate_qualified_columns(expr_info[:expression], table_name)
        @evaluator.validate_types(expr_info[:expression], dummy_row_data)
      end
    end
    
    # Validate WHERE clause
    if where_clause
      result = validation_context.validate_where_clause(where_clause)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    # Validate ORDER BY
    if order_by
      result = validation_context.validate_order_by(order_by)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    # Validate LIMIT and OFFSET
    if limit
      result = validation_context.validate_limit_offset(limit)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    if offset
      result = validation_context.validate_limit_offset(offset)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    nil
  end
  
  private
  
  def extract_row_data(row_context, table_info)
    row_data = {}
    
    # Handle both simple row contexts and complex ones
    if row_context.is_a?(Hash) && row_context[:row]
      # Simple row context with :row key
      table_info[:columns].each_with_index do |col, i|
        row_data[col[:name]] = row_context[:row][i]
      end
    elsif row_context.is_a?(Hash)
      # Complex row context - extract column values directly
      table_info[:columns].each do |col|
        row_data[col[:name]] = row_context[col[:name]]
      end
    end
    
    row_data
  end
end

class JoinQueryProcessor < QueryProcessor
  def process_rows(row_contexts, expressions, where_clause, table_context)
    # Filter rows based on WHERE clause
    filtered_contexts = if where_clause
      row_contexts.select do |row_context|
        @evaluator.evaluate_with_context(where_clause, row_context, table_context) == true
      end
    else
      row_contexts
    end
    
    # Evaluate SELECT expressions for each row
    filtered_contexts.map do |row_context|
      result = evaluate_select_expressions_with_context(expressions, row_context, table_context)
      { result: result, row_context: row_context }
    end
  end
  
  def process_grouped_rows(row_contexts, expressions, where_clause, group_by, table_context)
    # Filter rows based on WHERE clause
    filtered_contexts = if where_clause
      row_contexts.select do |row_context|
        @evaluator.evaluate_with_context(where_clause, row_context, table_context) == true
      end
    else
      row_contexts
    end
    
    # Return empty if no rows
    return [] if filtered_contexts.empty?
    
    # Group the row contexts directly
    grouped_contexts = group_join_contexts(filtered_contexts, group_by, table_context)
    
    # For each group, evaluate the SELECT expressions with aggregate support
    grouped_contexts.map do |group|
      evaluate_select_expressions_with_aggregates_context(expressions, group, table_context)
    end
  end
  
  def process_implicit_group(row_contexts, expressions, where_clause, table_context)
    # Filter rows based on WHERE clause
    filtered_contexts = if where_clause
      row_contexts.select do |row_context|
        @evaluator.evaluate_with_context(where_clause, row_context, table_context) == true
      end
    else
      row_contexts
    end
    
    # If no rows, return a single result with aggregate defaults
    if filtered_contexts.empty?
      # For empty result set, COUNT returns 0, SUM returns NULL
      result = evaluate_select_expressions_with_aggregates_context(expressions, [], table_context)
      return [result]
    end
    
    # Treat all rows as a single group
    [evaluate_select_expressions_with_aggregates_context(expressions, filtered_contexts, table_context)]
  end
  
  def validate_query(parsed_sql, table_context, alias_mapping, dummy_row_data)
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    group_by = parsed_sql[:group_by]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    joins = parsed_sql[:joins]
    
    # Create validation context for join queries
    validation_context = ValidationContext.new(table_context, @evaluator, @validator)
                        .with_aliases(alias_mapping)
    
    # Check if we have aggregate functions in SELECT
    has_aggregates = expressions.any? do |expr_info|
      validation_context.has_aggregate_functions?(expr_info[:expression])
    end
    
    # Validate SELECT expressions with context and GROUP BY
    if group_by
      # Validate GROUP BY expression and SELECT expressions
      result = validation_context.validate_group_by_clause(expressions, group_by)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    elsif has_aggregates
      # Implicit grouping - validate aggregate expressions
      result = validation_context.validate_implicit_grouping(expressions)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    else
      # Normal validation for non-aggregate queries
      expressions.each do |expr_info|
        result = validation_context.validate_expression(expr_info[:expression])
        raise ExpressionEvaluator::ValidationError if result && result[:error]
      end
    end
    
    # Validate WHERE clause
    if where_clause
      result = validation_context.validate_where_clause(where_clause)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    # Validate JOINs
    validate_joins(joins, table_context, validation_context)
    
    # Validate ORDER BY
    if order_by
      result = validation_context.validate_order_by(order_by)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    # Validate LIMIT and OFFSET
    if limit
      result = validation_context.validate_limit_offset(limit)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    if offset
      result = validation_context.validate_limit_offset(offset)
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
    
    nil
  end
  
  private
  
  def group_join_contexts(row_contexts, group_by_expr, table_context)
    groups = {}
    
    row_contexts.each do |row_context|
      group_key = @evaluator.evaluate_with_context(group_by_expr, row_context, table_context)
      group_key = group_key.nil? ? :null_group : group_key
      
      groups[group_key] ||= []
      groups[group_key] << row_context
    end
    
    groups.values
  end
  
  def validate_joins(joins, table_context, validation_context)
    joins.each do |join|
      result = validation_context.validate_join_condition(join[:on])
      raise ExpressionEvaluator::ValidationError if result && result[:error]
    end
  end
end