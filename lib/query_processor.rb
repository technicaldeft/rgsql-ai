require_relative 'boolean_converter'

class QueryProcessor
  def initialize(evaluator, row_processor, validator, group_by_processor)
    @evaluator = evaluator
    @row_processor = row_processor
    @validator = validator
    @group_by_processor = group_by_processor
  end
  
  def process_rows(row_contexts, expressions, where_clause, table_context)
    raise NotImplementedError, "Subclass must implement process_rows"
  end
  
  def process_grouped_rows(row_contexts, expressions, where_clause, group_by, table_context)
    raise NotImplementedError, "Subclass must implement process_grouped_rows"
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
    
    # For each group, evaluate the SELECT expressions on the first row
    grouped_rows.map do |group|
      representative_row = group.first
      evaluate_select_expressions(expressions, representative_row)
    end
  end
  
  def validate_query(parsed_sql, table_context, alias_mapping, dummy_row_data)
    table_name = parsed_sql[:table_name]
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    group_by = parsed_sql[:group_by]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    
    # Validate GROUP BY clause first
    if group_by
      @evaluator.validate_types(group_by, dummy_row_data)
      
      # Validate that SELECT expressions are valid with GROUP BY
      expressions.each do |expr_info|
        @validator.validate_group_by_expression(expr_info[:expression], group_by, dummy_row_data)
      end
    else
      # Validate SELECT expressions normally
      expressions.each do |expr_info|
        @validator.validate_qualified_columns(expr_info[:expression], table_name)
        @evaluator.validate_types(expr_info[:expression], dummy_row_data)
      end
    end
    
    # Validate WHERE clause
    if where_clause
      @evaluator.validate_types(where_clause, dummy_row_data)
      where_type = @evaluator.get_expression_type(where_clause, dummy_row_data)
      unless where_type == :boolean || where_type.nil?
        raise ExpressionEvaluator::ValidationError
      end
    end
    
    # Validate ORDER BY
    if order_by
      validate_order_by_expression(order_by, alias_mapping, dummy_row_data)
    end
    
    # Validate LIMIT and OFFSET
    if limit && !@validator.validate_limit_offset_expression(limit)
      raise ExpressionEvaluator::ValidationError
    end
    
    if offset && !@validator.validate_limit_offset_expression(offset)
      raise ExpressionEvaluator::ValidationError
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
  
  def validate_order_by_expression(order_by, alias_mapping, dummy_row_data)
    order_expr = order_by[:expression]
    if order_expr[:type] == :column && alias_mapping[order_expr[:name]]
      @evaluator.validate_types(alias_mapping[order_expr[:name]], dummy_row_data)
    else
      @evaluator.validate_types(order_expr, dummy_row_data)
    end
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
    
    # For each group, evaluate the SELECT expressions on the first row context
    grouped_contexts.map do |group|
      representative_context = group.first
      evaluate_select_expressions_with_context(expressions, representative_context, table_context)
    end
  end
  
  def validate_query(parsed_sql, table_context, alias_mapping, dummy_row_data)
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    group_by = parsed_sql[:group_by]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    joins = parsed_sql[:joins]
    
    # Validate SELECT expressions with context
    expressions.each do |expr_info|
      validation_error = @validator.validate_expression_with_context(expr_info[:expression], table_context)
      raise ExpressionEvaluator::ValidationError if validation_error
    end
    
    # Validate GROUP BY clause
    if group_by
      validation_error = @validator.validate_expression_with_context(group_by, table_context)
      raise ExpressionEvaluator::ValidationError if validation_error
      
      # TODO: Add GROUP BY validation for JOIN queries when needed
    end
    
    # Validate WHERE clause
    if where_clause
      validation_error = @validator.validate_expression_with_context(where_clause, table_context)
      raise ExpressionEvaluator::ValidationError if validation_error
    end
    
    # Validate JOINs
    validate_joins(joins, table_context)
    
    # Validate ORDER BY
    if order_by
      validation_error = @validator.validate_expression_with_context(order_by[:expression], table_context)
      raise ExpressionEvaluator::ValidationError if validation_error
    end
    
    # Validate LIMIT and OFFSET
    if limit && !@validator.validate_limit_offset_expression(limit)
      raise ExpressionEvaluator::ValidationError
    end
    
    if offset && !@validator.validate_limit_offset_expression(offset)
      raise ExpressionEvaluator::ValidationError
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
  
  def validate_joins(joins, table_context)
    joins.each do |join|
      condition = join[:on]
      
      # First validate the expression
      validation_error = @validator.validate_expression_with_context(condition, table_context)
      raise ExpressionEvaluator::ValidationError if validation_error
      
      # Check that JOIN condition evaluates to boolean
      join_type = @validator.get_expression_type_with_context(condition, table_context)
      unless join_type == :boolean || join_type.nil?
        raise ExpressionEvaluator::ValidationError
      end
      
      # Additional validation for comparison operators
      if condition[:type] == :binary_op && 
         (condition[:operator] == :equal || condition[:operator] == :not_equal)
        
        left_type = @validator.get_expression_type_with_context(condition[:left], table_context)
        right_type = @validator.get_expression_type_with_context(condition[:right], table_context)
        
        if (left_type == :integer && right_type == :boolean) ||
           (left_type == :boolean && right_type == :integer)
          raise ExpressionEvaluator::ValidationError
        end
      end
    end
  end
end