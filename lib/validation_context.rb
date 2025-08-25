require_relative 'expression_evaluator'
require_relative 'aggregate_evaluator'
require_relative 'expression_matcher'

class ValidationContext
  attr_reader :table_context, :evaluator, :validator, :aggregate_evaluator, :expression_matcher
  
  def initialize(table_context, evaluator, validator)
    @table_context = table_context
    @evaluator = evaluator
    @validator = validator
    @aggregate_evaluator = AggregateEvaluator.new(evaluator)
    @expression_matcher = ExpressionMatcher.new
    @dummy_row_data = nil
    @alias_mapping = {}
  end
  
  def with_aliases(alias_mapping)
    @alias_mapping = alias_mapping
    self
  end
  
  def dummy_row_data
    @dummy_row_data ||= build_dummy_row_data
  end
  
  def validate_expression(expression)
    validate_columns_in_expression(expression)
    
    if has_aggregate_functions?(expression)
      validate_expression_with_aggregates(expression)
    else
      @evaluator.validate_types(expression, dummy_row_data)
    end
    
    nil
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error', message: e.message }
  end
  
  def validate_where_clause(where_clause)
    if has_aggregate_functions?(where_clause)
      raise ExpressionEvaluator::ValidationError, "Aggregate functions not allowed in WHERE clause"
    end
    
    # Validate columns and types in the expression
    validate_columns_in_expression(where_clause)
    @evaluator.validate_types(where_clause, dummy_row_data)
    
    where_type = @evaluator.get_expression_type(where_clause, dummy_row_data)
    unless where_type == :boolean || where_type.nil?
      raise ExpressionEvaluator::ValidationError, "WHERE clause must evaluate to boolean"
    end
    
    nil
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error', message: e.message }
  end
  
  def validate_group_by_clause(select_expressions, group_by_expression)
    validate_expression(group_by_expression)
    
    select_expressions.each do |expr_info|
      validate_select_with_group_by(expr_info[:expression], group_by_expression)
    end
    
    nil
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error', message: e.message }
  end
  
  def validate_implicit_grouping(select_expressions)
    select_expressions.each do |expr_info|
      expr = expr_info[:expression]
      if expr[:type] == :aggregate_function || has_aggregate_functions?(expr)
        validate_expression_with_aggregates(expr)
      else
        unless expr[:type] == :literal
          raise ExpressionEvaluator::ValidationError, 
                "Non-aggregate expressions require GROUP BY when aggregates are present"
        end
      end
    end
    
    nil
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error', message: e.message }
  end
  
  def validate_order_by(order_by_expression)
    order_expr = order_by_expression[:expression]
    
    if order_expr[:type] == :column && @alias_mapping[order_expr[:name]]
      aliased_expr = @alias_mapping[order_expr[:name]]
      validate_columns_in_expression(aliased_expr)
      if has_aggregate_functions?(aliased_expr)
        @aggregate_evaluator.validate_aggregate_types(aliased_expr, dummy_row_data)
      else
        @evaluator.validate_types(aliased_expr, dummy_row_data)
      end
    else
      validate_columns_in_expression(order_expr)
      if has_aggregate_functions?(order_expr)
        @aggregate_evaluator.validate_aggregate_types(order_expr, dummy_row_data)
      else
        @evaluator.validate_types(order_expr, dummy_row_data)
      end
    end
    
    nil
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error', message: e.message }
  end
  
  def validate_limit_offset(expression)
    if @expression_matcher.contains_column_reference?(expression)
      raise ExpressionEvaluator::ValidationError, "LIMIT/OFFSET cannot contain column references"
    end
    
    if has_aggregate_functions?(expression)
      raise ExpressionEvaluator::ValidationError, "LIMIT/OFFSET cannot contain aggregate functions"
    end
    
    @evaluator.validate_types(expression, {})
    expr_type = @evaluator.get_expression_type(expression, {})
    
    unless expr_type == :integer || expr_type.nil?
      raise ExpressionEvaluator::ValidationError, "LIMIT/OFFSET must evaluate to integer"
    end
    
    nil
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error', message: e.message }
  end
  
  def validate_join_condition(join_condition)
    validate_columns_in_expression(join_condition)
    
    # Check type compatibility for comparison operators
    if join_condition[:type] == :binary_op && 
       (join_condition[:operator] == :equal || join_condition[:operator] == :not_equal)
      
      left_type = get_expression_type(join_condition[:left])
      right_type = get_expression_type(join_condition[:right])
      
      if (left_type == :integer && right_type == :boolean) ||
         (left_type == :boolean && right_type == :integer)
        raise ExpressionEvaluator::ValidationError, "Type mismatch in JOIN condition"
      end
    end
    
    condition_type = get_expression_type(join_condition)
    unless condition_type == :boolean || condition_type.nil?
      raise ExpressionEvaluator::ValidationError, "JOIN condition must evaluate to boolean"
    end
    
    nil
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error', message: e.message }
  end
  
  def has_aggregate_functions?(expression)
    @aggregate_evaluator.has_aggregate_functions?(expression)
  end
  
  def get_expression_type(expression)
    @validator.get_expression_type_with_context(expression, @table_context)
  end
  
  private
  
  def build_dummy_row_data
    if @table_context.is_a?(TableContext)
      @table_context.build_dummy_row_data
    else
      # For multi-table contexts, build dummy data for all tables
      dummy_data = {}
      @table_context[:tables].each do |table_name, table_info|
        table_info[:columns].each do |col|
          dummy_data[col[:name]] = case col[:type]
                                  when 'INTEGER' then 1
                                  when 'BOOLEAN' then true
                                  else nil
                                  end
        end
      end
      dummy_data
    end
  end
  
  def validate_columns_in_expression(expression)
    @validator.validate_columns_in_context(expression, table_context_hash)
  end
  
  def validate_expression_with_aggregates(expression)
    @validator.validate_aggregate_expression_with_context(expression, table_context_hash)
  end
  
  def validate_select_with_group_by(select_expr, group_by_expr)
    if select_expr[:type] == :aggregate_function
      validate_expression_with_aggregates(select_expr)
    elsif has_aggregate_functions?(select_expr)
      validate_expression_with_aggregates(select_expr)
      validate_non_aggregate_parts_in_group_by(select_expr, group_by_expr)
    else
      validate_non_aggregate_in_group_by(select_expr, group_by_expr)
    end
  end
  
  def validate_non_aggregate_parts_in_group_by(expr, group_by_expr)
    case expr[:type]
    when :binary_op
      validate_non_aggregate_parts_in_group_by(expr[:left], group_by_expr) if expr[:left]
      validate_non_aggregate_parts_in_group_by(expr[:right], group_by_expr) if expr[:right]
    when :unary_op
      validate_non_aggregate_parts_in_group_by(expr[:operand], group_by_expr) if expr[:operand]
    when :function
      expr[:args]&.each { |arg| validate_non_aggregate_parts_in_group_by(arg, group_by_expr) }
    when :column, :qualified_column
      unless column_allowed_in_group_by?(expr, group_by_expr)
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{expr[:name] || expr[:column]}"
      end
    end
  end
  
  def validate_non_aggregate_in_group_by(expr, group_by_expr)
    return if @expression_matcher.expressions_equal?(expr, group_by_expr)
    
    columns = @expression_matcher.extract_columns_from_expression(expr)
    columns.each do |col|
      unless column_allowed_in_group_by?(col, group_by_expr)
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{col[:name] || col[:column]}"
      end
    end
  end
  
  def column_allowed_in_group_by?(column, group_by_expr)
    @expression_matcher.expressions_equal?(column, group_by_expr) ||
    @expression_matcher.expression_contains_column?(group_by_expr, column)
  end
  
  def table_context_hash
    @table_context.is_a?(TableContext) ? @table_context.to_hash : @table_context
  end
end