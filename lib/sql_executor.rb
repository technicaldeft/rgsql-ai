require_relative 'table_manager'
require_relative 'boolean_converter'
require_relative 'expression_evaluator'
require_relative 'error_handler'
require_relative 'row_processor'
require_relative 'sql_validator'

class SqlExecutor
  include ErrorHandler
  def initialize
    @table_manager = TableManager.new
    @evaluator = ExpressionEvaluator.new
    @row_processor = RowProcessor.new(@evaluator)
    @validator = SqlValidator.new(@evaluator)
  end
  
  def execute(parsed_sql)
    return parsed_sql if has_error?(parsed_sql)
    
    case parsed_sql[:type]
    when :select
      execute_select(parsed_sql)
    when :select_from
      execute_select_from(parsed_sql)
    when :create_table
      execute_create_table(parsed_sql)
    when :drop_table
      execute_drop_table(parsed_sql)
    when :insert_multiple
      execute_insert_multiple(parsed_sql)
    else
      unknown_command_error
    end
  end
  
  private
  
  def execute_select(parsed_sql)
    expressions = parsed_sql[:expressions]
    columns = parsed_sql[:columns]
    
    if expressions.empty?
      return { rows: [] }
    end
    
    values = []
    
    begin
      # First validate types
      expressions.each do |expr|
        @evaluator.validate_types(expr)
      end
      
      # Then evaluate
      values = expressions.map do |expr|
        value = @evaluator.evaluate(expr)
        BooleanConverter.convert(value)
      end
    rescue ExpressionEvaluator::DivisionByZeroError
      return division_by_zero_error
    rescue ExpressionEvaluator::ValidationError
      return validation_error
    end
    
    result = { rows: [values] }
    
    if columns.any?(&:itself)
      result[:columns] = columns.map { |col| col || '' }
    end
    
    result
  end
  
  def execute_select_from(parsed_sql)
    table_name = parsed_sql[:table_name]
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    
    # Get table info
    table_info = @table_manager.get_table_info(table_name)
    return { error: 'validation_error' } unless table_info
    
    # Get all rows from table
    all_data = @table_manager.get_all_rows(table_name)
    return all_data if has_error?(all_data)
    
    result_rows = []
    
    # Build alias mapping for ORDER BY validation
    alias_mapping = build_alias_mapping(expressions)
    
    # Validate expressions against schema even if table is empty
    begin
      # Create a dummy row with appropriate types for validation
      dummy_row_data = create_dummy_row_data(table_info[:columns])
      
      # Validate SELECT expressions
      @validator.validate_select_expressions(expressions, dummy_row_data, table_name)
      
      # Validate WHERE clause if present
      if where_clause
        @validator.validate_qualified_columns(where_clause, table_name)
        return validation_error unless @validator.validate_where_clause(where_clause, dummy_row_data)
      end
      
      # Validate ORDER BY if present
      if order_by
        # First check if it's a simple alias reference
        if order_by[:expression][:type] == :column && alias_mapping[order_by[:expression][:name]]
          # It's an alias - this is allowed for simple references
        else
          # For expressions containing aliases, they're not allowed
          if @validator.contains_alias_in_expression?(order_by[:expression], alias_mapping)
            return validation_error
          end
          # Validate the ORDER BY expression normally
          @validator.validate_qualified_columns(order_by[:expression], table_name)
          @evaluator.validate_types(order_by[:expression], dummy_row_data)
        end
      end
      
      # Validate LIMIT if present
      if limit
        return validation_error unless @validator.validate_limit_offset_expression(limit)
      end
      
      # Validate OFFSET if present
      if offset
        return validation_error unless @validator.validate_limit_offset_expression(offset)
      end
      
      # Process rows with WHERE filtering
      filtered_rows = @row_processor.filter_rows_with_where(all_data[:rows], table_info[:columns], where_clause, expressions)
      
      # Apply ORDER BY if present
      if order_by
        sorted_rows = sort_rows(filtered_rows, order_by, alias_mapping, @evaluator, table_info[:columns])
        filtered_rows = sorted_rows
      end
      
      # Extract just the result rows
      result_rows = filtered_rows.map { |row_info| row_info[:result] }
      
      # Apply LIMIT and OFFSET
      if limit || offset
        result_rows = @row_processor.apply_limit_offset(result_rows, limit, offset)
      end
      
    rescue ExpressionEvaluator::DivisionByZeroError
      return division_by_zero_error
    rescue ExpressionEvaluator::ValidationError
      return validation_error
    end
    
    # Build column names from expressions or aliases
    column_names = extract_column_names(expressions)
    
    { rows: result_rows, columns: column_names }
  end
  
  def execute_create_table(parsed_sql)
    @table_manager.create_table(parsed_sql[:table_name], parsed_sql[:columns])
  end
  
  def execute_drop_table(parsed_sql)
    @table_manager.drop_table(parsed_sql[:table_name], if_exists: parsed_sql[:if_exists])
  end
  
  def execute_insert_multiple(parsed_sql)
    table_name = parsed_sql[:table_name]
    value_sets = parsed_sql[:value_sets]
    
    # Get table info for type validation
    table_info = @table_manager.get_table_info(table_name)
    return { error: 'validation_error' } unless table_info
    
    # First, evaluate and validate all rows
    all_values = []
    begin
      value_sets.each do |expressions|
        # Evaluate each expression to get actual values
        values = expressions.map do |expr|
          @evaluator.evaluate(expr)
        end
        
        # Validate types match table schema
        return validation_error unless @validator.validate_row_types(values, table_info[:columns])
        
        all_values << values
      end
    rescue ExpressionEvaluator::DivisionByZeroError
      return division_by_zero_error
    rescue ExpressionEvaluator::ValidationError
      return validation_error
    end
    
    # Only insert if all rows are valid
    all_values.each do |values|
      result = @table_manager.insert_row(table_name, values)
      return result if has_error?(result)
    end
    
    ok_status
  end
  
  def create_dummy_row_data(columns)
    dummy_row_data = {}
    columns.each do |col|
      case col[:type]
      when 'BOOLEAN'
        dummy_row_data[col[:name]] = true
      when 'INTEGER'
        dummy_row_data[col[:name]] = 0
      else
        dummy_row_data[col[:name]] = nil
      end
    end
    dummy_row_data
  end
  
  
  def extract_column_names(expressions)
    expressions.map do |expr_info|
      if expr_info[:alias]
        expr_info[:alias]
      elsif expr_info[:expression][:type] == :column
        expr_info[:expression][:name]
      elsif expr_info[:expression][:type] == :qualified_column
        expr_info[:expression][:column]
      else
        ''
      end
    end
  end
  
  
  def build_alias_mapping(expressions)
    mapping = {}
    expressions.each do |expr_info|
      if expr_info[:alias]
        mapping[expr_info[:alias]] = expr_info[:expression]
      end
    end
    mapping
  end
  
  
  def sort_rows(rows, order_by, alias_mapping, evaluator, columns)
    rows.sort do |a, b|
      a_value = evaluate_sort_value(a, order_by, alias_mapping, evaluator)
      b_value = evaluate_sort_value(b, order_by, alias_mapping, evaluator)
      
      apply_sort_direction(compare_values_for_sort(a_value, b_value), order_by[:direction])
    end
  end
  
  def evaluate_sort_value(row_info, order_by, alias_mapping, evaluator)
    expr = order_by[:expression]
    
    if is_alias_reference?(expr, alias_mapping)
      evaluator.evaluate(alias_mapping[expr[:name]], row_info[:row_data])
    else
      evaluator.evaluate(expr, row_info[:row_data])
    end
  end
  
  def is_alias_reference?(expr, alias_mapping)
    expr[:type] == :column && alias_mapping[expr[:name]]
  end
  
  def apply_sort_direction(comparison, direction)
    direction == 'DESC' ? -comparison : comparison
  end
  
  
  def compare_values_for_sort(a, b)
    # Handle NULLs - they sort as larger than any non-null value
    if a.nil? && b.nil?
      0
    elsif a.nil?
      1
    elsif b.nil?
      -1
    elsif a.is_a?(TrueClass) || a.is_a?(FalseClass)
      # Boolean comparison: false < true
      if b.is_a?(TrueClass) || b.is_a?(FalseClass)
        a_val = a ? 1 : 0
        b_val = b ? 1 : 0
        a_val <=> b_val
      else
        # Type mismatch
        0
      end
    else
      # Regular comparison
      a <=> b
    end
  end
  
end