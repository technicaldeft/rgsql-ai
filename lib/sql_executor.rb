require_relative 'table_manager'
require_relative 'boolean_converter'
require_relative 'expression_evaluator'
require_relative 'error_handler'
require_relative 'row_processor'
require_relative 'sql_validator'
require_relative 'row_sorter'
require_relative 'query_planner'

class SqlExecutor
  include ErrorHandler
  def initialize
    @table_manager = TableManager.new
    @evaluator = ExpressionEvaluator.new
    @row_processor = RowProcessor.new(@evaluator)
    @validator = SqlValidator.new(@evaluator)
    @row_sorter = RowSorter.new(@evaluator)
    @query_planner = QueryPlanner.new(@validator, @evaluator)
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
    alias_mapping = @query_planner.build_alias_mapping(expressions)
    
    # Validate expressions against schema even if table is empty
    begin
      unless @query_planner.validate_query(parsed_sql, table_info, alias_mapping)
        return validation_error
      end
      
      # Process rows with WHERE filtering
      filtered_rows = @row_processor.filter_rows_with_where(all_data[:rows], table_info[:columns], where_clause, expressions)
      
      # Apply ORDER BY if present
      if order_by
        filtered_rows = @row_sorter.sort_rows(filtered_rows, order_by, alias_mapping, table_info[:columns])
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
    column_names = @query_planner.extract_column_names(expressions)
    
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
  
  
  
  
  
  
end