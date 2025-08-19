require_relative 'table_manager'
require_relative 'boolean_converter'
require_relative 'expression_evaluator'

class SqlExecutor
  def initialize
    @table_manager = TableManager.new
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
      { error: 'unknown_command' }
    end
  end
  
  private
  
  def has_error?(result)
    result.is_a?(Hash) && result[:error]
  end
  
  def execute_select(parsed_sql)
    expressions = parsed_sql[:expressions]
    columns = parsed_sql[:columns]
    
    if expressions.empty?
      return { rows: [] }
    end
    
    evaluator = ExpressionEvaluator.new
    values = []
    
    begin
      values = expressions.map do |expr|
        value = evaluator.evaluate(expr)
        BooleanConverter.convert(value)
      end
    rescue ExpressionEvaluator::ValidationError
      return { error: 'validation_error' }
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
    
    # Get table info
    table_info = @table_manager.get_table_info(table_name)
    return { error: 'validation_error' } unless table_info
    
    # Get all rows from table
    all_data = @table_manager.get_all_rows(table_name)
    return all_data if has_error?(all_data)
    
    evaluator = ExpressionEvaluator.new
    result_rows = []
    
    begin
      all_data[:rows].each do |row|
        # Build row data hash for column lookups
        row_data = {}
        table_info[:columns].each_with_index do |col, idx|
          row_data[col[:name]] = row[idx]
        end
        
        # Evaluate expressions for this row
        result_row = expressions.map do |expr_info|
          value = evaluator.evaluate(expr_info[:expression], row_data)
          BooleanConverter.convert(value)
        end
        result_rows << result_row
      end
    rescue ExpressionEvaluator::ValidationError
      return { error: 'validation_error' }
    end
    
    # Build column names from expressions or aliases
    column_names = expressions.map do |expr_info|
      if expr_info[:alias]
        expr_info[:alias]
      elsif expr_info[:expression][:type] == :column
        expr_info[:expression][:name]
      else
        ''
      end
    end
    
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
    
    evaluator = ExpressionEvaluator.new
    
    begin
      value_sets.each do |expressions|
        # Evaluate each expression to get actual values
        values = expressions.map do |expr|
          evaluator.evaluate(expr)
        end
        
        result = @table_manager.insert_row(table_name, values)
        return result if has_error?(result)
      end
    rescue ExpressionEvaluator::ValidationError
      return { error: 'validation_error' }
    end
    
    { status: 'ok' }
  end
end