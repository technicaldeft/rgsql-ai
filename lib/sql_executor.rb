require_relative 'table_manager'
require_relative 'boolean_converter'

class SqlExecutor
  def initialize
    @table_manager = TableManager.new
  end
  
  def execute(parsed_sql)
    if parsed_sql[:error]
      return { error: parsed_sql[:error] }
    end
    
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
  
  def execute_select(parsed_sql)
    values = parsed_sql[:values]
    columns = parsed_sql[:columns]
    
    if values.empty?
      return { rows: [] }
    end
    
    converted_values = values.map { |value| BooleanConverter.convert(value) }
    
    result = { rows: [converted_values] }
    
    if columns.any?(&:itself)
      result[:columns] = columns.map { |col| col || '' }
    end
    
    result
  end
  
  def execute_select_from(parsed_sql)
    table_name = parsed_sql[:table_name]
    column_names = parsed_sql[:columns]
    
    result = @table_manager.select_from_table(table_name, column_names)
    return result if result[:error]
    
    converted_rows = result[:rows].map do |row|
      row.map { |value| BooleanConverter.convert(value) }
    end
    
    { rows: converted_rows, columns: result[:columns] }
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
    
    value_sets.each do |values|
      result = @table_manager.insert_row(table_name, values)
      return result if result[:error]
    end
    
    { status: 'ok' }
  end
end