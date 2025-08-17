class SqlExecutor
  def execute(parsed_sql)
    if parsed_sql[:error]
      return { error: parsed_sql[:error] }
    end
    
    case parsed_sql[:type]
    when :select
      execute_select(parsed_sql)
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
    
    converted_values = values.map do |value|
      case value
      when 'TRUE'
        true
      when 'FALSE'
        false
      else
        value
      end
    end
    
    result = { rows: [converted_values] }
    
    if columns.any?(&:itself)
      result[:columns] = columns.map { |col| col || '' }
    end
    
    result
  end
end