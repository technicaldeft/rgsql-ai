class Executor
  def execute(parsed_query)
    return parsed_query if parsed_query[:error]
    
    case parsed_query[:type]
    when :select
      execute_select(parsed_query)
    else
      { error: 'unknown_statement' }
    end
  end
  
  private
  
  def execute_select(query)
    values = query[:values]
    columns = query[:columns]
    
    # Handle empty SELECT
    if values.empty?
      return { rows: [] }
    end
    
    # Build result
    result = { rows: [values] }
    
    # Add column names if specified
    if columns.any? { |c| !c.nil? }
      result[:columns] = columns.map.with_index do |col, i|
        col || "column_#{i + 1}"
      end
    end
    
    result
  end
end