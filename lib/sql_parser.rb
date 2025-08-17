class SqlParser
  BOOLEAN_TRUE = 'TRUE'
  BOOLEAN_FALSE = 'FALSE'
  PARSING_ERROR = 'parsing_error'
  
  def parse(sql)
    sql = sql.strip
    
    if sql.empty?
      return { error: PARSING_ERROR }
    end
    
    if sql.upcase.start_with?('SELECT')
      parse_select(sql)
    else
      { error: PARSING_ERROR }
    end
  end
  
  private
  
  def parse_select(sql)
    match = sql.match(/\ASELECT\s*(.*?)(?:\s*;\s*\z|\z)/i)
    
    if match.nil?
      return { error: PARSING_ERROR }
    end
    
    remainder = sql[match.end(0)..-1].strip
    if !remainder.empty?
      return { error: PARSING_ERROR }
    end
    
    select_list = match[1].strip
    
    if select_list.empty?
      return { type: :select, values: [], columns: [] }
    end
    
    values = []
    columns = []
    
    parts = split_select_list(select_list)
    
    parts.each do |part|
      parsed_value = parse_select_value(part.strip)
      return parsed_value if parsed_value[:error]
      
      values << parsed_value[:value]
      columns << parsed_value[:column]
    end
    
    { type: :select, values: values, columns: columns }
  end
  
  def parse_select_value(expression)
    if match = expression.match(/\A(-?\d+)(?:\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?\z/i)
      { value: match[1].to_i, column: match[2] }
    elsif match = expression.match(/\A(#{BOOLEAN_TRUE}|#{BOOLEAN_FALSE})(?:\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?\z/i)
      { value: match[1].upcase, column: match[2] }
    else
      { error: PARSING_ERROR }
    end
  end
  
  def split_select_list(select_list)
    parts = []
    current = ""
    depth = 0
    
    select_list.chars.each do |char|
      if char == ',' && depth == 0
        parts << current
        current = ""
      else
        current += char
        depth += 1 if char == '('
        depth -= 1 if char == ')'
      end
    end
    
    parts << current unless current.empty?
    parts
  end
end