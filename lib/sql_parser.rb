class SqlParser
  def parse(sql)
    sql = sql.strip
    
    if sql.empty?
      return { error: 'parsing_error' }
    end
    
    if sql.upcase.start_with?('SELECT')
      parse_select(sql)
    else
      { error: 'parsing_error' }
    end
  end
  
  private
  
  def parse_select(sql)
    match = sql.match(/\ASELECT\s*(.*?)(?:\s*;\s*\z|\z)/i)
    
    if match.nil?
      return { error: 'parsing_error' }
    end
    
    remainder = sql[match.end(0)..-1].strip
    if !remainder.empty?
      return { error: 'parsing_error' }
    end
    
    select_list = match[1].strip
    
    if select_list.empty?
      return { type: :select, values: [], columns: [] }
    end
    
    values = []
    columns = []
    
    parts = split_select_list(select_list)
    
    parts.each_with_index do |part, index|
      part = part.strip
      
      if part.match(/\A(-?\d+)(?:\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?\z/i)
        value = $1.to_i
        column_name = $2
        values << value
        columns << (column_name || nil)
      elsif part.match(/\A(TRUE|FALSE)(?:\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?\z/i)
        value = $1.upcase
        column_name = $2
        values << value
        columns << (column_name || nil)
      else
        return { error: 'parsing_error' }
      end
    end
    
    { type: :select, values: values, columns: columns }
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