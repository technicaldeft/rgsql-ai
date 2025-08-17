class Parser
  def parse(sql)
    sql = sql.strip
    
    # Check for unexpected characters after semicolon
    if sql.include?(';')
      parts = sql.split(';', 2)
      if parts[1] && !parts[1].strip.empty?
        return { error: 'parsing_error' }
      end
      sql = parts[0].strip
    end
    
    # Handle SELECT statements
    if sql.upcase.start_with?('SELECT')
      parse_select(sql)
    else
      { error: 'parsing_error' }
    end
  end
  
  private
  
  def parse_select(sql)
    # Remove SELECT keyword
    rest = sql[6..-1].strip
    
    # Handle empty SELECT
    if rest.empty? || rest == ';'
      return { type: :select, values: [], columns: [] }
    end
    
    values = []
    columns = []
    
    # Parse comma-separated values
    parts = rest.split(',').map(&:strip)
    
    parts.each do |part|
      # Remove trailing semicolon if present
      part = part.chomp(';').strip
      
      # Check for AS clause
      if part =~ /^(.+)\s+AS\s+(.+)$/i
        value_part = $1.strip
        column_name = $2.strip
        
        # Validate column name (must not start with a number)
        if column_name =~ /^\d/
          return { error: 'parsing_error' }
        end
        
        value = parse_value(value_part)
        return { error: 'parsing_error' } if value.nil?
        
        values << value
        columns << column_name
      else
        value = parse_value(part)
        return { error: 'parsing_error' } if value.nil?
        
        values << value
        columns << nil
      end
    end
    
    { type: :select, values: values, columns: columns }
  end
  
  def parse_value(str)
    return nil if str.empty?
    
    # Check for boolean values
    return true if str.upcase == 'TRUE'
    return false if str.upcase == 'FALSE'
    
    # Check for integers (including negative)
    if str =~ /^-?\d+$/
      return str.to_i
    end
    
    nil
  end
end