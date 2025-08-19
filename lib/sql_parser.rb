require_relative 'boolean_converter'

class SqlParser
  PARSING_ERROR = 'parsing_error'
  IDENTIFIER_PATTERN = /[a-zA-Z_][a-zA-Z0-9_]*/
  RESERVED_KEYWORDS = %w[
    SELECT FROM CREATE TABLE DROP INSERT INTO VALUES
    INTEGER BOOLEAN AS IF EXISTS
  ].freeze
  
  def parse(sql)
    sql = sql.strip
    
    if sql.empty?
      return { error: PARSING_ERROR }
    end
    
    first_word = sql.match(/\A([A-Z]+)/i)
    return { error: PARSING_ERROR } unless first_word
    
    case first_word[1].upcase
    when 'SELECT'
      parse_select(sql)
    when 'CREATE'
      parse_create_table(sql)
    when 'DROP'
      parse_drop_table(sql)
    when 'INSERT'
      parse_insert(sql)
    else
      { error: PARSING_ERROR }
    end
  end
  
  private
  
  def is_reserved_keyword?(name)
    RESERVED_KEYWORDS.include?(name.upcase)
  end
  
  def parse_select(sql)
    # Check for SELECT with FROM clause first
    if sql.match(/\bFROM\b/i)
      match = sql.match(/\ASELECT\s+(.*?)\s+FROM\s+(#{IDENTIFIER_PATTERN})\s*;?\s*\z/im)
      return { error: PARSING_ERROR } unless match
      
      select_list = match[1].strip
      table_name = match[2]
      
      column_names = parse_column_names(select_list)
      return column_names if column_names.is_a?(Hash) && column_names[:error]
      
      return { type: :select_from, table_name: table_name, columns: column_names }
    end
    
    # Original SELECT without FROM
    match = sql.match(/\ASELECT\s*(.*?)(?:\s*;\s*\z|\z)/im)
    
    if match.nil?
      return { error: PARSING_ERROR }
    end
    
    remainder = sql[match.end(0)..-1].strip
    if !remainder.empty?
      return { error: PARSING_ERROR }
    end
    
    select_list = match[1].strip
    
    values = []
    columns = []
    
    if !select_list.empty?
      parts = split_select_list(select_list)
      
      parts.each do |part|
        parsed_value = parse_select_value(part)
        return parsed_value if parsed_value[:error]
        
        values << parsed_value[:value]
        columns << parsed_value[:column]
      end
    end
    
    { type: :select, values: values, columns: columns }
  end
  
  def parse_column_names(select_list)
    return [] if select_list.empty?
    
    parts = split_select_list(select_list)
    column_names = []
    
    parts.each do |part|
      part = part.strip
      
      # Allow column names with optional AS alias
      if match = part.match(/\A(#{IDENTIFIER_PATTERN})(?:\s+AS\s+(#{IDENTIFIER_PATTERN}))?\z/i)
        column_names << match[1]
      else
        return { error: PARSING_ERROR }
      end
    end
    
    column_names
  end
  
  def parse_select_value(expression)
    if match = expression.match(/\A(-?\d+)(?:\s+AS\s+(#{IDENTIFIER_PATTERN}))?\z/i)
      { value: match[1].to_i, column: match[2] }
    elsif match = expression.match(/\A(#{BooleanConverter::BOOLEAN_TRUE}|#{BooleanConverter::BOOLEAN_FALSE})(?:\s+AS\s+(#{IDENTIFIER_PATTERN}))?\z/i)
      { value: match[1].upcase, column: match[2] }
    else
      { error: PARSING_ERROR }
    end
  end
  
  def split_select_list(select_list)
    return [] if select_list.empty?
    
    parts = []
    current = ""
    depth = 0
    
    select_list.chars.each do |char|
      if char == ',' && depth == 0
        parts << current.strip
        current = ""
      else
        current += char
        depth += 1 if char == '('
        depth -= 1 if char == ')'
      end
    end
    
    parts << current.strip unless current.strip.empty?
    parts
  end
  
  def parse_create_table(sql)
    match = sql.match(/\ACREATE\s+TABLE\s+(#{IDENTIFIER_PATTERN})\s*\((.*?)\)\s*;?\s*\z/im)
    
    return { error: PARSING_ERROR } unless match
    
    table_name = match[1]
    columns_str = match[2]
    
    # Check if table name is a reserved keyword
    return { error: PARSING_ERROR } if is_reserved_keyword?(table_name)
    
    columns = parse_column_definitions(columns_str)
    return columns if columns.is_a?(Hash) && columns[:error]
    
    { type: :create_table, table_name: table_name, columns: columns }
  end
  
  def parse_column_definitions(columns_str)
    column_parts = split_column_list(columns_str)
    columns = []
    
    column_parts.each do |part|
      part = part.strip
      match = part.match(/\A(#{IDENTIFIER_PATTERN})\s+(INTEGER|BOOLEAN)\z/i)
      
      return { error: PARSING_ERROR } unless match
      
      column_name = match[1]
      # Check if column name is a reserved keyword
      return { error: PARSING_ERROR } if is_reserved_keyword?(column_name)
      
      columns << { name: column_name, type: match[2].upcase }
    end
    
    columns
  end
  
  def split_column_list(columns_str)
    split_select_list(columns_str)
  end
  
  def parse_drop_table(sql)
    if match = sql.match(/\ADROP\s+TABLE\s+IF\s+EXISTS\s+(#{IDENTIFIER_PATTERN})\s*;?\s*\z/i)
      { type: :drop_table, table_name: match[1], if_exists: true }
    elsif match = sql.match(/\ADROP\s+TABLE\s+(#{IDENTIFIER_PATTERN})\s*;?\s*\z/i)
      { type: :drop_table, table_name: match[1], if_exists: false }
    else
      { error: PARSING_ERROR }
    end
  end
  
  def parse_insert(sql)
    # Handle multiple value sets: VALUES (1, 2), (3, 4)
    match = sql.match(/\AINSERT\s+INTO\s+(#{IDENTIFIER_PATTERN})\s+VALUES\s*(.*)\s*;?\s*\z/im)
    
    return { error: PARSING_ERROR } unless match
    
    table_name = match[1]
    values_clause = match[2].strip
    
    # Extract all value sets - handle nested parentheses properly
    value_sets = []
    current_set = ""
    depth = 0
    in_set = false
    
    values_clause.chars.each do |char|
      if char == '(' && depth == 0
        in_set = true
        depth = 1
        current_set = ""
      elsif char == '(' && in_set
        depth += 1
        current_set += char
      elsif char == ')' && depth == 1
        values = parse_value_list(current_set)
        return values if values.is_a?(Hash) && values[:error]
        value_sets << values
        in_set = false
        depth = 0
      elsif char == ')' && in_set
        depth -= 1
        current_set += char
      elsif in_set
        current_set += char
      end
    end
    
    return { error: PARSING_ERROR } if value_sets.empty?
    
    { type: :insert_multiple, table_name: table_name, value_sets: value_sets }
  end
  
  def parse_value_list(values_str)
    return [] if values_str.strip.empty?
    
    parts = split_select_list(values_str)
    values = []
    
    parts.each do |part|
      part = part.strip
      
      if part.match(/\A-?\d+\z/)
        values << part.to_i
      elsif part.match(/\A(TRUE|FALSE)\z/i)
        values << part.upcase
      else
        return { error: PARSING_ERROR }
      end
    end
    
    values
  end
end