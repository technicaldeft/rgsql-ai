require_relative 'boolean_converter'
require_relative 'parsing_utils'
require_relative 'expression_parser'
require_relative 'sql_constants'

class SqlParser
  include ParsingUtils
  include SqlConstants
  
  def parse(sql)
    sql = sql.strip
    return parse_error if sql.empty?
    
    statement_type = extract_statement_type(sql)
    return parse_error unless statement_type
    
    case statement_type
    when SqlConstants::STATEMENT_TYPES[:select]
      parse_select(sql)
    when SqlConstants::STATEMENT_TYPES[:create]
      parse_create_table(sql)
    when SqlConstants::STATEMENT_TYPES[:drop]
      parse_drop_table(sql)
    when SqlConstants::STATEMENT_TYPES[:insert]
      parse_insert(sql)
    else
      parse_error
    end
  end
  
  private
  
  def extract_statement_type(sql)
    first_word = sql.match(/\A([A-Z]+)/i)
    return nil unless first_word
    first_word[1].upcase
  end
  
  def is_reserved_keyword?(name)
    SqlConstants::RESERVED_KEYWORDS.include?(name.upcase)
  end
  
  def validate_identifier(name)
    return parse_error if is_reserved_keyword?(name)
    return parse_error unless name.match(/\A#{SqlConstants::PATTERNS[:identifier]}\z/)
    true
  end
  
  def parse_select(sql)
    # Check for SELECT with FROM clause first
    if sql.match(/\bFROM\b/i)
      match = sql.match(/\ASELECT#{SqlConstants::PATTERNS[:whitespace]}(.*?)#{SqlConstants::PATTERNS[:whitespace]}FROM#{SqlConstants::PATTERNS[:whitespace]}(#{SqlConstants::PATTERNS[:identifier]})#{SqlConstants::PATTERNS[:optional_whitespace]}#{SqlConstants::PATTERNS[:optional_semicolon]}/im)
      return parse_error unless match
      
      select_list = match[1].strip
      table_name = match[2]
      
      expressions = parse_column_names(select_list)
      return expressions if expressions.is_a?(Hash) && expressions[:error]
      
      return { type: :select_from, table_name: table_name, expressions: expressions }
    end
    
    # Original SELECT without FROM
    match = sql.match(/\ASELECT#{SqlConstants::PATTERNS[:optional_whitespace]}(.*?)#{SqlConstants::PATTERNS[:optional_semicolon]}/im)
    
    return parse_error unless match
    
    remainder = sql[match.end(0)..-1].strip
    return parse_error unless remainder.empty?
    
    select_list = match[1].strip
    
    expressions = []
    columns = []
    
    if !select_list.empty?
      parts = split_on_comma(select_list)
      
      parts.each do |part|
        parsed_value = parse_select_value(part)
        return parsed_value if parsed_value[:error]
        
        expressions << parsed_value[:expression]
        columns << parsed_value[:column]
      end
    end
    
    { type: :select, expressions: expressions, columns: columns }
  end
  
  def parse_column_names(select_list)
    return [] if select_list.empty?
    
    parts = split_on_comma(select_list)
    expressions = []
    
    parts.each do |part|
      part = part.strip
      
      # Check for AS alias
      alias_match = part.match(/(.+?)\s+AS\s+(#{SqlConstants::PATTERNS[:identifier]})\s*\z/i)
      
      if alias_match
        expr_str = alias_match[1].strip
        column_alias = alias_match[2]
      else
        expr_str = part
        column_alias = nil
      end
      
      # Parse the expression
      parser = ExpressionParser.new
      parsed_expr = parser.parse(expr_str)
      
      return parsed_expr if parsed_expr[:error]
      
      expressions << { expression: parsed_expr, alias: column_alias }
    end
    
    expressions
  end
  
  def parse_select_value(expression)
    # Check for AS alias
    alias_match = expression.match(/(.+?)\s+AS\s+(#{SqlConstants::PATTERNS[:identifier]})\s*\z/i)
    
    if alias_match
      expr_str = alias_match[1].strip
      column_alias = alias_match[2]
    else
      expr_str = expression.strip
      column_alias = nil
    end
    
    # Parse the expression
    parser = ExpressionParser.new
    parsed_expr = parser.parse(expr_str)
    
    return parsed_expr if parsed_expr[:error]
    
    { expression: parsed_expr, column: column_alias }
  end
  
  def parse_create_table(sql)
    match = sql.match(/\ACREATE\s+TABLE\s+(#{SqlConstants::PATTERNS[:identifier]})\s*\((.*?)\)\s*;?\s*\z/im)
    return parse_error unless match
    
    table_name = match[1]
    columns_str = match[2]
    
    # Check if table name is a reserved keyword
    return parse_error if is_reserved_keyword?(table_name)
    
    columns = parse_column_definitions(columns_str)
    return columns if columns.is_a?(Hash) && columns[:error]
    
    { type: :create_table, table_name: table_name, columns: columns }
  end
  
  def parse_column_definitions(columns_str)
    column_parts = split_column_list(columns_str)
    columns = []
    
    column_parts.each do |part|
      column = parse_single_column_definition(part)
      return column if column.is_a?(Hash) && column[:error]
      columns << column
    end
    
    columns
  end
  
  def parse_single_column_definition(column_def)
    column_def = column_def.strip
    match = column_def.match(/\A(#{SqlConstants::PATTERNS[:identifier]})\s+(#{SqlConstants::DATA_TYPES.join('|')})\z/i)
    return parse_error unless match
    
    column_name = match[1]
    return parse_error if is_reserved_keyword?(column_name)
    
    { name: column_name, type: match[2].upcase }
  end
  
  def split_column_list(columns_str)
    split_on_comma(columns_str)
  end
  
  def parse_drop_table(sql)
    if match = sql.match(/\ADROP\s+TABLE\s+IF\s+EXISTS\s+(#{SqlConstants::PATTERNS[:identifier]})\s*;?\s*\z/i)
      { type: :drop_table, table_name: match[1], if_exists: true }
    elsif match = sql.match(/\ADROP\s+TABLE\s+(#{SqlConstants::PATTERNS[:identifier]})\s*;?\s*\z/i)
      { type: :drop_table, table_name: match[1], if_exists: false }
    else
      parse_error
    end
  end
  
  def parse_insert(sql)
    # Handle multiple value sets: VALUES (1, 2), (3, 4)
    match = sql.match(/\AINSERT\s+INTO\s+(#{SqlConstants::PATTERNS[:identifier]})\s+VALUES\s*(.*)\s*;?\s*\z/im)
    return parse_error unless match
    
    table_name = match[1]
    values_clause = match[2].strip
    
    # Extract all value sets - handle nested parentheses properly
    groups = extract_parenthesized_groups(values_clause)
    value_sets = []
    
    groups.each do |group|
      values = parse_value_list(group)
      return values if is_error?(values)
      value_sets << values
    end
    
    return parse_error if value_sets.empty?
    
    { type: :insert_multiple, table_name: table_name, value_sets: value_sets }
  end
  
  def parse_value_list(values_str)
    return [] if values_str.strip.empty?
    
    parts = split_on_comma(values_str)
    values = []
    
    parts.each do |part|
      part = part.strip
      
      # Parse as expression
      parser = ExpressionParser.new
      parsed_expr = parser.parse(part)
      
      return parsed_expr if parsed_expr[:error]
      
      values << parsed_expr
    end
    
    values
  end
end