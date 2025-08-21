require_relative 'boolean_converter'
require_relative 'parsing_utils'
require_relative 'expression_parser'
require_relative 'sql_constants'
require_relative 'select_parser'
require_relative 'value_list_parser'

class SqlParser
  include ParsingUtils
  include SqlConstants
  include SelectParser
  include ValueListParser
  
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
    
    result = parse_select_list(select_list)
    return result if is_error?(result)
    
    { type: :select, expressions: result[:expressions], columns: result[:columns] }
  end
  
  def parse_column_names(select_list)
    parse_select_expressions(select_list)
  end
  
  def parse_select_value(expression)
    parse_select_item(expression)
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
    values_clause = match[2]
    
    value_sets = parse_multiple_value_sets(values_clause)
    return value_sets if is_error?(value_sets)
    
    { type: :insert_multiple, table_name: table_name, value_sets: value_sets }
  end
  
end