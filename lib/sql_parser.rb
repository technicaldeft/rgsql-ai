require_relative 'parsing_utils'
require_relative 'sql_constants'
require_relative 'select_parser'
require_relative 'table_parser'
require_relative 'insert_parser'

class SqlParser
  include ParsingUtils
  include SqlConstants
  include SelectParser
  include TableParser
  include InsertParser
  
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
  
end