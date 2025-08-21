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
  
  
  
end