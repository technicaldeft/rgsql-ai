require_relative 'sql_constants'
require_relative 'parsing_utils'
require_relative 'value_list_parser'

module InsertParser
  include SqlConstants
  include ParsingUtils
  include ValueListParser
  
  def parse_insert(sql)
    match = sql.match(/\AINSERT\s+INTO\s+(#{PATTERNS[:identifier]})\s+VALUES\s*(.*)\s*;?\s*\z/im)
    return parse_error unless match
    
    table_name = match[1]
    values_clause = match[2]
    
    value_sets = parse_multiple_value_sets(values_clause)
    return value_sets if is_error?(value_sets)
    
    { type: :insert_multiple, table_name: table_name, value_sets: value_sets }
  end
end