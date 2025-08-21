require_relative 'sql_constants'
require_relative 'parsing_utils'

module TableParser
  include SqlConstants
  include ParsingUtils
  
  def parse_create_table(sql)
    match = sql.match(/\ACREATE\s+TABLE\s+(#{PATTERNS[:identifier]})\s*\((.*?)\)\s*;?\s*\z/im)
    return parse_error unless match
    
    table_name = match[1]
    columns_str = match[2]
    
    return parse_error if reserved_keyword?(table_name)
    
    columns = parse_column_definitions(columns_str)
    return columns if is_error?(columns)
    
    { type: :create_table, table_name: table_name, columns: columns }
  end
  
  def parse_drop_table(sql)
    if match = sql.match(/\ADROP\s+TABLE\s+IF\s+EXISTS\s+(#{PATTERNS[:identifier]})\s*;?\s*\z/i)
      { type: :drop_table, table_name: match[1], if_exists: true }
    elsif match = sql.match(/\ADROP\s+TABLE\s+(#{PATTERNS[:identifier]})\s*;?\s*\z/i)
      { type: :drop_table, table_name: match[1], if_exists: false }
    else
      parse_error
    end
  end
  
  private
  
  def parse_column_definitions(columns_str)
    column_parts = split_on_comma(columns_str)
    columns = []
    
    column_parts.each do |part|
      column = parse_single_column_definition(part.strip)
      return column if is_error?(column)
      columns << column
    end
    
    columns
  end
  
  def parse_single_column_definition(column_def)
    match = column_def.match(/\A(#{PATTERNS[:identifier]})\s+(#{DATA_TYPES.join('|')})\z/i)
    return parse_error unless match
    
    column_name = match[1]
    return parse_error if reserved_keyword?(column_name)
    
    { name: column_name, type: match[2].upcase }
  end
  
  def reserved_keyword?(name)
    RESERVED_KEYWORDS.include?(name.upcase)
  end
end