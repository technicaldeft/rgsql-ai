require_relative 'sql_constants'
require_relative 'expression_parser'
require_relative 'parsing_utils'

module SelectParser
  include SqlConstants
  include ParsingUtils
  
  def parse_select_list(select_list)
    return { expressions: [], columns: [] } if select_list.empty?
    
    parts = split_on_comma(select_list)
    expressions = []
    columns = []
    
    parts.each do |part|
      parsed_value = parse_select_item(part)
      return parsed_value if is_error?(parsed_value)
      
      expressions << parsed_value[:expression]
      columns << parsed_value[:column]
    end
    
    { expressions: expressions, columns: columns }
  end
  
  def parse_select_item(expression)
    expr_str, column_alias = extract_alias(expression)
    
    parser = ExpressionParser.new
    parsed_expr = parser.parse(expr_str)
    
    return parsed_expr if is_error?(parsed_expr)
    
    { expression: parsed_expr, column: column_alias }
  end
  
  def parse_select_expressions(select_list)
    return [] if select_list.empty?
    
    parts = split_on_comma(select_list)
    expressions = []
    
    parts.each do |part|
      expr_str, column_alias = extract_alias(part.strip)
      
      parser = ExpressionParser.new
      parsed_expr = parser.parse(expr_str)
      
      return parsed_expr if is_error?(parsed_expr)
      
      expressions << { expression: parsed_expr, alias: column_alias }
    end
    
    expressions
  end
  
  private
  
  def extract_alias(expression)
    alias_match = expression.match(/(.+?)\s+AS\s+(#{PATTERNS[:identifier]})\s*\z/i)
    
    if alias_match
      [alias_match[1].strip, alias_match[2]]
    else
      [expression.strip, nil]
    end
  end
end