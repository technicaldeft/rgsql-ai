require_relative 'expression_parser'
require_relative 'parsing_utils'

module ValueListParser
  include ParsingUtils
  
  def parse_value_list(values_str)
    return [] if values_str.strip.empty?
    
    parts = split_on_comma(values_str)
    values = []
    
    parts.each do |part|
      parsed_expr = parse_expression(part.strip)
      return parsed_expr if is_error?(parsed_expr)
      values << parsed_expr
    end
    
    values
  end
  
  def parse_multiple_value_sets(values_clause)
    groups = extract_parenthesized_groups(values_clause.strip)
    return parse_error if groups.empty?
    
    value_sets = []
    groups.each do |group|
      values = parse_value_list(group)
      return values if is_error?(values)
      value_sets << values
    end
    
    value_sets
  end
  
  private
  
  def parse_expression(expr_str)
    parser = ExpressionParser.new
    parser.parse(expr_str)
  end
end