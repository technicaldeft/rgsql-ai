require_relative 'sql_constants'
require_relative 'expression_parser'
require_relative 'parsing_utils'

module SelectParser
  include SqlConstants
  include ParsingUtils
  
  def parse_select(sql)
    if sql.match(/\bFROM\b/i)
      parse_select_from(sql)
    else
      parse_simple_select(sql)
    end
  end
  
  private
  
  def parse_select_from(sql)
    # First extract the SELECT expression list and FROM table
    # The table name may be followed by optional alias
    select_match = sql.match(/\ASELECT#{PATTERNS[:whitespace]}(.*?)#{PATTERNS[:whitespace]}FROM#{PATTERNS[:whitespace]}(#{PATTERNS[:identifier]})/im)
    return parse_error unless select_match
    
    select_list = select_match[1].strip
    table_name = select_match[2]
    
    # Parse the rest of the statement starting after the table name
    remainder = sql[select_match.end(0)..-1].strip
    
    # Check for table alias (but not if it's a keyword like INNER, LEFT, etc.)
    table_alias = nil
    alias_match = remainder.match(/\A(#{PATTERNS[:identifier]})(?=#{PATTERNS[:whitespace]}|;|\z)/im)
    if alias_match && !alias_match[1].match(/\A(INNER|LEFT|RIGHT|FULL|JOIN|WHERE|ORDER|LIMIT|OFFSET)\z/i)
      table_alias = alias_match[1]
      remainder = remainder[alias_match.end(0)..-1].strip
    end
    
    # Parse the expression list
    expressions = parse_expression_list(select_list)
    return expressions if is_error?(expressions)
    
    # Start building the result
    result = { type: :select_from, table_name: table_name, expressions: expressions }
    result[:table_alias] = table_alias if table_alias
    
    # Parse JOIN clauses if present
    join_result = parse_join_clauses(remainder)
    return join_result if is_error?(join_result)
    if join_result[:joins] && !join_result[:joins].empty?
      result[:joins] = join_result[:joins]
      remainder = join_result[:remainder]
    end
    
    # Parse WHERE clause if present
    where_result = parse_where_clause(remainder)
    return where_result if is_error?(where_result)
    if where_result[:where]
      result[:where] = where_result[:where]
      remainder = where_result[:remainder]
    end
    
    # Parse ORDER BY clause if present  
    order_result = parse_order_by_clause(remainder)
    return order_result if is_error?(order_result)
    if order_result[:order_by]
      result[:order_by] = order_result[:order_by]
      remainder = order_result[:remainder]
    end
    
    # Parse LIMIT/OFFSET clause if present
    limit_result = parse_limit_clause(remainder)
    return limit_result if is_error?(limit_result)
    if limit_result[:limit] || limit_result[:offset]
      result[:limit] = limit_result[:limit] if limit_result[:limit]
      result[:offset] = limit_result[:offset] if limit_result[:offset]
      remainder = limit_result[:remainder]
    end
    
    # Check for any remaining content (should only be whitespace and optional semicolon)
    remainder = remainder.strip
    unless remainder.match(/\A#{PATTERNS[:optional_semicolon]}\z/)
      return parse_error
    end
    
    result
  end
  
  def parse_simple_select(sql)
    match = sql.match(/\ASELECT#{PATTERNS[:optional_whitespace]}(.*?)#{PATTERNS[:optional_semicolon]}/im)
    return parse_error unless match
    
    remainder = sql[match.end(0)..-1].strip
    return parse_error unless remainder.empty?
    
    select_list = match[1].strip
    
    parsed_items = parse_expression_list(select_list)
    return parsed_items if is_error?(parsed_items)
    
    expressions = parsed_items.map { |item| item[:expression] }
    columns = parsed_items.map { |item| item[:alias] }
    
    { type: :select, expressions: expressions, columns: columns }
  end
  
  def parse_expression_list(select_list)
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
  
  def parse_where_clause(sql)
    sql = sql.strip
    return { where: nil, remainder: sql } unless sql.match(/\AWHERE\b/i)
    
    # Match WHERE keyword and capture the rest
    match = sql.match(/\AWHERE#{PATTERNS[:whitespace]}(.*)/im)
    return parse_error unless match
    
    remainder = match[1]
    
    # Parse the WHERE expression - it ends at ORDER, LIMIT, OFFSET, or semicolon/end
    expr_match = remainder.match(/\A(.*?)(?:#{PATTERNS[:whitespace]}(?:ORDER|LIMIT|OFFSET)\b|\s*;|\s*\z)/im)
    
    if expr_match
      where_expr_str = expr_match[1].strip
      
      # Check for empty WHERE expression
      return parse_error if where_expr_str.empty?
      
      parser = ExpressionParser.new
      where_expr = parser.parse(where_expr_str)
      return where_expr if is_error?(where_expr)
      
      # Calculate the remainder after the WHERE expression
      remainder_start = expr_match.end(1)
      new_remainder = remainder[remainder_start..-1]
      
      { where: where_expr, remainder: new_remainder }
    else
      # WHERE clause takes everything remaining
      where_expr_str = remainder.strip
      
      # Check for empty WHERE expression
      return parse_error if where_expr_str.empty?
      
      parser = ExpressionParser.new
      where_expr = parser.parse(where_expr_str)
      return where_expr if is_error?(where_expr)
      
      { where: where_expr, remainder: '' }
    end
  end
  
  def parse_order_by_clause(sql)
    sql = sql.strip
    
    # Check if ORDER BY is present
    order_match = sql.match(/\AORDER#{PATTERNS[:whitespace]}BY\b/i)
    return { order_by: nil, remainder: sql } unless order_match
    
    # Get everything after ORDER BY
    remainder = sql[order_match.end(0)..-1].strip
    
    # Parse the ORDER BY expression and optional direction
    # It ends at LIMIT, OFFSET, semicolon, or end of string
    expr_match = remainder.match(/\A(.*?)(?:#{PATTERNS[:whitespace]}(?:LIMIT|OFFSET)\b|\s*;|\s*\z)/im)
    
    if expr_match
      order_clause = expr_match[1].strip
      
      # Check for empty ORDER BY expression
      return parse_error if order_clause.empty?
      
      # Parse direction (ASC/DESC) if present
      dir_match = order_clause.match(/\A(.+?)#{PATTERNS[:whitespace]}(ASC|DESC)\s*\z/i)
      
      if dir_match
        order_expr_str = dir_match[1].strip
        direction = dir_match[2].upcase
        
        # Check for empty expression before direction
        return parse_error if order_expr_str.empty?
        
        # Check for invalid direction keywords
        if order_expr_str.match(/\b(ASC|DESC|CLOCKWISE|COUNTERCLOCKWISE)\s*\z/i)
          return parse_error
        end
      else
        # Check for ASC/DESC at the beginning (no expression)
        if order_clause.match(/\A(ASC|DESC)\s*\z/i)
          return parse_error
        end
        
        # Check for invalid keywords at the end
        if order_clause.match(/\b(CLOCKWISE|COUNTERCLOCKWISE)\s*\z/i)
          return parse_error
        end
        
        order_expr_str = order_clause
        direction = 'ASC' # Default direction
      end
      
      # Parse the order expression
      parser = ExpressionParser.new
      order_expr = parser.parse(order_expr_str)
      return order_expr if is_error?(order_expr)
      
      # Calculate the remainder
      remainder_start = expr_match.end(1)
      new_remainder = remainder[remainder_start..-1]
      
      { order_by: { expression: order_expr, direction: direction }, remainder: new_remainder }
    else
      { order_by: nil, remainder: sql }
    end
  end
  
  def parse_join_clauses(sql)
    joins = []
    remainder = sql
    
    while remainder.match(/\A(INNER|LEFT\s+OUTER|RIGHT\s+OUTER|FULL\s+OUTER)#{PATTERNS[:whitespace]}JOIN\b/i)
      # Match the JOIN type
      join_match = remainder.match(/\A(INNER|LEFT\s+OUTER|RIGHT\s+OUTER|FULL\s+OUTER)#{PATTERNS[:whitespace]}JOIN#{PATTERNS[:whitespace]}(#{PATTERNS[:identifier]})/im)
      
      unless join_match
        # Try to match JOIN without type specification, should fail
        if remainder.match(/\AJOIN\b/i)
          return parse_error
        end
        break
      end
      
      join_type = join_match[1].gsub(/\s+/, '_').upcase
      joined_table = join_match[2]
      
      # Move past the JOIN clause
      remainder = remainder[join_match.end(0)..-1].strip
      
      # Check for table alias (but not if it's ON or another keyword)
      joined_alias = nil
      alias_match = remainder.match(/\A(#{PATTERNS[:identifier]})(?=#{PATTERNS[:whitespace]}|;|\z)/im)
      if alias_match && !alias_match[1].match(/\A(ON|INNER|LEFT|RIGHT|FULL|JOIN|WHERE|ORDER|LIMIT|OFFSET)\z/i)
        joined_alias = alias_match[1]
        remainder = remainder[alias_match.end(0)..-1].strip
      end
      
      # Check for ON clause (required for INNER JOIN)
      if remainder.match(/\AON\b/i)
        on_match = remainder.match(/\AON#{PATTERNS[:whitespace]}(.*)/im)
        return parse_error unless on_match
        
        on_remainder = on_match[1]
        
        # Parse ON expression - ends at next JOIN, WHERE, ORDER, LIMIT, OFFSET, semicolon, or end
        on_expr_match = on_remainder.match(/\A(.*?)(?:#{PATTERNS[:whitespace]}(?:INNER|LEFT|RIGHT|FULL|JOIN|WHERE|ORDER|LIMIT|OFFSET)\b|\s*;|\s*\z)/im)
        
        if on_expr_match
          on_expr_str = on_expr_match[1].strip
          
          # Check for empty ON expression
          return parse_error if on_expr_str.empty?
          
          parser = ExpressionParser.new
          on_expr = parser.parse(on_expr_str)
          return on_expr if is_error?(on_expr)
          
          join_info = {
            type: join_type,
            table: joined_table,
            on: on_expr
          }
          join_info[:alias] = joined_alias if joined_alias
          
          joins << join_info
          
          # Update remainder
          remainder_start = on_expr_match.end(1)
          remainder = on_remainder[remainder_start..-1].strip
        else
          # ON clause takes everything remaining
          on_expr_str = on_remainder.strip
          
          # Check for empty ON expression
          return parse_error if on_expr_str.empty?
          
          parser = ExpressionParser.new
          on_expr = parser.parse(on_expr_str)
          return on_expr if is_error?(on_expr)
          
          join_info = {
            type: join_type,
            table: joined_table,
            on: on_expr
          }
          join_info[:alias] = joined_alias if joined_alias
          
          joins << join_info
          remainder = ''
        end
      else
        # Missing ON clause for INNER JOIN
        return parse_error
      end
    end
    
    { joins: joins, remainder: remainder }
  end
  
  def parse_limit_clause(sql)
    sql = sql.strip
    
    # Check for LIMIT
    limit_match = sql.match(/\ALIMIT#{PATTERNS[:whitespace]}(.*)/im)
    
    # Check for OFFSET without LIMIT
    offset_only_match = sql.match(/\AOFFSET#{PATTERNS[:whitespace]}(.*)/im)
    
    if limit_match
      remainder = limit_match[1]
      
      # Parse LIMIT expression - ends at OFFSET, semicolon, or end
      limit_expr_match = remainder.match(/\A(.*?)(?:#{PATTERNS[:whitespace]}OFFSET\b|\s*;|\s*\z)/im)
      
      if limit_expr_match
        limit_expr_str = limit_expr_match[1].strip
        
        # Check for empty LIMIT expression
        return parse_error if limit_expr_str.empty?
        
        parser = ExpressionParser.new
        limit_expr = parser.parse(limit_expr_str)
        return limit_expr if is_error?(limit_expr)
        
        result = { limit: limit_expr }
        
        # Check for OFFSET after LIMIT
        remainder_after_limit = remainder[limit_expr_match.end(1)..-1].strip
        
        offset_match = remainder_after_limit.match(/\AOFFSET#{PATTERNS[:whitespace]}(.*)/im)
        if offset_match
          offset_remainder = offset_match[1]
          
          # Parse OFFSET expression - ends at semicolon or end
          offset_expr_match = offset_remainder.match(/\A(.*?)(?:\s*;|\s*\z)/im)
          
          if offset_expr_match
            offset_expr_str = offset_expr_match[1].strip
            
            # Check for empty OFFSET expression
            return parse_error if offset_expr_str.empty?
            
            parser = ExpressionParser.new
            offset_expr = parser.parse(offset_expr_str)
            return offset_expr if is_error?(offset_expr)
            
            result[:offset] = offset_expr
            result[:remainder] = offset_remainder[offset_expr_match.end(1)..-1]
          else
            result[:remainder] = remainder_after_limit
          end
        else
          result[:remainder] = remainder_after_limit
        end
        
        result
      else
        { limit: nil, offset: nil, remainder: sql }
      end
    elsif offset_only_match
      # Handle OFFSET without LIMIT
      remainder = offset_only_match[1]
      
      # Parse OFFSET expression - ends at semicolon or end
      offset_expr_match = remainder.match(/\A(.*?)(?:\s*;|\s*\z)/im)
      
      if offset_expr_match
        offset_expr_str = offset_expr_match[1].strip
        
        # Check for empty OFFSET expression
        return parse_error if offset_expr_str.empty?
        
        parser = ExpressionParser.new
        offset_expr = parser.parse(offset_expr_str)
        return offset_expr if is_error?(offset_expr)
        
        { limit: nil, offset: offset_expr, remainder: remainder[offset_expr_match.end(1)..-1] }
      else
        { limit: nil, offset: nil, remainder: sql }
      end
    else
      { limit: nil, offset: nil, remainder: sql }
    end
  end
end