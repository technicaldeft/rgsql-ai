require_relative 'boolean_converter'
require_relative 'sql_constants'

class ExpressionParser
  include SqlConstants
  
  Token = Struct.new(:type, :value)
  
  def initialize
    @tokens = []
    @current = 0
  end
  
  def parse(expression)
    @tokens = tokenize(expression)
    @current = 0
    return { error: SqlConstants::ERROR_TYPES[:parsing] } if @tokens.empty?
    
    result = parse_or_expression
    
    if !at_end?
      return { error: SqlConstants::ERROR_TYPES[:parsing] }
    end
    
    result
  rescue
    { error: SqlConstants::ERROR_TYPES[:parsing] }
  end
  
  private
  
  def tokenize(expression)
    tokens = []
    i = 0
    
    while i < expression.length
      char = expression[i]
      
      case char
      when ' ', "\t", "\n", "\r"
        i += 1
      when '('
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:lparen], '(')
        i += 1
      when ')'
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:rparen], ')')
        i += 1
      when ','
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:comma], ',')
        i += 1
      when '+'
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:plus], '+')
        i += 1
      when '-'
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:minus], '-')
        i += 1
      when '*'
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:star], '*')
        i += 1
      when '/'
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:slash], '/')
        i += 1
      when '<'
        if i + 1 < expression.length && expression[i + 1] == '='
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:lte], '<=')
          i += 2
        elsif i + 1 < expression.length && expression[i + 1] == '>'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:not_equal], '<>')
          i += 2
        else
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:lt], '<')
          i += 1
        end
      when '>'
        if i + 1 < expression.length && expression[i + 1] == '='
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:gte], '>=')
          i += 2
        else
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:gt], '>')
          i += 1
        end
      when '='
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:equal], '=')
        i += 1
      when /[0-9]/
        j = i
        while j < expression.length && expression[j] =~ /[0-9]/
          j += 1
        end
        tokens << Token.new(SqlConstants::TOKEN_TYPES[:integer], expression[i...j].to_i)
        i = j
      when /[a-zA-Z]/
        j = i
        while j < expression.length && expression[j] =~ /[a-zA-Z_0-9]/
          j += 1
        end
        word = expression[i...j]
        
        case word.upcase
        when 'TRUE'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:boolean], true)
        when 'FALSE'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:boolean], false)
        when 'NULL'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:null], nil)
        when 'NOT'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:not], 'NOT')
        when 'AND'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:and], 'AND')
        when 'OR'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:or], 'OR')
        when 'ABS'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:abs], 'ABS')
        when 'MOD'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:mod], 'MOD')
        when 'AS'
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:as], 'AS')
        else
          tokens << Token.new(SqlConstants::TOKEN_TYPES[:identifier], word)
        end
        i = j
      else
        return []
      end
    end
    
    tokens
  end
  
  def parse_or_expression
    parse_left_associative_binary_op(
      :parse_and_expression,
      [[SqlConstants::TOKEN_TYPES[:or], SqlConstants::OPERATORS[:or]]]
    )
  end
  
  def parse_and_expression
    parse_left_associative_binary_op(
      :parse_comparison,
      [[SqlConstants::TOKEN_TYPES[:and], SqlConstants::OPERATORS[:and]]]
    )
  end
  
  def parse_comparison
    expr = parse_addition
    return expr if expr[:error]
    
    comparison_ops = [
      [SqlConstants::TOKEN_TYPES[:lt], SqlConstants::OPERATORS[:lt]],
      [SqlConstants::TOKEN_TYPES[:gt], SqlConstants::OPERATORS[:gt]],
      [SqlConstants::TOKEN_TYPES[:lte], SqlConstants::OPERATORS[:lte]],
      [SqlConstants::TOKEN_TYPES[:gte], SqlConstants::OPERATORS[:gte]],
      [SqlConstants::TOKEN_TYPES[:equal], SqlConstants::OPERATORS[:equal]],
      [SqlConstants::TOKEN_TYPES[:not_equal], SqlConstants::OPERATORS[:not_equal]]
    ]
    
    comparison_ops.each do |token_type, operator|
      if match(token_type)
        right = parse_addition
        return right if right[:error]
        return { type: :binary_op, operator: operator, left: expr, right: right }
      end
    end
    
    expr
  end
  
  def parse_addition
    parse_left_associative_binary_op(
      :parse_multiplication,
      [
        [SqlConstants::TOKEN_TYPES[:plus], SqlConstants::OPERATORS[:plus]],
        [SqlConstants::TOKEN_TYPES[:minus], SqlConstants::OPERATORS[:minus]]
      ]
    )
  end
  
  def parse_multiplication
    parse_left_associative_binary_op(
      :parse_unary,
      [
        [SqlConstants::TOKEN_TYPES[:star], SqlConstants::OPERATORS[:star]],
        [SqlConstants::TOKEN_TYPES[:slash], SqlConstants::OPERATORS[:slash]]
      ]
    )
  end
  
  def parse_unary
    if match(SqlConstants::TOKEN_TYPES[:not])
      expr = parse_unary
      return expr if expr[:error]
      return { type: :unary_op, operator: SqlConstants::OPERATORS[:not], operand: expr }
    elsif match(SqlConstants::TOKEN_TYPES[:minus])
      expr = parse_unary
      return expr if expr[:error]
      return { type: :unary_op, operator: SqlConstants::OPERATORS[:minus], operand: expr }
    end
    
    parse_primary
  end
  
  def parse_primary
    if match(SqlConstants::TOKEN_TYPES[:integer])
      return { type: :literal, value: previous.value }
    end
    
    if match(SqlConstants::TOKEN_TYPES[:boolean])
      return { type: :literal, value: previous.value }
    end
    
    if match(SqlConstants::TOKEN_TYPES[:null])
      return { type: :literal, value: nil }
    end
    
    if match(SqlConstants::TOKEN_TYPES[:identifier])
      name = previous.value
      
      if match(SqlConstants::TOKEN_TYPES[:lparen])
        # Parse as a function call - parse arguments generically
        args = []
        unless check(SqlConstants::TOKEN_TYPES[:rparen])
          loop do
            arg = parse_or_expression
            return arg if arg[:error]
            args << arg
            break unless match(SqlConstants::TOKEN_TYPES[:comma])
          end
        end
        return { error: SqlConstants::ERROR_TYPES[:parsing] } unless match(SqlConstants::TOKEN_TYPES[:rparen])
        
        # Return the function with the appropriate name
        case name.upcase
        when 'ABS'
          return { type: :function, name: :abs, args: args }
        when 'MOD'
          return { type: :function, name: :mod, args: args }
        else
          return { type: :function, name: name.downcase.to_sym, args: args }
        end
      else
        return { type: :column, name: name }
      end
    end
    
    if match(SqlConstants::TOKEN_TYPES[:abs]) && match(SqlConstants::TOKEN_TYPES[:lparen])
      args = []
      unless check(SqlConstants::TOKEN_TYPES[:rparen])
        loop do
          arg = parse_or_expression
          return arg if arg[:error]
          args << arg
          break unless match(SqlConstants::TOKEN_TYPES[:comma])
        end
      end
      return { error: SqlConstants::ERROR_TYPES[:parsing] } unless match(SqlConstants::TOKEN_TYPES[:rparen])
      return { type: :function, name: :abs, args: args }
    end
    
    if match(SqlConstants::TOKEN_TYPES[:mod]) && match(SqlConstants::TOKEN_TYPES[:lparen])
      args = []
      unless check(SqlConstants::TOKEN_TYPES[:rparen])
        loop do
          arg = parse_or_expression
          return arg if arg[:error]
          args << arg
          break unless match(SqlConstants::TOKEN_TYPES[:comma])
        end
      end
      return { error: SqlConstants::ERROR_TYPES[:parsing] } unless match(SqlConstants::TOKEN_TYPES[:rparen])
      return { type: :function, name: :mod, args: args }
    end
    
    if match(SqlConstants::TOKEN_TYPES[:lparen])
      expr = parse_or_expression
      return expr if expr[:error]
      return { error: SqlConstants::ERROR_TYPES[:parsing] } unless match(SqlConstants::TOKEN_TYPES[:rparen])
      return expr
    end
    
    { error: SqlConstants::ERROR_TYPES[:parsing] }
  end
  
  def match(*types)
    types.each do |type|
      if check(type)
        advance
        return true
      end
    end
    false
  end
  
  def check(type)
    return false if at_end?
    peek.type == type
  end
  
  def advance
    @current += 1 unless at_end?
    previous
  end
  
  def at_end?
    @current >= @tokens.length
  end
  
  def peek
    @tokens[@current]
  end
  
  def previous
    @tokens[@current - 1]
  end
  
  def parse_left_associative_binary_op(next_method, operators)
    expr = send(next_method)
    return expr if expr[:error]
    
    while true
      matched = false
      operators.each do |token_type, operator|
        if match(token_type)
          right = send(next_method)
          return right if right[:error]
          expr = { type: :binary_op, operator: operator, left: expr, right: right }
          matched = true
          break
        end
      end
      break unless matched
    end
    
    expr
  end
end