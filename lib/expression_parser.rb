require_relative 'boolean_converter'

class ExpressionParser
  PARSING_ERROR = 'parsing_error'
  
  Token = Struct.new(:type, :value)
  
  # Token types
  TOKEN_LPAREN = :lparen
  TOKEN_RPAREN = :rparen
  TOKEN_COMMA = :comma
  TOKEN_PLUS = :plus
  TOKEN_MINUS = :minus
  TOKEN_STAR = :star
  TOKEN_SLASH = :slash
  TOKEN_LT = :lt
  TOKEN_LTE = :lte
  TOKEN_GT = :gt
  TOKEN_GTE = :gte
  TOKEN_EQUAL = :equal
  TOKEN_NOT_EQUAL = :not_equal
  TOKEN_INTEGER = :integer
  TOKEN_BOOLEAN = :boolean
  TOKEN_IDENTIFIER = :identifier
  TOKEN_NOT = :not
  TOKEN_AND = :and
  TOKEN_OR = :or
  TOKEN_ABS = :abs
  TOKEN_MOD = :mod
  TOKEN_AS = :as
  
  # Operator symbols for AST
  OP_PLUS = :plus
  OP_MINUS = :minus
  OP_STAR = :star
  OP_SLASH = :slash
  OP_LT = :lt
  OP_GT = :gt
  OP_LTE = :lte
  OP_GTE = :gte
  OP_EQUAL = :equal
  OP_NOT_EQUAL = :not_equal
  OP_AND = :and
  OP_OR = :or
  OP_NOT = :not
  
  def initialize
    @tokens = []
    @current = 0
  end
  
  def parse(expression)
    @tokens = tokenize(expression)
    @current = 0
    return { error: PARSING_ERROR } if @tokens.empty?
    
    result = parse_or_expression
    
    if !at_end?
      return { error: PARSING_ERROR }
    end
    
    result
  rescue
    { error: PARSING_ERROR }
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
        tokens << Token.new(TOKEN_LPAREN, '(')
        i += 1
      when ')'
        tokens << Token.new(TOKEN_RPAREN, ')')
        i += 1
      when ','
        tokens << Token.new(TOKEN_COMMA, ',')
        i += 1
      when '+'
        tokens << Token.new(TOKEN_PLUS, '+')
        i += 1
      when '-'
        tokens << Token.new(TOKEN_MINUS, '-')
        i += 1
      when '*'
        tokens << Token.new(TOKEN_STAR, '*')
        i += 1
      when '/'
        tokens << Token.new(TOKEN_SLASH, '/')
        i += 1
      when '<'
        if i + 1 < expression.length && expression[i + 1] == '='
          tokens << Token.new(TOKEN_LTE, '<=')
          i += 2
        elsif i + 1 < expression.length && expression[i + 1] == '>'
          tokens << Token.new(TOKEN_NOT_EQUAL, '<>')
          i += 2
        else
          tokens << Token.new(TOKEN_LT, '<')
          i += 1
        end
      when '>'
        if i + 1 < expression.length && expression[i + 1] == '='
          tokens << Token.new(TOKEN_GTE, '>=')
          i += 2
        else
          tokens << Token.new(TOKEN_GT, '>')
          i += 1
        end
      when '='
        tokens << Token.new(TOKEN_EQUAL, '=')
        i += 1
      when /[0-9]/
        j = i
        while j < expression.length && expression[j] =~ /[0-9]/
          j += 1
        end
        tokens << Token.new(TOKEN_INTEGER, expression[i...j].to_i)
        i = j
      when /[a-zA-Z]/
        j = i
        while j < expression.length && expression[j] =~ /[a-zA-Z_0-9]/
          j += 1
        end
        word = expression[i...j]
        
        case word.upcase
        when 'TRUE'
          tokens << Token.new(TOKEN_BOOLEAN, true)
        when 'FALSE'
          tokens << Token.new(TOKEN_BOOLEAN, false)
        when 'NOT'
          tokens << Token.new(TOKEN_NOT, 'NOT')
        when 'AND'
          tokens << Token.new(TOKEN_AND, 'AND')
        when 'OR'
          tokens << Token.new(TOKEN_OR, 'OR')
        when 'ABS'
          tokens << Token.new(TOKEN_ABS, 'ABS')
        when 'MOD'
          tokens << Token.new(TOKEN_MOD, 'MOD')
        when 'AS'
          tokens << Token.new(TOKEN_AS, 'AS')
        else
          tokens << Token.new(TOKEN_IDENTIFIER, word)
        end
        i = j
      else
        return []
      end
    end
    
    tokens
  end
  
  def parse_or_expression
    expr = parse_and_expression
    return expr if expr[:error]
    
    while match(TOKEN_OR)
      right = parse_and_expression
      return right if right[:error]
      expr = { type: :binary_op, operator: OP_OR, left: expr, right: right }
    end
    
    expr
  end
  
  def parse_and_expression
    expr = parse_comparison
    return expr if expr[:error]
    
    while match(TOKEN_AND)
      right = parse_comparison
      return right if right[:error]
      expr = { type: :binary_op, operator: OP_AND, left: expr, right: right }
    end
    
    expr
  end
  
  def parse_comparison
    expr = parse_addition
    return expr if expr[:error]
    
    if match(TOKEN_LT)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: OP_LT, left: expr, right: right }
    elsif match(TOKEN_GT)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: OP_GT, left: expr, right: right }
    elsif match(TOKEN_LTE)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: OP_LTE, left: expr, right: right }
    elsif match(TOKEN_GTE)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: OP_GTE, left: expr, right: right }
    elsif match(TOKEN_EQUAL)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: OP_EQUAL, left: expr, right: right }
    elsif match(TOKEN_NOT_EQUAL)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: OP_NOT_EQUAL, left: expr, right: right }
    end
    
    expr
  end
  
  def parse_addition
    expr = parse_multiplication
    return expr if expr[:error]
    
    while true
      if match(TOKEN_PLUS)
        right = parse_multiplication
        return right if right[:error]
        expr = { type: :binary_op, operator: OP_PLUS, left: expr, right: right }
      elsif match(TOKEN_MINUS)
        right = parse_multiplication
        return right if right[:error]
        expr = { type: :binary_op, operator: OP_MINUS, left: expr, right: right }
      else
        break
      end
    end
    
    expr
  end
  
  def parse_multiplication
    expr = parse_unary
    return expr if expr[:error]
    
    while true
      if match(TOKEN_STAR)
        right = parse_unary
        return right if right[:error]
        expr = { type: :binary_op, operator: OP_STAR, left: expr, right: right }
      elsif match(TOKEN_SLASH)
        right = parse_unary
        return right if right[:error]
        expr = { type: :binary_op, operator: OP_SLASH, left: expr, right: right }
      else
        break
      end
    end
    
    expr
  end
  
  def parse_unary
    if match(TOKEN_NOT)
      expr = parse_unary
      return expr if expr[:error]
      return { type: :unary_op, operator: OP_NOT, operand: expr }
    elsif match(TOKEN_MINUS)
      expr = parse_unary
      return expr if expr[:error]
      return { type: :unary_op, operator: OP_MINUS, operand: expr }
    end
    
    parse_primary
  end
  
  def parse_primary
    if match(TOKEN_INTEGER)
      return { type: :literal, value: previous.value }
    end
    
    if match(TOKEN_BOOLEAN)
      return { type: :literal, value: previous.value }
    end
    
    if match(TOKEN_IDENTIFIER)
      name = previous.value
      
      if match(TOKEN_LPAREN)
        # Parse as a function call - parse arguments generically
        args = []
        unless check(TOKEN_RPAREN)
          loop do
            arg = parse_or_expression
            return arg if arg[:error]
            args << arg
            break unless match(TOKEN_COMMA)
          end
        end
        return { error: PARSING_ERROR } unless match(TOKEN_RPAREN)
        
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
    
    if match(TOKEN_ABS) && match(TOKEN_LPAREN)
      args = []
      unless check(TOKEN_RPAREN)
        loop do
          arg = parse_or_expression
          return arg if arg[:error]
          args << arg
          break unless match(TOKEN_COMMA)
        end
      end
      return { error: PARSING_ERROR } unless match(TOKEN_RPAREN)
      return { type: :function, name: :abs, args: args }
    end
    
    if match(TOKEN_MOD) && match(TOKEN_LPAREN)
      args = []
      unless check(TOKEN_RPAREN)
        loop do
          arg = parse_or_expression
          return arg if arg[:error]
          args << arg
          break unless match(TOKEN_COMMA)
        end
      end
      return { error: PARSING_ERROR } unless match(TOKEN_RPAREN)
      return { type: :function, name: :mod, args: args }
    end
    
    if match(TOKEN_LPAREN)
      expr = parse_or_expression
      return expr if expr[:error]
      return { error: PARSING_ERROR } unless match(:rparen)
      return expr
    end
    
    { error: PARSING_ERROR }
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
end