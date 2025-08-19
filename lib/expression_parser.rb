require_relative 'boolean_converter'

class ExpressionParser
  PARSING_ERROR = 'parsing_error'
  
  Token = Struct.new(:type, :value)
  
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
        tokens << Token.new(:lparen, '(')
        i += 1
      when ')'
        tokens << Token.new(:rparen, ')')
        i += 1
      when ','
        tokens << Token.new(:comma, ',')
        i += 1
      when '+'
        tokens << Token.new(:plus, '+')
        i += 1
      when '-'
        tokens << Token.new(:minus, '-')
        i += 1
      when '*'
        tokens << Token.new(:star, '*')
        i += 1
      when '/'
        tokens << Token.new(:slash, '/')
        i += 1
      when '<'
        if i + 1 < expression.length && expression[i + 1] == '='
          tokens << Token.new(:lte, '<=')
          i += 2
        elsif i + 1 < expression.length && expression[i + 1] == '>'
          tokens << Token.new(:not_equal, '<>')
          i += 2
        else
          tokens << Token.new(:lt, '<')
          i += 1
        end
      when '>'
        if i + 1 < expression.length && expression[i + 1] == '='
          tokens << Token.new(:gte, '>=')
          i += 2
        else
          tokens << Token.new(:gt, '>')
          i += 1
        end
      when '='
        tokens << Token.new(:equal, '=')
        i += 1
      when /[0-9]/
        j = i
        while j < expression.length && expression[j] =~ /[0-9]/
          j += 1
        end
        tokens << Token.new(:integer, expression[i...j].to_i)
        i = j
      when /[a-zA-Z]/
        j = i
        while j < expression.length && expression[j] =~ /[a-zA-Z_0-9]/
          j += 1
        end
        word = expression[i...j]
        
        case word.upcase
        when 'TRUE'
          tokens << Token.new(:boolean, true)
        when 'FALSE'
          tokens << Token.new(:boolean, false)
        when 'NOT'
          tokens << Token.new(:not, 'NOT')
        when 'AND'
          tokens << Token.new(:and, 'AND')
        when 'OR'
          tokens << Token.new(:or, 'OR')
        when 'ABS'
          tokens << Token.new(:abs, 'ABS')
        when 'MOD'
          tokens << Token.new(:mod, 'MOD')
        when 'AS'
          tokens << Token.new(:as, 'AS')
        else
          tokens << Token.new(:identifier, word)
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
    
    while match(:or)
      right = parse_and_expression
      return right if right[:error]
      expr = { type: :binary_op, operator: :or, left: expr, right: right }
    end
    
    expr
  end
  
  def parse_and_expression
    expr = parse_comparison
    return expr if expr[:error]
    
    while match(:and)
      right = parse_comparison
      return right if right[:error]
      expr = { type: :binary_op, operator: :and, left: expr, right: right }
    end
    
    expr
  end
  
  def parse_comparison
    expr = parse_addition
    return expr if expr[:error]
    
    if match(:lt)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: :lt, left: expr, right: right }
    elsif match(:gt)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: :gt, left: expr, right: right }
    elsif match(:lte)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: :lte, left: expr, right: right }
    elsif match(:gte)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: :gte, left: expr, right: right }
    elsif match(:equal)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: :equal, left: expr, right: right }
    elsif match(:not_equal)
      right = parse_addition
      return right if right[:error]
      return { type: :binary_op, operator: :not_equal, left: expr, right: right }
    end
    
    expr
  end
  
  def parse_addition
    expr = parse_multiplication
    return expr if expr[:error]
    
    while true
      if match(:plus)
        right = parse_multiplication
        return right if right[:error]
        expr = { type: :binary_op, operator: :plus, left: expr, right: right }
      elsif match(:minus)
        right = parse_multiplication
        return right if right[:error]
        expr = { type: :binary_op, operator: :minus, left: expr, right: right }
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
      if match(:star)
        right = parse_unary
        return right if right[:error]
        expr = { type: :binary_op, operator: :star, left: expr, right: right }
      elsif match(:slash)
        right = parse_unary
        return right if right[:error]
        expr = { type: :binary_op, operator: :slash, left: expr, right: right }
      else
        break
      end
    end
    
    expr
  end
  
  def parse_unary
    if match(:not)
      expr = parse_unary
      return expr if expr[:error]
      return { type: :unary_op, operator: :not, operand: expr }
    elsif match(:minus)
      expr = parse_unary
      return expr if expr[:error]
      return { type: :unary_op, operator: :minus, operand: expr }
    end
    
    parse_primary
  end
  
  def parse_primary
    if match(:integer)
      return { type: :literal, value: previous.value }
    end
    
    if match(:boolean)
      return { type: :literal, value: previous.value }
    end
    
    if match(:identifier)
      name = previous.value
      
      if name.upcase == 'ABS' && match(:lparen)
        arg = parse_or_expression
        return arg if arg[:error]
        return { error: PARSING_ERROR } unless match(:rparen)
        return { type: :function, name: :abs, args: [arg] }
      elsif name.upcase == 'MOD' && match(:lparen)
        arg1 = parse_or_expression
        return arg1 if arg1[:error]
        return { error: PARSING_ERROR } unless match(:comma)
        arg2 = parse_or_expression
        return arg2 if arg2[:error]
        return { error: PARSING_ERROR } unless match(:rparen)
        return { type: :function, name: :mod, args: [arg1, arg2] }
      else
        return { type: :column, name: name }
      end
    end
    
    if match(:abs) && match(:lparen)
      arg = parse_or_expression
      return arg if arg[:error]
      return { error: PARSING_ERROR } unless match(:rparen)
      return { type: :function, name: :abs, args: [arg] }
    end
    
    if match(:mod) && match(:lparen)
      arg1 = parse_or_expression
      return arg1 if arg1[:error]
      return { error: PARSING_ERROR } unless match(:comma)
      arg2 = parse_or_expression
      return arg2 if arg2[:error]
      return { error: PARSING_ERROR } unless match(:rparen)
      return { type: :function, name: :mod, args: [arg1, arg2] }
    end
    
    if match(:lparen)
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