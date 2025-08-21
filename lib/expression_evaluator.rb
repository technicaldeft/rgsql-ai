class ExpressionEvaluator
  class ValidationError < StandardError; end
  class DivisionByZeroError < StandardError; end
  
  def validate_types(expression, row_data = {})
    case expression[:type]
    when :literal
      # Literals have their own type
      expression[:value]
    when :column
      column_name = expression[:name]
      if row_data.key?(column_name)
        row_data[column_name]
      else
        raise ValidationError, "Unknown column: #{column_name}"
      end
    when :binary_op
      validate_binary_op_types(expression, row_data)
    when :unary_op
      validate_unary_op_types(expression, row_data)
    when :function
      validate_function_types(expression, row_data)
    else
      raise ValidationError, "Unknown expression type: #{expression[:type]}"
    end
  end
  
  def evaluate(expression, row_data = {})
    case expression[:type]
    when :literal
      expression[:value]
    when :column
      column_name = expression[:name]
      if row_data.key?(column_name)
        row_data[column_name]
      else
        raise ValidationError, "Unknown column: #{column_name}"
      end
    when :binary_op
      evaluate_binary_op(expression, row_data)
    when :unary_op
      evaluate_unary_op(expression, row_data)
    when :function
      evaluate_function(expression, row_data)
    else
      raise ValidationError, "Unknown expression type: #{expression[:type]}"
    end
  end
  
  private
  
  def validate_binary_op_types(expression, row_data)
    left = validate_types(expression[:left], row_data)
    right = validate_types(expression[:right], row_data)
    
    case expression[:operator]
    when :plus, :minus, :star, :slash
      validate_integers(left, right)
      0  # Return dummy integer type
    when :lt, :gt, :lte, :gte
      validate_same_type(left, right)
      true  # Return dummy boolean type
    when :equal, :not_equal
      validate_same_type(left, right)
      true  # Return dummy boolean type
    when :and, :or
      validate_boolean(left)
      validate_boolean(right)
      true  # Return dummy boolean type
    else
      raise ValidationError, "Unknown operator: #{expression[:operator]}"
    end
  end
  
  def validate_unary_op_types(expression, row_data)
    operand = validate_types(expression[:operand], row_data)
    
    case expression[:operator]
    when :minus
      validate_integer(operand)
      0  # Return dummy integer type
    when :not
      validate_boolean(operand)
      true  # Return dummy boolean type
    else
      raise ValidationError, "Unknown unary operator: #{expression[:operator]}"
    end
  end
  
  def validate_function_types(expression, row_data)
    case expression[:name]
    when :abs
      raise ValidationError, "Wrong number of arguments for ABS" unless expression[:args].length == 1
      arg = validate_types(expression[:args][0], row_data)
      validate_integer(arg)
      0  # Return dummy integer type
    when :mod
      raise ValidationError, "Wrong number of arguments for MOD" unless expression[:args].length == 2
      arg1 = validate_types(expression[:args][0], row_data)
      arg2 = validate_types(expression[:args][1], row_data)
      validate_integer(arg1)
      validate_integer(arg2)
      0  # Return dummy integer type
    else
      raise ValidationError, "Unknown function: #{expression[:name]}"
    end
  end
  
  def evaluate_binary_op(expression, row_data)
    left = evaluate(expression[:left], row_data)
    right = evaluate(expression[:right], row_data)
    
    case expression[:operator]
    when :plus, :minus, :star, :slash
      validate_integers(left, right)
      case expression[:operator]
      when :plus
        left + right
      when :minus
        left - right
      when :star
        left * right
      when :slash
        raise DivisionByZeroError, "Division by zero" if right == 0
        left / right
      end
    when :lt, :gt, :lte, :gte
      validate_same_type(left, right)
      case expression[:operator]
      when :lt
        compare_values(left, right) < 0
      when :gt
        compare_values(left, right) > 0
      when :lte
        compare_values(left, right) <= 0
      when :gte
        compare_values(left, right) >= 0
      end
    when :equal, :not_equal
      validate_same_type(left, right)
      case expression[:operator]
      when :equal
        left == right
      when :not_equal
        left != right
      end
    when :and
      validate_boolean(left)
      validate_boolean(right)
      to_boolean(left) && to_boolean(right)
    when :or
      validate_boolean(left)
      validate_boolean(right)
      to_boolean(left) || to_boolean(right)
    else
      raise ValidationError, "Unknown operator: #{expression[:operator]}"
    end
  end
  
  def evaluate_unary_op(expression, row_data)
    operand = evaluate(expression[:operand], row_data)
    
    case expression[:operator]
    when :minus
      validate_integer(operand)
      -operand
    when :not
      validate_boolean(operand)
      !to_boolean(operand)
    else
      raise ValidationError, "Unknown unary operator: #{expression[:operator]}"
    end
  end
  
  def evaluate_function(expression, row_data)
    case expression[:name]
    when :abs
      raise ValidationError, "Wrong number of arguments for ABS" unless expression[:args].length == 1
      args = expression[:args].map { |arg| evaluate(arg, row_data) }
      validate_integer(args[0])
      args[0].abs
    when :mod
      raise ValidationError, "Wrong number of arguments for MOD" unless expression[:args].length == 2
      args = expression[:args].map { |arg| evaluate(arg, row_data) }
      validate_integer(args[0])
      validate_integer(args[1])
      args[0] % args[1]
    else
      raise ValidationError, "Unknown function: #{expression[:name]}"
    end
  end
  
  def to_boolean(value)
    case value
    when true, false
      value
    when 'TRUE'
      true
    when 'FALSE'
      false
    else
      raise ValidationError, "Cannot convert to boolean: #{value}"
    end
  end
  
  def compare_values(left, right)
    if (left == true || left == false) && (right == true || right == false)
      left_val = left ? 1 : 0
      right_val = right ? 1 : 0
      left_val <=> right_val
    else
      left <=> right
    end
  end
  
  def validate_integers(*values)
    values.each do |value|
      validate_integer(value)
    end
  end
  
  def validate_integer(value)
    unless value.is_a?(Integer)
      raise ValidationError, "Type mismatch: expected integer"
    end
  end
  
  def validate_same_type(left, right)
    left_is_bool = [true, false].include?(left)
    right_is_bool = [true, false].include?(right)
    
    if left_is_bool != right_is_bool
      raise ValidationError, "Type mismatch: operands must be same type"
    end
  end
  
  def validate_boolean(value)
    unless [true, false].include?(value)
      raise ValidationError, "Type mismatch: expected boolean"
    end
  end
end