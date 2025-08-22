class ExpressionEvaluator
  class ValidationError < StandardError; end
  class DivisionByZeroError < StandardError; end
  
  def validate_types(expression, row_data = {})
    case expression[:type]
    when :literal
      # Literals have their own type (including NULL/nil)
      expression[:value]
    when :column
      lookup_column(expression[:name], row_data)
    when :qualified_column
      lookup_qualified_column(expression, row_data)
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
      lookup_column(expression[:name], row_data)
    when :qualified_column
      lookup_qualified_column(expression, row_data)
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
  
  def evaluate_with_context(expression, row_context, table_context = nil)
    case expression[:type]
    when :literal
      expression[:value]
    when :column
      lookup_column_with_context(expression[:name], row_context, table_context)
    when :qualified_column
      lookup_qualified_column_with_context(expression, row_context, table_context)
    when :binary_op
      evaluate_binary_op_with_context(expression, row_context, table_context)
    when :unary_op
      evaluate_unary_op_with_context(expression, row_context, table_context)
    when :function
      evaluate_function_with_context(expression, row_context, table_context)
    else
      raise ValidationError, "Unknown expression type: #{expression[:type]}"
    end
  end
  
  def get_expression_type(expression, row_data = {})
    case expression[:type]
    when :literal
      infer_type(expression[:value])
    when :column
      value = lookup_column(expression[:name], row_data)
      infer_type(value)
    when :qualified_column
      value = lookup_qualified_column(expression, row_data)
      infer_type(value)
    when :binary_op
      get_binary_op_type(expression, row_data)
    when :unary_op
      get_unary_op_type(expression, row_data)
    when :function
      get_function_type(expression, row_data)
    else
      raise ValidationError, "Unknown expression type: #{expression[:type]}"
    end
  end
  
  private
  
  def get_binary_op_type(expression, row_data)
    case expression[:operator]
    when :plus, :minus, :star, :slash
      :integer
    when :lt, :gt, :lte, :gte, :equal, :not_equal
      :boolean
    when :and, :or
      :boolean
    else
      raise ValidationError, "Unknown operator: #{expression[:operator]}"
    end
  end
  
  def get_unary_op_type(expression, row_data)
    case expression[:operator]
    when :minus
      :integer
    when :not
      :boolean
    else
      raise ValidationError, "Unknown unary operator: #{expression[:operator]}"
    end
  end
  
  def get_function_type(expression, row_data)
    case expression[:name]
    when :abs, :mod
      :integer
    else
      raise ValidationError, "Unknown function: #{expression[:name]}"
    end
  end
  
  def validate_binary_op_types(expression, row_data)
    left = validate_types(expression[:left], row_data)
    right = validate_types(expression[:right], row_data)
    
    # NULL is valid in any expression context
    return nil if left.nil? || right.nil?
    
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
    
    # NULL is valid in any expression context
    return nil if operand.nil?
    
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
      # NULL is valid
      return nil if arg.nil?
      validate_integer(arg)
      0  # Return dummy integer type
    when :mod
      raise ValidationError, "Wrong number of arguments for MOD" unless expression[:args].length == 2
      arg1 = validate_types(expression[:args][0], row_data)
      arg2 = validate_types(expression[:args][1], row_data)
      # NULL is valid
      return nil if arg1.nil? || arg2.nil?
      validate_integer(arg1)
      validate_integer(arg2)
      0  # Return dummy integer type
    else
      raise ValidationError, "Unknown function: #{expression[:name]}"
    end
  end
  
  def evaluate_binary_op_with_context(expression, row_context, table_context)
    left = evaluate_with_context(expression[:left], row_context, table_context)
    right = evaluate_with_context(expression[:right], row_context, table_context)
    
    # Use the same logic as evaluate_binary_op
    case expression[:operator]
    when :plus, :minus, :star, :slash
      # NULL propagation for mathematical operators
      return nil if left.nil? || right.nil?
      
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
      # NULL propagation for comparison operators
      return nil if left.nil? || right.nil?
      
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
      # NULL propagation for equality operators
      return nil if left.nil? || right.nil?
      
      validate_same_type(left, right)
      case expression[:operator]
      when :equal
        left == right
      when :not_equal
        left != right
      end
    when :and
      # Special NULL handling for AND:
      # FALSE AND NULL = FALSE
      # NULL AND FALSE = FALSE
      # TRUE AND NULL = NULL
      # NULL AND TRUE = NULL
      # NULL AND NULL = NULL
      if left == false || right == false
        false
      elsif left.nil? || right.nil?
        nil
      else
        validate_booleans(left, right)
        left && right
      end
    when :or
      # Special NULL handling for OR:
      # TRUE OR NULL = TRUE
      # NULL OR TRUE = TRUE
      # FALSE OR NULL = NULL
      # NULL OR FALSE = NULL
      # NULL OR NULL = NULL
      if left == true || right == true
        true
      elsif left.nil? || right.nil?
        nil
      else
        validate_booleans(left, right)
        left || right
      end
    else
      raise ValidationError, "Unknown operator: #{expression[:operator]}"
    end
  end
  
  def evaluate_unary_op_with_context(expression, row_context, table_context)
    operand = evaluate_with_context(expression[:operand], row_context, table_context)
    
    case expression[:operator]
    when :minus
      return nil if operand.nil?
      validate_integer(operand)
      -operand
    when :not
      return nil if operand.nil?
      validate_boolean(operand)
      !operand
    else
      raise ValidationError, "Unknown unary operator: #{expression[:operator]}"
    end
  end
  
  def evaluate_function_with_context(expression, row_context, table_context)
    args = expression[:args].map { |arg| evaluate_with_context(arg, row_context, table_context) }
    
    case expression[:name]
    when :abs
      raise ValidationError if args.length != 1
      arg = args[0]
      return nil if arg.nil?
      validate_integer(arg)
      arg.abs
    when :mod
      raise ValidationError if args.length != 2
      left = args[0]
      right = args[1]
      return nil if left.nil? || right.nil?
      validate_integers(left, right)
      raise DivisionByZeroError if right == 0
      left % right
    else
      raise ValidationError, "Unknown function: #{expression[:name]}"
    end
  end
  
  def evaluate_binary_op(expression, row_data)
    left = evaluate(expression[:left], row_data)
    right = evaluate(expression[:right], row_data)
    
    case expression[:operator]
    when :plus, :minus, :star, :slash
      # NULL propagation for mathematical operators
      return nil if left.nil? || right.nil?
      
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
      # NULL propagation for comparison operators
      return nil if left.nil? || right.nil?
      
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
      # NULL propagation for equality operators
      return nil if left.nil? || right.nil?
      
      validate_same_type(left, right)
      case expression[:operator]
      when :equal
        left == right
      when :not_equal
        left != right
      end
    when :and
      # Special NULL handling for AND:
      # FALSE AND NULL = FALSE
      # NULL AND FALSE = FALSE  
      # TRUE AND NULL = NULL
      # NULL AND TRUE = NULL
      # NULL AND NULL = NULL
      if left == false || right == false
        return false
      elsif left.nil? || right.nil?
        return nil
      end
      
      validate_boolean(left)
      validate_boolean(right)
      to_boolean(left) && to_boolean(right)
    when :or
      # Special NULL handling for OR:
      # TRUE OR NULL = TRUE
      # NULL OR TRUE = TRUE
      # FALSE OR NULL = NULL
      # NULL OR FALSE = NULL
      # NULL OR NULL = NULL
      if left == true || right == true
        return true
      elsif left.nil? || right.nil?
        return nil
      end
      
      validate_boolean(left)
      validate_boolean(right)
      to_boolean(left) || to_boolean(right)
    else
      raise ValidationError, "Unknown operator: #{expression[:operator]}"
    end
  end
  
  def evaluate_unary_op(expression, row_data)
    operand = evaluate(expression[:operand], row_data)
    
    # NULL propagation for unary operators
    return nil if operand.nil?
    
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
      # NULL propagation for ABS
      return nil if args[0].nil?
      validate_integer(args[0])
      args[0].abs
    when :mod
      raise ValidationError, "Wrong number of arguments for MOD" unless expression[:args].length == 2
      args = expression[:args].map { |arg| evaluate(arg, row_data) }
      # NULL propagation for MOD
      return nil if args[0].nil? || args[1].nil?
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
  
  def lookup_column(column_name, row_data)
    if row_data.key?(column_name)
      row_data[column_name]
    else
      raise ValidationError, "Unknown column: #{column_name}"
    end
  end
  
  def lookup_column_with_context(column_name, row_context, table_context)
    # First check if it's in the row context (unqualified column)
    if row_context.key?(column_name)
      # Check for ambiguity - if column appears in multiple tables
      if table_context
        count = 0
        table_context[:aliases].each do |alias_name, _|
          if row_context.key?("#{alias_name}.#{column_name}")
            count += 1
          end
        end
        if count > 1
          raise ValidationError, "Ambiguous column: #{column_name}"
        end
      end
      row_context[column_name]
    else
      raise ValidationError, "Unknown column: #{column_name}"
    end
  end
  
  def lookup_qualified_column_with_context(expression, row_context, table_context)
    table_ref = expression[:table]
    column_name = expression[:column]
    qualified_name = "#{table_ref}.#{column_name}"
    
    if row_context.key?(qualified_name)
      row_context[qualified_name]
    else
      raise ValidationError, "Unknown column: #{qualified_name}"
    end
  end
  
  def lookup_qualified_column(expression, row_data)
    column_name = expression[:column]
    # Try case-insensitive match for column name
    matching_key = row_data.keys.find { |key| key.downcase == column_name.downcase }
    if matching_key
      row_data[matching_key]
    else
      raise ValidationError, "Unknown column: #{column_name}"
    end
  end
  
  def infer_type(value)
    if value.nil?
      nil
    elsif value.is_a?(Integer)
      :integer
    elsif value == true || value == false
      :boolean
    else
      :unknown
    end
  end
end