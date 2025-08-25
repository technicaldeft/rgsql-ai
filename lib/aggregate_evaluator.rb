require_relative 'expression_evaluator'
require_relative 'expression_visitor'
require_relative 'aggregate_function_registry'

class AggregateEvaluator
  def initialize(evaluator)
    @evaluator = evaluator
    @registry = AggregateFunctionRegistry.instance
  end
  
  def has_aggregate_functions?(expression)
    visitor = AggregateDetectorVisitor.new
    visitor.visit(expression)
    visitor.has_aggregates?
  end
  
  def has_nested_aggregate?(expression)
    return false unless expression[:type] == :aggregate_function
    
    expression[:args]&.any? { |arg| has_aggregate_functions?(arg) } || false
  end
  
  def evaluate_aggregate(expression, group_rows)
    case expression[:type]
    when :aggregate_function
      evaluate_aggregate_function(expression, group_rows)
    when :binary_op
      left_val = evaluate_expression_with_aggregates(expression[:left], group_rows)
      right_val = evaluate_expression_with_aggregates(expression[:right], group_rows)
      apply_binary_op(expression[:operator], left_val, right_val)
    when :unary_op
      operand_val = evaluate_expression_with_aggregates(expression[:operand], group_rows)
      apply_unary_op(expression[:operator], operand_val)
    when :function
      args = expression[:args].map { |arg| evaluate_expression_with_aggregates(arg, group_rows) }
      apply_function(expression[:name], args)
    else
      # For non-aggregate expressions, evaluate on the first row
      group_rows.empty? ? nil : @evaluator.evaluate(expression, group_rows.first)
    end
  end
  
  def evaluate_aggregate_with_context(expression, group_contexts, table_context)
    case expression[:type]
    when :aggregate_function
      evaluate_aggregate_function_with_context(expression, group_contexts, table_context)
    when :binary_op
      left_val = evaluate_expression_with_aggregates_context(expression[:left], group_contexts, table_context)
      right_val = evaluate_expression_with_aggregates_context(expression[:right], group_contexts, table_context)
      apply_binary_op(expression[:operator], left_val, right_val)
    when :unary_op
      operand_val = evaluate_expression_with_aggregates_context(expression[:operand], group_contexts, table_context)
      apply_unary_op(expression[:operator], operand_val)
    when :function
      args = expression[:args].map { |arg| evaluate_expression_with_aggregates_context(arg, group_contexts, table_context) }
      apply_function(expression[:name], args)
    else
      # For non-aggregate expressions, evaluate on the first row context
      group_contexts.empty? ? nil : @evaluator.evaluate_with_context(expression, group_contexts.first, table_context)
    end
  end
  
  def validate_aggregate_types(expression, row_data = {})
    case expression[:type]
    when :aggregate_function
      validate_aggregate_function_types(expression, row_data)
    when :binary_op, :unary_op, :function
      @evaluator.validate_types(expression, row_data)
    else
      @evaluator.validate_types(expression, row_data)
    end
  end
  
  def get_aggregate_type(expression, row_data = {})
    case expression[:type]
    when :aggregate_function
      get_aggregate_function_type(expression, row_data)
    else
      @evaluator.get_expression_type(expression, row_data)
    end
  end
  
  private
  
  def evaluate_expression_with_aggregates(expression, group_rows)
    if has_aggregate_functions?(expression)
      evaluate_aggregate(expression, group_rows)
    else
      # For non-aggregate expressions in aggregate context, use first row
      group_rows.empty? ? nil : @evaluator.evaluate(expression, group_rows.first)
    end
  end
  
  def evaluate_expression_with_aggregates_context(expression, group_contexts, table_context)
    if has_aggregate_functions?(expression)
      evaluate_aggregate_with_context(expression, group_contexts, table_context)
    else
      # For non-aggregate expressions in aggregate context, use first row context
      group_contexts.empty? ? nil : @evaluator.evaluate_with_context(expression, group_contexts.first, table_context)
    end
  end
  
  def evaluate_aggregate_function(expression, group_rows)
    function_name = expression[:name]
    
    unless @registry.exists?(function_name)
      raise "Unknown aggregate function: #{function_name}"
    end
    
    case function_name
    when :count
      evaluate_count(expression[:args]&.first, group_rows)
    when :sum
      evaluate_sum(expression[:args]&.first, group_rows)
    else
      raise "Aggregate function not implemented: #{function_name}"
    end
  end
  
  def evaluate_aggregate_function_with_context(expression, group_contexts, table_context)
    function_name = expression[:name]
    
    unless @registry.exists?(function_name)
      raise "Unknown aggregate function: #{function_name}"
    end
    
    case function_name
    when :count
      evaluate_count_with_context(expression[:args]&.first, group_contexts, table_context)
    when :sum
      evaluate_sum_with_context(expression[:args]&.first, group_contexts, table_context)
    else
      raise "Aggregate function not implemented: #{function_name}"
    end
  end
  
  def evaluate_count(arg_expression, group_rows)
    return 0 if group_rows.empty?
    
    if arg_expression.nil?
      # COUNT() with no arguments counts all rows
      group_rows.length
    else
      # COUNT(expression) counts non-NULL values
      count = 0
      group_rows.each do |row|
        value = @evaluator.evaluate(arg_expression, row)
        count += 1 unless value.nil?
      end
      count
    end
  end
  
  def evaluate_count_with_context(arg_expression, group_contexts, table_context)
    return 0 if group_contexts.empty?
    
    if arg_expression.nil?
      # COUNT() with no arguments counts all rows
      group_contexts.length
    else
      # COUNT(expression) counts non-NULL values
      count = 0
      group_contexts.each do |row_context|
        value = @evaluator.evaluate_with_context(arg_expression, row_context, table_context)
        count += 1 unless value.nil?
      end
      count
    end
  end
  
  def evaluate_sum(arg_expression, group_rows)
    return nil if group_rows.empty? || arg_expression.nil?
    
    sum = nil
    group_rows.each do |row|
      value = @evaluator.evaluate(arg_expression, row)
      next if value.nil?
      
      if sum.nil?
        sum = value
      else
        sum += value
      end
    end
    sum
  end
  
  def evaluate_sum_with_context(arg_expression, group_contexts, table_context)
    return nil if group_contexts.empty? || arg_expression.nil?
    
    sum = nil
    group_contexts.each do |row_context|
      value = @evaluator.evaluate_with_context(arg_expression, row_context, table_context)
      next if value.nil?
      
      if sum.nil?
        sum = value
      else
        sum += value
      end
    end
    sum
  end
  
  def validate_aggregate_function_types(expression, row_data)
    function_name = expression[:name]
    function = @registry.get(function_name)
    
    unless function
      raise ExpressionEvaluator::ValidationError, "Unknown aggregate function: #{function_name}"
    end
    
    arg = expression[:args]&.first
    
    if function.requires_argument && arg.nil?
      raise ExpressionEvaluator::ValidationError, "#{function_name.upcase} requires an argument"
    end
    
    if arg
      @evaluator.validate_types(arg, row_data)
      
      # Check argument type if function has specific requirements
      if function.argument_type
        arg_type = @evaluator.get_expression_type(arg, row_data)
        unless arg_type == function.argument_type
          raise ExpressionEvaluator::ValidationError, 
                "#{function_name.upcase} requires #{function.argument_type} argument"
        end
      end
    end
    
    nil
  end
  
  def get_aggregate_function_type(expression, row_data)
    function = @registry.get(expression[:name])
    
    unless function
      raise ExpressionEvaluator::ValidationError, "Unknown aggregate function: #{expression[:name]}"
    end
    
    function.return_type
  end
  
  def apply_binary_op(operator, left, right)
    return nil if left.nil? || right.nil?
    
    case operator
    when :plus
      left + right
    when :minus
      left - right
    when :star
      left * right
    when :slash
      return nil if right == 0
      left / right
    when :lt
      left < right
    when :gt
      left > right
    when :lte
      left <= right
    when :gte
      left >= right
    when :equal
      left == right
    when :not_equal
      left != right
    when :and
      left && right
    when :or
      left || right
    else
      raise "Unknown operator: #{operator}"
    end
  end
  
  def apply_unary_op(operator, operand)
    return nil if operand.nil?
    
    case operator
    when :minus
      -operand
    when :not
      !operand
    else
      raise "Unknown unary operator: #{operator}"
    end
  end
  
  def apply_function(name, args)
    case name
    when :abs
      return nil if args[0].nil?
      args[0].abs
    when :mod
      return nil if args[0].nil? || args[1].nil?
      return nil if args[1] == 0
      args[0] % args[1]
    else
      raise "Unknown function: #{name}"
    end
  end
end