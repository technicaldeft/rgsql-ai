class SqlValidator
  def initialize(evaluator)
    @evaluator = evaluator
  end
  
  def validate_select_expressions(expressions, dummy_row_data, table_name = nil)
    expressions.each do |expr_info|
      # Validate qualified column references if present
      validate_qualified_columns(expr_info[:expression], table_name) if table_name
      @evaluator.validate_types(expr_info[:expression], dummy_row_data)
    end
  end
  
  def validate_qualified_columns(expr, table_name)
    case expr[:type]
    when :qualified_column
      # Check if the table qualifier matches the current table
      if expr[:table].downcase != table_name.downcase
        raise ExpressionEvaluator::ValidationError, "Invalid table reference: #{expr[:table]}"
      end
    when :binary_op
      validate_qualified_columns(expr[:left], table_name) if expr[:left]
      validate_qualified_columns(expr[:right], table_name) if expr[:right]
    when :unary_op
      validate_qualified_columns(expr[:operand], table_name) if expr[:operand]
    when :function
      expr[:args].each { |arg| validate_qualified_columns(arg, table_name) } if expr[:args]
    end
  end
  
  def validate_where_clause(where_clause, dummy_row_data)
    @evaluator.validate_types(where_clause, dummy_row_data)
    
    # Check that WHERE clause evaluates to boolean or NULL
    where_type = @evaluator.get_expression_type(where_clause, dummy_row_data)
    where_type == :boolean || where_type.nil?
  end
  
  def validate_limit_offset_expression(expr)
    # Cannot contain column references
    return false if contains_column_reference?(expr)
    
    @evaluator.validate_types(expr, {})
    expr_type = @evaluator.get_expression_type(expr, {})
    expr_type == :integer || expr_type.nil?
  end
  
  def validate_row_types(values, columns)
    values.each_with_index do |value, idx|
      column = columns[idx]
      if column
        # NULL is allowed for any column type
        next if value.nil?
        
        case column[:type]
        when 'INTEGER'
          return false unless value.is_a?(Integer)
        when 'BOOLEAN'
          return false unless [true, false].include?(value)
        end
      end
    end
    true
  end
  
  def contains_column_reference?(expr)
    return false unless expr
    
    case expr[:type]
    when :column, :qualified_column
      true
    when :binary_op
      contains_column_reference?(expr[:left]) || contains_column_reference?(expr[:right])
    when :unary_op
      contains_column_reference?(expr[:operand])
    when :function
      expr[:args] && expr[:args].any? { |arg| contains_column_reference?(arg) }
    else
      false
    end
  end
  
  def contains_alias_in_expression?(expr, alias_mapping)
    return false unless expr
    
    case expr[:type]
    when :column
      # Check if this is an alias used in an expression (not as a simple reference)
      false  # Simple column references are checked separately
    when :binary_op
      # Check if either operand references an alias
      left_contains = expr[:left][:type] == :column && alias_mapping[expr[:left][:name]]
      right_contains = expr[:right][:type] == :column && alias_mapping[expr[:right][:name]]
      left_contains || right_contains || 
        contains_alias_in_expression?(expr[:left], alias_mapping) || 
        contains_alias_in_expression?(expr[:right], alias_mapping)
    when :unary_op
      operand_contains = expr[:operand][:type] == :column && alias_mapping[expr[:operand][:name]]
      operand_contains || contains_alias_in_expression?(expr[:operand], alias_mapping)
    when :function
      expr[:arguments].any? do |arg|
        (arg[:type] == :column && alias_mapping[arg[:name]]) ||
        contains_alias_in_expression?(arg, alias_mapping)
      end
    else
      false
    end
  end
end