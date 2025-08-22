class SqlValidator
  def initialize(evaluator)
    @evaluator = evaluator
  end
  
  def validate_expression_with_context(expression, table_context)
    # Check that all referenced columns exist in the table context
    validate_columns_in_context(expression, table_context)
    nil  # Return nil for success (no error)
  rescue ExpressionEvaluator::ValidationError => e
    { error: 'validation_error' }
  end
  
  def validate_columns_in_context(expr, table_context)
    case expr[:type]
    when :column
      # Check unqualified column exists in at least one table
      column_name = expr[:name]
      found = false
      count = 0
      
      table_context[:aliases].each do |alias_name, table_name|
        table_info = table_context[:tables][table_name]
        if table_info[:columns].any? { |col| col[:name] == column_name }
          found = true
          count += 1
        end
      end
      
      raise ExpressionEvaluator::ValidationError, "Unknown column: #{column_name}" unless found
      raise ExpressionEvaluator::ValidationError, "Ambiguous column: #{column_name}" if count > 1
      
    when :qualified_column
      # Check qualified column exists
      table_ref = expr[:table]
      column_name = expr[:column]
      
      # Find the actual table name from the alias
      actual_table = table_context[:aliases][table_ref]
      raise ExpressionEvaluator::ValidationError, "Unknown table: #{table_ref}" unless actual_table
      
      table_info = table_context[:tables][actual_table]
      unless table_info[:columns].any? { |col| col[:name] == column_name }
        raise ExpressionEvaluator::ValidationError, "Unknown column: #{table_ref}.#{column_name}"
      end
      
    when :binary_op
      validate_columns_in_context(expr[:left], table_context) if expr[:left]
      validate_columns_in_context(expr[:right], table_context) if expr[:right]
      
      # Validate operator types
      if expr[:operator] == :equal || expr[:operator] == :not_equal
        # Check if types are comparable
        left_type = get_expression_type_with_context(expr[:left], table_context)
        right_type = get_expression_type_with_context(expr[:right], table_context)
        
        # Can't compare integers with booleans
        if (left_type == :integer && right_type == :boolean) ||
           (left_type == :boolean && right_type == :integer)
          raise ExpressionEvaluator::ValidationError, "Type mismatch in comparison"
        end
      end
      
    when :unary_op
      validate_columns_in_context(expr[:operand], table_context) if expr[:operand]
      
    when :function
      expr[:args].each { |arg| validate_columns_in_context(arg, table_context) } if expr[:args]
    end
  end
  
  def get_expression_type_with_context(expr, table_context)
    case expr[:type]
    when :literal
      if expr[:value].nil?
        nil
      elsif expr[:value] == true || expr[:value] == false
        :boolean
      else
        :integer
      end
    when :column
      column_name = expr[:name]
      table_context[:aliases].each do |alias_name, table_name|
        table_info = table_context[:tables][table_name]
        col_info = table_info[:columns].find { |col| col[:name] == column_name }
        if col_info
          return col_info[:type].downcase.to_sym
        end
      end
      nil
    when :qualified_column
      table_ref = expr[:table]
      column_name = expr[:column]
      actual_table = table_context[:aliases][table_ref]
      return nil unless actual_table
      
      table_info = table_context[:tables][actual_table]
      col_info = table_info[:columns].find { |col| col[:name] == column_name }
      col_info ? col_info[:type].downcase.to_sym : nil
    when :binary_op
      case expr[:operator]
      when :plus, :minus, :star, :slash
        :integer
      when :lt, :gt, :lte, :gte, :equal, :not_equal, :and, :or
        :boolean
      end
    when :unary_op
      case expr[:operator]
      when :minus
        :integer
      when :not
        :boolean
      end
    when :function
      case expr[:name]
      when :abs, :mod
        :integer
      end
    end
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