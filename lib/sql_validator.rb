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
  
  def validate_group_by_expression(select_expr, group_by_expr, dummy_row_data)
    # Validate types first
    @evaluator.validate_types(select_expr, dummy_row_data)
    
    # First check if the entire SELECT expression matches the GROUP BY expression
    return if expressions_equal?(select_expr, group_by_expr)
    
    # If GROUP BY is a function, check if the same function appears in SELECT
    if group_by_expr[:type] == :function && expression_contains_subexpression?(select_expr, group_by_expr)
      # The GROUP BY function is used in SELECT, now check remaining columns
      remaining_columns = extract_columns_not_in_expression(select_expr, group_by_expr)
      remaining_columns.each do |col|
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{col[:name] || col[:column]}"
      end
      return
    end
    
    # Check if SELECT expression contains columns not in GROUP BY
    select_columns = extract_columns_from_expression(select_expr)
    
    # If the SELECT expression has columns, check they're allowed
    select_columns.each do |col|
      unless column_allowed_in_group_by?(col, group_by_expr)
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{col[:name] || col[:column]}"
      end
    end
  end
  
  def expression_contains_subexpression?(expr, subexpr)
    # Check if expr contains subexpr anywhere
    return true if expressions_equal?(expr, subexpr)
    
    case expr[:type]
    when :binary_op
      return true if expression_contains_subexpression?(expr[:left], subexpr) if expr[:left]
      return true if expression_contains_subexpression?(expr[:right], subexpr) if expr[:right]
    when :unary_op
      return true if expression_contains_subexpression?(expr[:operand], subexpr) if expr[:operand]
    when :function
      expr[:args].each { |arg| return true if expression_contains_subexpression?(arg, subexpr) } if expr[:args]
    end
    
    false
  end
  
  def extract_columns_not_in_expression(expr, exclude_expr)
    # Extract columns from expr that are not part of exclude_expr
    columns = []
    extract_columns_not_in_expression_helper(expr, exclude_expr, columns)
    columns
  end
  
  def extract_columns_not_in_expression_helper(expr, exclude_expr, columns)
    # Skip if this expression matches the exclude expression
    return if expressions_equal?(expr, exclude_expr)
    
    case expr[:type]
    when :column, :qualified_column
      columns << expr
    when :binary_op
      extract_columns_not_in_expression_helper(expr[:left], exclude_expr, columns) if expr[:left]
      extract_columns_not_in_expression_helper(expr[:right], exclude_expr, columns) if expr[:right]
    when :unary_op
      extract_columns_not_in_expression_helper(expr[:operand], exclude_expr, columns) if expr[:operand]
    when :function
      # Skip if this function matches the exclude expression
      return if expressions_equal?(expr, exclude_expr)
      expr[:args].each { |arg| extract_columns_not_in_expression_helper(arg, exclude_expr, columns) } if expr[:args]
    end
  end
  
  def extract_columns_from_expression(expr)
    columns = []
    
    case expr[:type]
    when :column
      columns << expr
    when :qualified_column
      columns << expr
    when :binary_op
      columns.concat(extract_columns_from_expression(expr[:left])) if expr[:left]
      columns.concat(extract_columns_from_expression(expr[:right])) if expr[:right]
    when :unary_op
      columns.concat(extract_columns_from_expression(expr[:operand])) if expr[:operand]
    when :function
      expr[:args].each { |arg| columns.concat(extract_columns_from_expression(arg)) } if expr[:args]
    end
    
    columns
  end
  
  def column_allowed_in_group_by?(column, group_by_expr)
    # Check if the column is directly the GROUP BY expression
    return true if expressions_equal?(column, group_by_expr)
    
    # For function GROUP BY expressions, columns can't be used raw in SELECT
    if group_by_expr[:type] == :function
      # Columns are not allowed unless they're the exact same expression
      return false
    end
    
    # Check if the GROUP BY expression contains this column
    return true if expression_contains_column?(group_by_expr, column)
    
    # Check if qualified column matches unqualified GROUP BY column
    if column[:type] == :qualified_column && group_by_expr[:type] == :column
      return true if column[:column].downcase == group_by_expr[:name].downcase
    end
    
    # Check if unqualified column matches qualified GROUP BY column
    if column[:type] == :column && group_by_expr[:type] == :qualified_column
      return true if column[:name].downcase == group_by_expr[:column].downcase
    end
    
    false
  end
  
  def expressions_equal?(expr1, expr2)
    # Allow qualified/unqualified column matching
    if expr1[:type] == :column && expr2[:type] == :qualified_column
      return expr1[:name].downcase == expr2[:column].downcase
    elsif expr1[:type] == :qualified_column && expr2[:type] == :column
      return expr1[:column].downcase == expr2[:name].downcase
    end
    
    # Simple equality check for same type expressions
    return false unless expr1[:type] == expr2[:type]
    
    case expr1[:type]
    when :column
      expr1[:name].downcase == expr2[:name].downcase
    when :qualified_column
      expr1[:table].downcase == expr2[:table].downcase &&
        expr1[:column].downcase == expr2[:column].downcase
    when :function
      expr1[:name] == expr2[:name] &&
        expr1[:args].size == expr2[:args].size &&
        expr1[:args].zip(expr2[:args]).all? { |a, b| expressions_equal?(a, b) }
    else
      false
    end
  end
  
  def expression_contains_column?(expr, column)
    case expr[:type]
    when :column
      column[:type] == :column && expr[:name].downcase == column[:name].downcase
    when :qualified_column
      if column[:type] == :column
        expr[:column].downcase == column[:name].downcase
      elsif column[:type] == :qualified_column
        expr[:table].downcase == column[:table].downcase &&
          expr[:column].downcase == column[:column].downcase
      else
        false
      end
    when :function
      # Check if any argument contains the column
      expr[:args].any? { |arg| expression_contains_column?(arg, column) } if expr[:args]
    else
      false
    end
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