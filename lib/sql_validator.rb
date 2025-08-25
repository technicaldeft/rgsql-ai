require_relative 'expression_matcher'
require_relative 'aggregate_evaluator'
require_relative 'validation_context'

class SqlValidator
  def initialize(evaluator)
    @evaluator = evaluator
    @expression_matcher = ExpressionMatcher.new
    @aggregate_evaluator = AggregateEvaluator.new(evaluator)
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
    when :aggregate_function
      case expr[:name]
      when :count, :sum
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
    when :aggregate_function
      expr[:args].each { |arg| validate_qualified_columns(arg, table_name) } if expr[:args] && !expr[:args].empty?
    end
  end
  
  def validate_where_clause(where_clause, dummy_row_data)
    # Check for aggregate functions in WHERE clause
    if @aggregate_evaluator.has_aggregate_functions?(where_clause)
      raise ExpressionEvaluator::ValidationError, "Aggregate functions not allowed in WHERE clause"
    end
    
    @evaluator.validate_types(where_clause, dummy_row_data)
    
    # Check that WHERE clause evaluates to boolean or NULL
    where_type = @evaluator.get_expression_type(where_clause, dummy_row_data)
    where_type == :boolean || where_type.nil?
  end
  
  def validate_group_by_expression(select_expr, group_by_expr, dummy_row_data)
    # Validate types first - but handle aggregate functions specially
    if select_expr[:type] == :aggregate_function
      @aggregate_evaluator.validate_aggregate_types(select_expr, dummy_row_data)
    elsif contains_aggregate_function?(select_expr)
      # For expressions containing aggregates, validate the aggregate parts
      validate_expression_with_aggregates(select_expr, dummy_row_data)
    else
      @evaluator.validate_types(select_expr, dummy_row_data)
    end
    
    # Aggregate functions are always allowed with GROUP BY
    return if select_expr[:type] == :aggregate_function
    
    # If the expression contains aggregate functions, validate only the non-aggregate parts
    if contains_aggregate_function?(select_expr)
      validate_non_aggregate_parts_in_group_by(select_expr, group_by_expr, dummy_row_data)
      return
    end
    
    # First check if the entire SELECT expression matches the GROUP BY expression
    return if @expression_matcher.expressions_equal?(select_expr, group_by_expr)
    
    # If GROUP BY is a function, check if the same function appears in SELECT
    if group_by_expr[:type] == :function && @expression_matcher.expression_contains_subexpression?(select_expr, group_by_expr)
      # The GROUP BY function is used in SELECT, now check remaining columns
      remaining_columns = @expression_matcher.extract_columns_not_in_expression(select_expr, group_by_expr)
      remaining_columns.each do |col|
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{col[:name] || col[:column]}"
      end
      return
    end
    
    # Check if SELECT expression contains columns not in GROUP BY
    select_columns = @expression_matcher.extract_columns_from_expression(select_expr)
    
    # If the SELECT expression has columns, check they're allowed
    select_columns.each do |col|
      unless column_allowed_in_group_by?(col, group_by_expr)
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{col[:name] || col[:column]}"
      end
    end
  end
  
  def contains_aggregate_function?(expr)
    case expr[:type]
    when :aggregate_function
      true
    when :binary_op
      contains_aggregate_function?(expr[:left]) || contains_aggregate_function?(expr[:right])
    when :unary_op
      contains_aggregate_function?(expr[:operand])
    when :function
      expr[:args]&.any? { |arg| contains_aggregate_function?(arg) } || false
    else
      false
    end
  end
  
  def validate_non_aggregate_parts_in_group_by(expr, group_by_expr, dummy_row_data)
    case expr[:type]
    when :binary_op
      validate_non_aggregate_parts_in_group_by(expr[:left], group_by_expr, dummy_row_data) if expr[:left]
      validate_non_aggregate_parts_in_group_by(expr[:right], group_by_expr, dummy_row_data) if expr[:right]
    when :unary_op
      validate_non_aggregate_parts_in_group_by(expr[:operand], group_by_expr, dummy_row_data) if expr[:operand]
    when :function
      expr[:args]&.each { |arg| validate_non_aggregate_parts_in_group_by(arg, group_by_expr, dummy_row_data) }
    when :column, :qualified_column
      unless column_allowed_in_group_by?(expr, group_by_expr)
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{expr[:name] || expr[:column]}"
      end
    end
  end
  
  
  def column_allowed_in_group_by?(column, group_by_expr)
    # Check if the column is directly the GROUP BY expression
    return true if @expression_matcher.expressions_equal?(column, group_by_expr)
    
    # For function GROUP BY expressions, columns can't be used raw in SELECT
    if group_by_expr[:type] == :function
      # Columns are not allowed unless they're the exact same expression
      return false
    end
    
    # Check if the GROUP BY expression contains this column
    return true if @expression_matcher.expression_contains_column?(group_by_expr, column)
    
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
  
  
  def validate_limit_offset_expression(expr)
    # Cannot contain column references
    return false if @expression_matcher.contains_column_reference?(expr)
    
    # Cannot contain aggregate functions
    return false if @aggregate_evaluator.has_aggregate_functions?(expr)
    
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
  
  def validate_expression_with_aggregates(expr, dummy_row_data)
    case expr[:type]
    when :aggregate_function
      @aggregate_evaluator.validate_aggregate_types(expr, dummy_row_data)
    when :binary_op
      validate_expression_with_aggregates(expr[:left], dummy_row_data) if expr[:left]
      validate_expression_with_aggregates(expr[:right], dummy_row_data) if expr[:right]
    when :unary_op
      validate_expression_with_aggregates(expr[:operand], dummy_row_data) if expr[:operand]
    when :function
      expr[:args]&.each { |arg| validate_expression_with_aggregates(arg, dummy_row_data) }
    else
      @evaluator.validate_types(expr, dummy_row_data)
    end
  end
  
  def validate_group_by_expression_with_context(select_expr, group_by_expr, table_context)
    validation_context = ValidationContext.new(table_context, @evaluator, self)
    
    if select_expr[:type] == :aggregate_function
      validate_aggregate_expression_with_context(select_expr, table_context)
    elsif contains_aggregate_function?(select_expr)
      validate_aggregate_expression_with_context(select_expr, table_context)
    else
      validate_non_aggregate_in_group_by_context(select_expr, group_by_expr, table_context)
    end
  end
  
  def validate_aggregate_expression_with_context(expr, table_context)
    case expr[:type]
    when :aggregate_function
      # Check for nested aggregates
      if @aggregate_evaluator.has_nested_aggregate?(expr)
        raise ExpressionEvaluator::ValidationError, "Cannot nest aggregate functions"
      end
      
      # Validate the argument if present
      if expr[:args] && expr[:args].first
        validate_columns_in_context(expr[:args].first, table_context)
        
        # For SUM, check that the argument is an integer type
        if expr[:name] == :sum
          arg_type = get_expression_type_with_context(expr[:args].first, table_context)
          if arg_type && arg_type != :integer
            raise ExpressionEvaluator::ValidationError, "SUM requires integer argument"
          end
        end
      elsif expr[:name] == :sum
        # SUM requires an argument
        raise ExpressionEvaluator::ValidationError, "SUM requires an argument"
      end
    when :binary_op
      validate_aggregate_expression_with_context(expr[:left], table_context) if expr[:left]
      validate_aggregate_expression_with_context(expr[:right], table_context) if expr[:right]
    when :unary_op
      validate_aggregate_expression_with_context(expr[:operand], table_context) if expr[:operand]
    when :function
      expr[:args]&.each { |arg| validate_aggregate_expression_with_context(arg, table_context) }
    else
      validate_columns_in_context(expr, table_context)
    end
  end
  
  def validate_non_aggregate_in_group_by_context(expr, group_by_expr, table_context)
    # First check if the entire expression matches the GROUP BY expression
    return if expressions_equal_in_context?(expr, group_by_expr)
    
    # Extract columns from the expression
    columns = @expression_matcher.extract_columns_from_expression(expr)
    
    # Check each column is in GROUP BY
    columns.each do |col|
      unless column_in_group_by_context?(col, group_by_expr)
        raise ExpressionEvaluator::ValidationError, "Column not in GROUP BY: #{col[:name] || col[:column]}"
      end
    end
  end
  
  def expressions_equal_in_context?(expr1, expr2)
    @expression_matcher.expressions_equal?(expr1, expr2)
  end
  
  def column_in_group_by_context?(column, group_by_expr)
    @expression_matcher.expressions_equal?(column, group_by_expr) ||
    @expression_matcher.expression_contains_column?(group_by_expr, column)
  end
end