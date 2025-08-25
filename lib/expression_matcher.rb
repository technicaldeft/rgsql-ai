require_relative 'expression_visitor'

class ExpressionMatcher
  def expressions_equal?(expr1, expr2)
    return false unless expr1 && expr2
    
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
    when :literal
      expr1[:value] == expr2[:value]
    when :function
      expr1[:name] == expr2[:name] &&
        expr1[:args].size == expr2[:args].size &&
        expr1[:args].zip(expr2[:args]).all? { |a, b| expressions_equal?(a, b) }
    when :binary_op
      expr1[:operator] == expr2[:operator] &&
        expressions_equal?(expr1[:left], expr2[:left]) &&
        expressions_equal?(expr1[:right], expr2[:right])
    when :unary_op
      expr1[:operator] == expr2[:operator] &&
        expressions_equal?(expr1[:operand], expr2[:operand])
    else
      false
    end
  end
  
  def expression_contains_subexpression?(expr, subexpr)
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
  
  def extract_columns_from_expression(expr)
    visitor = ColumnExtractorVisitor.new
    visitor.visit(expr)
    visitor.columns
  end
  
  def extract_columns_not_in_expression(expr, exclude_expr)
    columns = []
    extract_columns_not_in_expression_helper(expr, exclude_expr, columns)
    columns
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
      expr[:args].any? { |arg| expression_contains_column?(arg, column) } if expr[:args]
    when :binary_op
      expression_contains_column?(expr[:left], column) || expression_contains_column?(expr[:right], column)
    when :unary_op
      expression_contains_column?(expr[:operand], column)
    else
      false
    end
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
  
  private
  
  def extract_columns_not_in_expression_helper(expr, exclude_expr, columns)
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
      return if expressions_equal?(expr, exclude_expr)
      expr[:args].each { |arg| extract_columns_not_in_expression_helper(arg, exclude_expr, columns) } if expr[:args]
    end
  end
end