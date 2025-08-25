class ExpressionVisitor
  def visit(expression)
    return nil unless expression
    
    case expression[:type]
    when :literal
      visit_literal(expression)
    when :column
      visit_column(expression)
    when :qualified_column
      visit_qualified_column(expression)
    when :binary_op
      visit_binary_op(expression)
    when :unary_op
      visit_unary_op(expression)
    when :function
      visit_function(expression)
    when :aggregate_function
      visit_aggregate_function(expression)
    when :is_null
      visit_is_null(expression)
    when :is_not_null
      visit_is_not_null(expression)
    else
      visit_unknown(expression)
    end
  end
  
  protected
  
  def visit_literal(expr)
    # Override in subclasses
  end
  
  def visit_column(expr)
    # Override in subclasses
  end
  
  def visit_qualified_column(expr)
    # Override in subclasses
  end
  
  def visit_binary_op(expr)
    visit(expr[:left])
    visit(expr[:right])
  end
  
  def visit_unary_op(expr)
    visit(expr[:operand])
  end
  
  def visit_function(expr)
    expr[:args]&.each { |arg| visit(arg) }
  end
  
  def visit_aggregate_function(expr)
    expr[:args]&.each { |arg| visit(arg) }
  end
  
  def visit_is_null(expr)
    visit(expr[:operand])
  end
  
  def visit_is_not_null(expr)
    visit(expr[:operand])
  end
  
  def visit_unknown(expr)
    # Handle unknown expression types
  end
end

class AggregateDetectorVisitor < ExpressionVisitor
  def initialize
    @has_aggregates = false
  end
  
  def has_aggregates?
    @has_aggregates
  end
  
  protected
  
  def visit_aggregate_function(expr)
    @has_aggregates = true
    super
  end
end

class ColumnExtractorVisitor < ExpressionVisitor
  def initialize
    @columns = []
  end
  
  def columns
    @columns
  end
  
  protected
  
  def visit_column(expr)
    @columns << expr
  end
  
  def visit_qualified_column(expr)
    @columns << expr
  end
end