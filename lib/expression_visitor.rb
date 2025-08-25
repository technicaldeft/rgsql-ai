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

class ExpressionValidatorVisitor < ExpressionVisitor
  def initialize(table_context, evaluator)
    @table_context = table_context
    @evaluator = evaluator
    @errors = []
  end
  
  def errors
    @errors
  end
  
  def valid?
    @errors.empty?
  end
  
  protected
  
  def visit_column(expr)
    column_name = expr[:name]
    found = false
    count = 0
    
    table_context_hash = @table_context.is_a?(Hash) ? @table_context : @table_context.to_hash
    
    table_context_hash[:aliases].each do |alias_name, table_name|
      table_info = table_context_hash[:tables][table_name]
      if table_info[:columns].any? { |col| col[:name] == column_name }
        found = true
        count += 1
      end
    end
    
    unless found
      @errors << "Unknown column: #{column_name}"
    end
    
    if count > 1
      @errors << "Ambiguous column: #{column_name}"
    end
  end
  
  def visit_qualified_column(expr)
    table_ref = expr[:table]
    column_name = expr[:column]
    
    table_context_hash = @table_context.is_a?(Hash) ? @table_context : @table_context.to_hash
    
    actual_table = table_context_hash[:aliases][table_ref]
    unless actual_table
      @errors << "Unknown table: #{table_ref}"
      return
    end
    
    table_info = table_context_hash[:tables][actual_table]
    unless table_info[:columns].any? { |col| col[:name] == column_name }
      @errors << "Unknown column: #{table_ref}.#{column_name}"
    end
  end
  
  def visit_binary_op(expr)
    super
    
    # Type validation for comparison operators
    if expr[:operator] == :equal || expr[:operator] == :not_equal
      left_type = get_expression_type(expr[:left])
      right_type = get_expression_type(expr[:right])
      
      if (left_type == :integer && right_type == :boolean) ||
         (left_type == :boolean && right_type == :integer)
        @errors << "Type mismatch in comparison"
      end
    end
  end
  
  private
  
  def get_expression_type(expr)
    # Simplified type inference
    case expr[:type]
    when :literal
      if expr[:value].nil?
        nil
      elsif expr[:value] == true || expr[:value] == false
        :boolean
      else
        :integer
      end
    when :column, :qualified_column
      # Would need to look up column type in table context
      :unknown
    when :binary_op
      case expr[:operator]
      when :plus, :minus, :star, :slash
        :integer
      when :lt, :gt, :lte, :gte, :equal, :not_equal, :and, :or
        :boolean
      end
    else
      :unknown
    end
  end
end

class ExpressionTransformVisitor < ExpressionVisitor
  def visit(expression)
    return nil unless expression
    
    case expression[:type]
    when :literal
      transform_literal(expression)
    when :column
      transform_column(expression)
    when :qualified_column
      transform_qualified_column(expression)
    when :binary_op
      transform_binary_op(expression)
    when :unary_op
      transform_unary_op(expression)
    when :function
      transform_function(expression)
    when :aggregate_function
      transform_aggregate_function(expression)
    when :is_null
      transform_is_null(expression)
    when :is_not_null
      transform_is_not_null(expression)
    else
      expression
    end
  end
  
  protected
  
  def transform_literal(expr)
    expr
  end
  
  def transform_column(expr)
    expr
  end
  
  def transform_qualified_column(expr)
    expr
  end
  
  def transform_binary_op(expr)
    {
      type: :binary_op,
      operator: expr[:operator],
      left: visit(expr[:left]),
      right: visit(expr[:right])
    }
  end
  
  def transform_unary_op(expr)
    {
      type: :unary_op,
      operator: expr[:operator],
      operand: visit(expr[:operand])
    }
  end
  
  def transform_function(expr)
    {
      type: :function,
      name: expr[:name],
      args: expr[:args]&.map { |arg| visit(arg) }
    }
  end
  
  def transform_aggregate_function(expr)
    {
      type: :aggregate_function,
      name: expr[:name],
      args: expr[:args]&.map { |arg| visit(arg) }
    }
  end
  
  def transform_is_null(expr)
    {
      type: :is_null,
      operand: visit(expr[:operand])
    }
  end
  
  def transform_is_not_null(expr)
    {
      type: :is_not_null,
      operand: visit(expr[:operand])
    }
  end
end