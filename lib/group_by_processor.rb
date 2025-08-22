require_relative 'expression_evaluator'

class GroupByProcessor
  def initialize(evaluator)
    @evaluator = evaluator
  end
  
  def group_rows(rows, group_by_expr, table_context)
    return [rows] if rows.empty?
    
    # Group rows by evaluating the GROUP BY expression for each row
    groups = {}
    
    rows.each do |row|
      # Evaluate the GROUP BY expression for this row
      group_key = @evaluator.evaluate(group_by_expr, row)
      
      # Handle NULL values in group key
      group_key = normalize_group_key(group_key)
      
      # Add row to appropriate group
      groups[group_key] ||= []
      groups[group_key] << row
    end
    
    # Return array of grouped rows
    groups.values
  end
  
  private
  
  def create_row_context(row, table_context)
    # Row is already the data we need for grouping
    row
  end
  
  def normalize_group_key(value)
    # Convert nil to a special marker for grouping
    # This ensures NULL values are grouped together
    value.nil? ? :null_group : value
  end
end