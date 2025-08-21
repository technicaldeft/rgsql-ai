require_relative 'boolean_converter'

class RowProcessor
  def initialize(evaluator)
    @evaluator = evaluator
  end
  
  def build_row_data_hash(row, columns)
    row_data = {}
    columns.each_with_index do |col, idx|
      row_data[col[:name]] = row[idx]
    end
    row_data
  end
  
  def evaluate_result_row(expressions, row_data)
    expressions.map do |expr_info|
      value = @evaluator.evaluate(expr_info[:expression], row_data)
      BooleanConverter.convert(value)
    end
  end
  
  def filter_rows_with_where(rows, columns, where_clause, expressions)
    filtered_rows = []
    
    rows.each do |row|
      row_data = build_row_data_hash(row, columns)
      
      # Apply WHERE filter if present
      if where_clause
        where_result = @evaluator.evaluate(where_clause, row_data)
        # Only include row if WHERE evaluates to true (not false, not null)
        next unless where_result == true
      end
      
      result_row = evaluate_result_row(expressions, row_data)
      filtered_rows << { result: result_row, row_data: row_data }
    end
    
    filtered_rows
  end
  
  def apply_limit_offset(rows, limit_expr, offset_expr)
    # Evaluate LIMIT
    limit_value = nil
    if limit_expr
      limit_value = @evaluator.evaluate(limit_expr, {})
      # NULL means no limit
      return rows if limit_value.nil?
      # Negative or zero limit
      limit_value = [limit_value, 0].max
    end
    
    # Evaluate OFFSET
    offset_value = 0
    if offset_expr
      offset_value = @evaluator.evaluate(offset_expr, {})
      # NULL means no offset (start from 0)
      offset_value = 0 if offset_value.nil?
      offset_value = [offset_value, 0].max
    end
    
    # Apply offset first
    result = rows[offset_value..-1] || []
    
    # Then apply limit
    if limit_value
      result = result[0...limit_value] || []
    end
    
    result
  end
end