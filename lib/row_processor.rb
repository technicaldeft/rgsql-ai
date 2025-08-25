require_relative 'boolean_converter'

class RowProcessor
  def initialize(evaluator)
    @evaluator = evaluator
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