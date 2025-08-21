class RowSorter
  def initialize(evaluator)
    @evaluator = evaluator
  end
  
  def sort_rows(rows, order_by, alias_mapping, columns)
    rows.sort do |a, b|
      a_value = evaluate_sort_value(a, order_by, alias_mapping)
      b_value = evaluate_sort_value(b, order_by, alias_mapping)
      
      apply_sort_direction(compare_values_for_sort(a_value, b_value), order_by[:direction])
    end
  end
  
  private
  
  def evaluate_sort_value(row_info, order_by, alias_mapping)
    expr = order_by[:expression]
    
    if is_alias_reference?(expr, alias_mapping)
      @evaluator.evaluate(alias_mapping[expr[:name]], row_info[:row_data])
    else
      @evaluator.evaluate(expr, row_info[:row_data])
    end
  end
  
  def is_alias_reference?(expr, alias_mapping)
    expr[:type] == :column && alias_mapping[expr[:name]]
  end
  
  def apply_sort_direction(comparison, direction)
    direction == 'DESC' ? -comparison : comparison
  end
  
  def compare_values_for_sort(a, b)
    # Handle NULLs - they sort as larger than any non-null value
    if a.nil? && b.nil?
      0
    elsif a.nil?
      1
    elsif b.nil?
      -1
    elsif a.is_a?(TrueClass) || a.is_a?(FalseClass)
      # Boolean comparison: false < true
      if b.is_a?(TrueClass) || b.is_a?(FalseClass)
        a_val = a ? 1 : 0
        b_val = b ? 1 : 0
        a_val <=> b_val
      else
        # Type mismatch
        0
      end
    else
      # Regular comparison
      a <=> b
    end
  end
end