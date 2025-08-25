class QueryPlanner
  def initialize(validator, evaluator)
    @validator = validator
    @evaluator = evaluator
  end
  
  def build_alias_mapping(expressions)
    mapping = {}
    expressions.each do |expr_info|
      if expr_info[:alias]
        mapping[expr_info[:alias]] = expr_info[:expression]
      end
    end
    mapping
  end
  
  def extract_column_names(expressions)
    expressions.map do |expr_info|
      if expr_info[:alias]
        expr_info[:alias]
      elsif expr_info[:expression][:type] == :column
        expr_info[:expression][:name]
      elsif expr_info[:expression][:type] == :qualified_column
        expr_info[:expression][:column]
      else
        ''
      end
    end
  end
end