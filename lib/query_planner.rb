class QueryPlanner
  def initialize(validator, evaluator)
    @validator = validator
    @evaluator = evaluator
  end
  
  def validate_query(parsed_sql, table_info, alias_mapping)
    table_name = parsed_sql[:table_name]
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    
    # Create a dummy row with appropriate types for validation
    dummy_row_data = create_dummy_row_data(table_info[:columns])
    
    # Validate SELECT expressions
    @validator.validate_select_expressions(expressions, dummy_row_data, table_name)
    
    # Validate WHERE clause if present
    if where_clause
      @validator.validate_qualified_columns(where_clause, table_name)
      return false unless @validator.validate_where_clause(where_clause, dummy_row_data)
    end
    
    # Validate ORDER BY if present
    if order_by
      unless validate_order_by(order_by, alias_mapping, table_name, dummy_row_data)
        return false
      end
    end
    
    # Validate LIMIT if present
    if limit
      return false unless @validator.validate_limit_offset_expression(limit)
    end
    
    # Validate OFFSET if present
    if offset
      return false unless @validator.validate_limit_offset_expression(offset)
    end
    
    true
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
  
  private
  
  def validate_order_by(order_by, alias_mapping, table_name, dummy_row_data)
    # First check if it's a simple alias reference
    if order_by[:expression][:type] == :column && alias_mapping[order_by[:expression][:name]]
      # It's an alias - this is allowed for simple references
      true
    else
      # For expressions containing aliases, they're not allowed
      if @validator.contains_alias_in_expression?(order_by[:expression], alias_mapping)
        return false
      end
      # Validate the ORDER BY expression normally
      @validator.validate_qualified_columns(order_by[:expression], table_name)
      @evaluator.validate_types(order_by[:expression], dummy_row_data)
      true
    end
  end
  
  def create_dummy_row_data(columns)
    dummy_row_data = {}
    columns.each do |col|
      case col[:type]
      when 'BOOLEAN'
        dummy_row_data[col[:name]] = true
      when 'INTEGER'
        dummy_row_data[col[:name]] = 0
      else
        dummy_row_data[col[:name]] = nil
      end
    end
    dummy_row_data
  end
end