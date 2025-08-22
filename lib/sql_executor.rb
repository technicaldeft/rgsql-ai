require 'set'
require_relative 'table_manager'
require_relative 'boolean_converter'
require_relative 'expression_evaluator'
require_relative 'error_handler'
require_relative 'row_processor'
require_relative 'sql_validator'
require_relative 'row_sorter'
require_relative 'query_planner'

class SqlExecutor
  include ErrorHandler
  def initialize
    @table_manager = TableManager.new
    @evaluator = ExpressionEvaluator.new
    @row_processor = RowProcessor.new(@evaluator)
    @validator = SqlValidator.new(@evaluator)
    @row_sorter = RowSorter.new(@evaluator)
    @query_planner = QueryPlanner.new(@validator, @evaluator)
  end
  
  def execute(parsed_sql)
    return parsed_sql if has_error?(parsed_sql)
    
    case parsed_sql[:type]
    when :select
      execute_select(parsed_sql)
    when :select_from
      execute_select_from(parsed_sql)
    when :create_table
      execute_create_table(parsed_sql)
    when :drop_table
      execute_drop_table(parsed_sql)
    when :insert_multiple
      execute_insert_multiple(parsed_sql)
    else
      unknown_command_error
    end
  end
  
  private
  
  def execute_select(parsed_sql)
    expressions = parsed_sql[:expressions]
    columns = parsed_sql[:columns]
    
    if expressions.empty?
      return { rows: [] }
    end
    
    values = []
    
    begin
      # First validate types
      expressions.each do |expr|
        @evaluator.validate_types(expr)
      end
      
      # Then evaluate
      values = expressions.map do |expr|
        value = @evaluator.evaluate(expr)
        BooleanConverter.convert(value)
      end
    rescue ExpressionEvaluator::DivisionByZeroError
      return division_by_zero_error
    rescue ExpressionEvaluator::ValidationError
      return validation_error
    end
    
    result = { rows: [values] }
    
    if columns.any?(&:itself)
      result[:columns] = columns.map { |col| col || '' }
    end
    
    result
  end
  
  def execute_select_from(parsed_sql)
    table_name = parsed_sql[:table_name]
    table_alias = parsed_sql[:table_alias]
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    joins = parsed_sql[:joins] || []
    
    # Build table context for column resolution
    table_context = build_table_context(table_name, table_alias, joins)
    return table_context if has_error?(table_context)
    
    # Get rows from all tables
    if joins.empty?
      # Simple query without joins
      table_info = @table_manager.get_table_info(table_name)
      return { error: 'validation_error' } unless table_info
      
      all_data = @table_manager.get_all_rows(table_name)
      return all_data if has_error?(all_data)
      
      # Build row context for single table
      row_contexts = all_data[:rows].map do |row|
        context = {}
        table_info[:columns].each_with_index do |col_info, idx|
          context[col_info[:name]] = row[idx]
          # Also add qualified names
          if table_alias
            context["#{table_alias}.#{col_info[:name]}"] = row[idx]
          else
            context["#{table_name}.#{col_info[:name]}"] = row[idx]
          end
        end
        context
      end
    else
      # Query with joins
      row_contexts = execute_joins(table_name, table_alias, joins, table_context)
      return row_contexts if has_error?(row_contexts)
    end
    
    result_rows = []
    
    # Build alias mapping for ORDER BY validation
    alias_mapping = @query_planner.build_alias_mapping(expressions)
    
    # Validate expressions against schema
    begin
      if joins.empty?
        # For non-JOIN queries, validate with the original validation logic
        dummy_row_data = {}
        table_info = table_context[:tables][table_name]
        table_info[:columns].each do |col|
          # Use appropriate dummy values based on type
          dummy_row_data[col[:name]] = case col[:type]
          when 'INTEGER'
            0
          when 'BOOLEAN'
            false
          else
            nil
          end
        end
        
        expressions.each do |expr_info|
          # First validate qualified column references
          @validator.validate_qualified_columns(expr_info[:expression], table_name)
          @evaluator.validate_types(expr_info[:expression], dummy_row_data)
        end
        
        if where_clause
          @evaluator.validate_types(where_clause, dummy_row_data)
          where_type = @evaluator.get_expression_type(where_clause, dummy_row_data)
          unless where_type == :boolean || where_type.nil?
            return validation_error
          end
        end
        
        if order_by
          # Check if it's an alias reference first
          order_expr = order_by[:expression]
          if order_expr[:type] == :column && alias_mapping[order_expr[:name]]
            # Validate the aliased expression instead
            @evaluator.validate_types(alias_mapping[order_expr[:name]], dummy_row_data)
          else
            @evaluator.validate_types(order_expr, dummy_row_data)
          end
        end
        
        if limit
          unless @validator.validate_limit_offset_expression(limit)
            return validation_error
          end
        end
        
        if offset
          unless @validator.validate_limit_offset_expression(offset)
            return validation_error
          end
        end
      else
        # For JOIN queries, use context-based validation
        validation_errors = validate_join_query(parsed_sql, table_context, alias_mapping)
        return validation_errors if has_error?(validation_errors)
      end
      
      # Process rows with WHERE filtering
      filtered_rows = []
      
      if joins.empty?
        # For non-JOIN queries, use original evaluation
        row_contexts.each do |row_context|
          # Convert row_context back to simple row_data for non-join queries
          row_data = {}
          table_info = table_context[:tables][table_name]
          table_info[:columns].each do |col|
            row_data[col[:name]] = row_context[col[:name]]
          end
          
          if where_clause
            where_result = @evaluator.evaluate(where_clause, row_data)
            next unless where_result == true
          end
          
          # Evaluate SELECT expressions
          result_row = expressions.map do |expr_info|
            value = @evaluator.evaluate(expr_info[:expression], row_data)
            BooleanConverter.convert(value)
          end
          
          filtered_rows << { result: result_row, row_data: row_data }
        end
      else
        # For JOIN queries, use context-based evaluation
        row_contexts.each do |row_context|
          if where_clause
            where_result = @evaluator.evaluate_with_context(where_clause, row_context, table_context)
            next unless where_result == true
          end
          
          # Evaluate SELECT expressions
          result_row = expressions.map do |expr_info|
            value = @evaluator.evaluate_with_context(expr_info[:expression], row_context, table_context)
            BooleanConverter.convert(value)
          end
          
          filtered_rows << { result: result_row, context: row_context }
        end
      end
      
      # Apply ORDER BY if present
      if order_by
        if joins.empty?
          filtered_rows = @row_sorter.sort_rows(filtered_rows, order_by, alias_mapping, table_info[:columns])
        else
          filtered_rows = @row_sorter.sort_rows_with_context(filtered_rows, order_by, alias_mapping, table_context)
        end
      end
      
      # Extract just the result rows
      result_rows = filtered_rows.map { |row_info| row_info[:result] }
      
      # Apply LIMIT and OFFSET
      if limit || offset
        result_rows = @row_processor.apply_limit_offset(result_rows, limit, offset)
      end
      
    rescue ExpressionEvaluator::DivisionByZeroError
      return division_by_zero_error
    rescue ExpressionEvaluator::ValidationError
      return validation_error
    end
    
    # Build column names from expressions or aliases
    column_names = @query_planner.extract_column_names(expressions)
    
    { rows: result_rows, columns: column_names }
  end
  
  private
  
  def build_table_context(from_table, from_alias, joins)
    context = {
      tables: {},
      aliases: {}
    }
    
    # Add FROM table
    table_info = @table_manager.get_table_info(from_table)
    return validation_error unless table_info
    
    actual_alias = from_alias || from_table
    context[:tables][from_table] = table_info
    context[:aliases][actual_alias] = from_table
    
    # Check for duplicate table names
    seen_aliases = Set.new([actual_alias])
    
    # Add JOIN tables
    joins.each do |join|
      join_table_info = @table_manager.get_table_info(join[:table])
      return validation_error unless join_table_info
      
      join_alias = join[:alias] || join[:table]
      
      # Check for duplicate aliases
      if seen_aliases.include?(join_alias)
        return validation_error
      end
      
      seen_aliases.add(join_alias)
      context[:tables][join[:table]] = join_table_info
      context[:aliases][join_alias] = join[:table]
    end
    
    context
  end
  
  def execute_joins(from_table, from_alias, joins, table_context)
    # Get rows from FROM table
    from_table_info = table_context[:tables][from_table]
    from_data = @table_manager.get_all_rows(from_table)
    return from_data if has_error?(from_data)
    
    actual_from_alias = from_alias || from_table
    
    # Build initial row contexts from FROM table
    row_contexts = from_data[:rows].map do |row|
      context = {}
      from_table_info[:columns].each_with_index do |col_info, idx|
        context[col_info[:name]] = row[idx]
        context["#{actual_from_alias}.#{col_info[:name]}"] = row[idx]
      end
      context
    end
    
    # Process each JOIN
    joins.each do |join|
      join_table = join[:table]
      join_alias = join[:alias] || join_table
      join_type = join[:type]
      join_condition = join[:on]
      
      # Get rows from joined table
      join_table_info = table_context[:tables][join_table]
      join_data = @table_manager.get_all_rows(join_table)
      return join_data if has_error?(join_data)
      
      # Perform the join
      new_row_contexts = []
      
      row_contexts.each do |left_context|
        matched = false
        
        join_data[:rows].each do |join_row|
          # Build context for joined row
          join_context = left_context.dup
          join_table_info[:columns].each_with_index do |col_info, idx|
            join_context[col_info[:name]] = join_row[idx]
            join_context["#{join_alias}.#{col_info[:name]}"] = join_row[idx]
          end
          
          # Evaluate JOIN condition
          begin
            condition_result = @evaluator.evaluate_with_context(join_condition, join_context, table_context)
            
            if condition_result == true
              matched = true
              new_row_contexts << join_context
            end
          rescue
            # Invalid comparison or type error in JOIN condition
          end
        end
        
        # Handle outer joins
        if !matched
          if join_type == "LEFT_OUTER" || join_type == "FULL_OUTER"
            # Add row with NULLs for joined table
            join_context = left_context.dup
            join_table_info[:columns].each do |col_info|
              join_context[col_info[:name]] = nil
              join_context["#{join_alias}.#{col_info[:name]}"] = nil
            end
            new_row_contexts << join_context
          end
        end
      end
      
      # Handle RIGHT and FULL OUTER JOINs
      if join_type == "RIGHT_OUTER" || join_type == "FULL_OUTER"
        # Find unmatched rows from right table
        join_data[:rows].each do |join_row|
          matched = false
          
          # Check if this row was matched
          row_contexts.each do |left_context|
            join_context = left_context.dup
            join_table_info[:columns].each_with_index do |col_info, idx|
              join_context[col_info[:name]] = join_row[idx]
              join_context["#{join_alias}.#{col_info[:name]}"] = join_row[idx]
            end
            
            begin
              condition_result = @evaluator.evaluate_with_context(join_condition, join_context, table_context)
              if condition_result == true
                matched = true
                break
              end
            rescue
              # Invalid comparison
            end
          end
          
          if !matched
            # Add row with NULLs for left tables
            join_context = {}
            
            # Add NULLs for all previous tables
            row_contexts.first&.each_key do |key|
              join_context[key] = nil unless key.include?(".")
            end
            
            # Add values from right table
            join_table_info[:columns].each_with_index do |col_info, idx|
              join_context[col_info[:name]] = join_row[idx]
              join_context["#{join_alias}.#{col_info[:name]}"] = join_row[idx]
            end
            
            new_row_contexts << join_context
          end
        end
      end
      
      row_contexts = new_row_contexts
    end
    
    row_contexts
  end
  
  def validate_join_query(parsed_sql, table_context, alias_mapping)
    # Validate all expressions can be resolved
    parsed_sql[:expressions].each do |expr_info|
      validation = @validator.validate_expression_with_context(expr_info[:expression], table_context)
      return validation if has_error?(validation)
    end
    
    # Validate WHERE clause if present
    if parsed_sql[:where]
      validation = @validator.validate_expression_with_context(parsed_sql[:where], table_context)
      return validation if has_error?(validation)
    end
    
    # Validate ORDER BY if present
    if parsed_sql[:order_by]
      # Check if it's an alias reference first
      order_expr = parsed_sql[:order_by][:expression]
      if order_expr[:type] == :column && alias_mapping[order_expr[:name]]
        # Validate the aliased expression instead
        validation = @validator.validate_expression_with_context(alias_mapping[order_expr[:name]], table_context)
      else
        validation = @validator.validate_expression_with_context(order_expr, table_context)
      end
      return validation if has_error?(validation)
    end
    
    # Validate JOIN conditions - these must check for boolean type
    if parsed_sql[:joins]
      parsed_sql[:joins].each do |join|
        validation = @validator.validate_expression_with_context(join[:on], table_context)
        return validation if has_error?(validation)
        
        # Check that JOIN condition evaluates to boolean
        join_type = @validator.get_expression_type_with_context(join[:on], table_context)
        unless join_type == :boolean || join_type.nil?
          return validation_error
        end
      end
    end
    
    # Validate LIMIT and OFFSET  
    if parsed_sql[:limit]
      unless @validator.validate_limit_offset_expression(parsed_sql[:limit])
        return validation_error
      end
    end
    
    if parsed_sql[:offset]
      unless @validator.validate_limit_offset_expression(parsed_sql[:offset])
        return validation_error
      end
    end
    
    nil  # Return nil for success
  end
  
  def execute_create_table(parsed_sql)
    @table_manager.create_table(parsed_sql[:table_name], parsed_sql[:columns])
  end
  
  def execute_drop_table(parsed_sql)
    @table_manager.drop_table(parsed_sql[:table_name], if_exists: parsed_sql[:if_exists])
  end
  
  def execute_insert_multiple(parsed_sql)
    table_name = parsed_sql[:table_name]
    value_sets = parsed_sql[:value_sets]
    
    # Get table info for type validation
    table_info = @table_manager.get_table_info(table_name)
    return { error: 'validation_error' } unless table_info
    
    # First, evaluate and validate all rows
    all_values = []
    begin
      value_sets.each do |expressions|
        # Evaluate each expression to get actual values
        values = expressions.map do |expr|
          @evaluator.evaluate(expr)
        end
        
        # Validate types match table schema
        return validation_error unless @validator.validate_row_types(values, table_info[:columns])
        
        all_values << values
      end
    rescue ExpressionEvaluator::DivisionByZeroError
      return division_by_zero_error
    rescue ExpressionEvaluator::ValidationError
      return validation_error
    end
    
    # Only insert if all rows are valid
    all_values.each do |values|
      result = @table_manager.insert_row(table_name, values)
      return result if has_error?(result)
    end
    
    ok_status
  end
  
  
  
  
  
  
end