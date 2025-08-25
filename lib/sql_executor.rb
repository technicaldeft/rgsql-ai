require 'set'
require_relative 'table_manager'
require_relative 'boolean_converter'
require_relative 'expression_evaluator'
require_relative 'error_handler'
require_relative 'row_processor'
require_relative 'sql_validator'
require_relative 'row_sorter'
require_relative 'query_planner'
require_relative 'row_context_builder'
require_relative 'group_by_processor'
require_relative 'query_processor'
require_relative 'table_context'

class SqlExecutor
  include ErrorHandler
  def initialize
    @table_manager = TableManager.new
    @evaluator = ExpressionEvaluator.new
    @row_processor = RowProcessor.new(@evaluator)
    @validator = SqlValidator.new(@evaluator)
    @row_sorter = RowSorter.new(@evaluator)
    @query_planner = QueryPlanner.new(@validator, @evaluator)
    @row_context_builder = RowContextBuilder.new
    @group_by_processor = GroupByProcessor.new(@evaluator)
    @simple_query_processor = SimpleQueryProcessor.new(@evaluator, @row_processor, @validator, @group_by_processor)
    @join_query_processor = JoinQueryProcessor.new(@evaluator, @row_processor, @validator, @group_by_processor)
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
    # Build and validate table context
    table_context = build_and_validate_table_context(parsed_sql)
    return table_context if has_error?(table_context)
    
    # Get row contexts from tables
    row_contexts = fetch_row_contexts(parsed_sql, table_context)
    return row_contexts if has_error?(row_contexts)
    
    # Build alias mapping for ORDER BY
    alias_mapping = @query_planner.build_alias_mapping(parsed_sql[:expressions])
    
    # Validate query against schema
    validation_result = validate_query(parsed_sql, table_context, alias_mapping)
    return validation_result if has_error?(validation_result)
    
    # Process rows: filter, evaluate, and sort
    processed_rows = process_rows(parsed_sql, row_contexts, table_context, alias_mapping)
    return processed_rows if has_error?(processed_rows)
    
    # Build final result with column names
    build_query_result(processed_rows, parsed_sql)
  end
  
  def build_and_validate_table_context(parsed_sql)
    table_name = parsed_sql[:table_name]
    table_alias = parsed_sql[:table_alias]
    joins = parsed_sql[:joins] || []
    
    build_table_context(table_name, table_alias, joins)
  end
  
  def fetch_row_contexts(parsed_sql, table_context)
    table_name = parsed_sql[:table_name]
    table_alias = parsed_sql[:table_alias]
    joins = parsed_sql[:joins] || []
    
    if joins.empty?
      fetch_simple_row_contexts(table_name, table_alias, table_context)
    else
      execute_joins(table_name, table_alias, joins, table_context)
    end
  end
  
  def fetch_simple_row_contexts(table_name, table_alias, table_context)
    table_info = @table_manager.get_table_info(table_name)
    return { error: 'validation_error' } unless table_info
    
    all_data = @table_manager.get_all_rows(table_name)
    return all_data if has_error?(all_data)
    
    # Build row context for single table
    all_data[:rows].map do |row|
      @row_context_builder.build_single_table_context(row, table_info, table_name, table_alias)
    end
  end
  
  
  def validate_query(parsed_sql, table_context, alias_mapping)
    joins = parsed_sql[:joins] || []
    
    begin
      query_processor = select_query_processor(joins)
      
      if joins.empty?
        dummy_row_data = table_context.build_dummy_row_data
        query_processor.validate_query(parsed_sql, table_context.to_hash, alias_mapping, dummy_row_data)
      else
        query_processor.validate_query(parsed_sql, table_context.to_hash, alias_mapping, nil)
      end
    rescue ExpressionEvaluator::ValidationError
      validation_error
    end
  end
  
  
  
  def process_rows(parsed_sql, row_contexts, table_context, alias_mapping)
    expressions = parsed_sql[:expressions]
    where_clause = parsed_sql[:where]
    group_by = parsed_sql[:group_by]
    order_by = parsed_sql[:order_by]
    limit = parsed_sql[:limit]
    offset = parsed_sql[:offset]
    joins = parsed_sql[:joins] || []
    
    begin
      query_processor = select_query_processor(joins)
      
      # Check if we have aggregate functions in SELECT
      has_aggregates = expressions.any? do |expr_info|
        AggregateEvaluator.new(@evaluator).has_aggregate_functions?(expr_info[:expression])
      end
      
      # Handle GROUP BY or implicit grouping
      if group_by
        result_rows = query_processor.process_grouped_rows(row_contexts, expressions, where_clause, group_by, table_context.to_hash)
        
        # Apply ORDER BY to grouped results
        if order_by
          result_rows = apply_ordering_to_result_rows(result_rows, order_by, alias_mapping, expressions)
        end
      elsif has_aggregates
        # Implicit grouping - treat all rows as a single group
        result_rows = query_processor.process_implicit_group(row_contexts, expressions, where_clause, table_context.to_hash)
        
        # Apply ORDER BY to grouped results
        if order_by
          result_rows = apply_ordering_to_result_rows(result_rows, order_by, alias_mapping, expressions)
        end
      else
        # Filter and evaluate rows
        filtered_rows = query_processor.process_rows(row_contexts, expressions, where_clause, table_context.to_hash)
        
        # Apply ORDER BY
        if order_by
          filtered_rows = apply_ordering(filtered_rows, order_by, alias_mapping, table_context, joins)
        end
        
        # Extract result rows
        result_rows = filtered_rows.map { |row_info| row_info[:result] }
      end
      
      # Apply LIMIT/OFFSET
      if limit || offset
        result_rows = @row_processor.apply_limit_offset(result_rows, limit, offset)
      end
      
      result_rows
    rescue ExpressionEvaluator::DivisionByZeroError
      division_by_zero_error
    rescue ExpressionEvaluator::ValidationError
      validation_error
    end
  end
  
  
  
  def extract_row_data(row_context, table_info)
    @row_context_builder.extract_row_data(row_context, table_info)
  end
  
  def evaluate_select_expressions(expressions, row_data)
    expressions.map do |expr_info|
      value = @evaluator.evaluate(expr_info[:expression], row_data)
      BooleanConverter.convert(value)
    end
  end
  
  def evaluate_select_expressions_with_context(expressions, row_context, table_context)
    expressions.map do |expr_info|
      value = @evaluator.evaluate_with_context(expr_info[:expression], row_context, table_context)
      BooleanConverter.convert(value)
    end
  end
  
  def apply_ordering(filtered_rows, order_by, alias_mapping, table_context, joins)
    if joins.empty?
      table_info = table_context.primary_table_info
      @row_sorter.sort_rows(filtered_rows, order_by, alias_mapping, table_info[:columns])
    else
      @row_sorter.sort_rows_with_context(filtered_rows, order_by, alias_mapping, table_context.to_hash)
    end
  end
  
  def apply_ordering_to_result_rows(result_rows, order_by, alias_mapping, expressions)
    # Create synthetic row data from result rows for ordering
    rows_with_data = result_rows.map do |row|
      row_data = {}
      expressions.each_with_index do |expr_info, idx|
        if expr_info[:alias]
          row_data[expr_info[:alias]] = row[idx]
        end
      end
      { result: row, row_data: row_data }
    end
    
    # Sort using the row sorter
    sorted_rows = @row_sorter.sort_result_rows(rows_with_data, order_by, alias_mapping)
    
    # Extract just the result rows
    sorted_rows.map { |row_info| row_info[:result] }
  end
  
  def build_query_result(result_rows, parsed_sql)
    column_names = @query_planner.extract_column_names(parsed_sql[:expressions])
    { rows: result_rows, columns: column_names }
  end
  
  def select_query_processor(joins)
    joins.empty? ? @simple_query_processor : @join_query_processor
  end
  
  private
  
  def build_table_context(from_table, from_alias, joins)
    context = TableContext.new
    
    # Add FROM table
    table_info = @table_manager.get_table_info(from_table)
    return validation_error unless table_info
    
    context.add_table(from_table, table_info, from_alias)
    
    # Check for duplicate table names
    seen_aliases = Set.new([from_alias || from_table])
    
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
      context.add_table(join[:table], join_table_info, join[:alias])
    end
    
    context
  end
  
  def execute_joins(from_table, from_alias, joins, table_context)
    # Get rows from FROM table
    from_table_info = table_context.get_table_info(from_table)
    from_data = @table_manager.get_all_rows(from_table)
    return from_data if has_error?(from_data)
    
    actual_from_alias = from_alias || from_table
    
    # Build initial row contexts from FROM table
    row_contexts = from_data[:rows].map do |row|
      @row_context_builder.build_single_table_context(row, from_table_info, from_table, from_alias)
    end
    
    # Process each JOIN
    joins.each do |join|
      join_table = join[:table]
      join_alias = join[:alias] || join_table
      join_type = join[:type]
      join_condition = join[:on]
      
      # Get rows from joined table
      join_table_info = table_context.get_table_info(join_table)
      join_data = @table_manager.get_all_rows(join_table)
      return join_data if has_error?(join_data)
      
      # Perform the join
      new_row_contexts = []
      
      row_contexts.each do |left_context|
        matched = false
        
        join_data[:rows].each do |join_row|
          # Build context for joined row
          join_context = @row_context_builder.build_join_context(left_context, join_row, join_table_info, join_table, join_alias)
          
          # Evaluate JOIN condition
          begin
            condition_result = @evaluator.evaluate_with_context(join_condition, join_context, table_context.to_hash)
            
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
            join_context = @row_context_builder.build_join_context(left_context, nil, join_table_info, join_table, join_alias)
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
            join_context = @row_context_builder.build_join_context(left_context, join_row, join_table_info, join_table, join_alias)
            
            begin
              condition_result = @evaluator.evaluate_with_context(join_condition, join_context, table_context.to_hash)
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
      validation = @validator.validate_expression_with_context(expr_info[:expression], table_context.to_hash)
      return validation if has_error?(validation)
    end
    
    # Validate WHERE clause if present
    if parsed_sql[:where]
      validation = @validator.validate_expression_with_context(parsed_sql[:where], table_context.to_hash)
      return validation if has_error?(validation)
    end
    
    # Validate ORDER BY if present
    if parsed_sql[:order_by]
      # Check if it's an alias reference first
      order_expr = parsed_sql[:order_by][:expression]
      if order_expr[:type] == :column && alias_mapping[order_expr[:name]]
        # Validate the aliased expression instead
        validation = @validator.validate_expression_with_context(alias_mapping[order_expr[:name]], table_context.to_hash)
      else
        validation = @validator.validate_expression_with_context(order_expr, table_context.to_hash)
      end
      return validation if has_error?(validation)
    end
    
    # Validate JOIN conditions - these must check for boolean type
    if parsed_sql[:joins]
      parsed_sql[:joins].each do |join|
        validation = @validator.validate_expression_with_context(join[:on], table_context.to_hash)
        return validation if has_error?(validation)
        
        # Check that JOIN condition evaluates to boolean
        join_type = @validator.get_expression_type_with_context(join[:on], table_context.to_hash)
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