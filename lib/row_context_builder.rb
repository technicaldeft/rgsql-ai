class RowContextBuilder
  def build_single_table_context(row, table_info, table_name, table_alias = nil)
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
  
  def build_join_context(left_context, join_row, join_table_info, join_table_name, join_alias = nil)
    # Start with the left context
    context = left_context.dup
    
    # Add columns from the joined table
    join_table_info[:columns].each_with_index do |col_info, idx|
      context[col_info[:name]] = join_row ? join_row[idx] : nil
      # Also add qualified names
      if join_alias
        context["#{join_alias}.#{col_info[:name]}"] = join_row ? join_row[idx] : nil
      else
        context["#{join_table_name}.#{col_info[:name]}"] = join_row ? join_row[idx] : nil
      end
    end
    
    context
  end
end