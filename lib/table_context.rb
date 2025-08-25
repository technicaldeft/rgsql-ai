class TableContext
  attr_reader :tables, :aliases
  
  def initialize
    @tables = {}
    @aliases = {}
  end
  
  def add_table(table_name, table_info, alias_name = nil)
    actual_alias = alias_name || table_name
    @tables[table_name] = table_info
    @aliases[actual_alias] = table_name
  end
  
  def primary_table
    @tables.keys.first
  end
  
  def primary_table_info
    @tables.values.first
  end
  
  def resolve_alias(name)
    @aliases[name]
  end
  
  def get_table_info(name_or_alias)
    table_name = @aliases[name_or_alias] || name_or_alias
    @tables[table_name]
  end
  
  def table_exists?(name_or_alias)
    @aliases.key?(name_or_alias)
  end
  
  def find_column(column_name)
    matching_tables = []
    
    @aliases.each do |alias_name, table_name|
      table_info = @tables[table_name]
      if table_info[:columns].any? { |col| col[:name] == column_name }
        matching_tables << { alias: alias_name, table: table_name, info: table_info }
      end
    end
    
    matching_tables
  end
  
  def validate_column(table_ref, column_name)
    actual_table = @aliases[table_ref]
    return false unless actual_table
    
    table_info = @tables[actual_table]
    table_info[:columns].any? { |col| col[:name] == column_name }
  end
  
  def column_type(table_ref, column_name)
    actual_table = @aliases[table_ref] || table_ref
    return nil unless actual_table
    
    table_info = @tables[actual_table]
    return nil unless table_info
    
    col_info = table_info[:columns].find { |col| col[:name] == column_name }
    col_info ? col_info[:type].downcase.to_sym : nil
  end
  
  def build_dummy_row_data
    dummy_data = {}
    
    @tables.values.first[:columns].each do |col|
      dummy_data[col[:name]] = case col[:type]
      when 'INTEGER'
        0
      when 'BOOLEAN'
        false
      else
        nil
      end
    end
    
    dummy_data
  end
  
  def to_hash
    { tables: @tables, aliases: @aliases }
  end
  
  def self.from_hash(hash)
    context = new
    context.instance_variable_set(:@tables, hash[:tables])
    context.instance_variable_set(:@aliases, hash[:aliases])
    context
  end
end