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
end