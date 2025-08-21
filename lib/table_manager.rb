require_relative 'error_handler'

class TableManager
  include ErrorHandler
  def initialize
    @tables = {}
  end
  
  def create_table(name, columns)
    if @tables.key?(name)
      return validation_error
    end
    
    column_names = columns.map { |col| col[:name] }
    if column_names.uniq.length != column_names.length
      return validation_error
    end
    
    @tables[name] = {
      columns: columns,
      rows: []
    }
    
    ok_status
  end
  
  def drop_table(name, if_exists: false)
    if !@tables.key?(name)
      return if_exists ? ok_status : validation_error
    end
    
    @tables.delete(name)
    ok_status
  end
  
  def table_exists?(name)
    @tables.key?(name)
  end
  
  def get_table(name)
    @tables[name]
  end
  
  def insert_row(table_name, values)
    table = @tables[table_name]
    return { error: 'validation_error' } unless table
    
    if values.length != table[:columns].length
      return validation_error
    end
    
    table[:rows] << values
    ok_status
  end
  
  def select_from_table(table_name, column_names)
    table = @tables[table_name]
    return { error: 'validation_error' } unless table
    
    column_indices = column_names.map do |col_name|
      index = table[:columns].find_index { |col| col[:name] == col_name }
      return validation_error unless index
      index
    end
    
    rows = table[:rows].map do |row|
      column_indices.map { |i| row[i] }
    end
    
    { rows: rows, columns: column_names }
  end
  
  def get_table_info(table_name)
    @tables[table_name]
  end
  
  def get_all_rows(table_name)
    table = @tables[table_name]
    return { error: 'validation_error' } unless table
    
    { rows: table[:rows] }
  end
end