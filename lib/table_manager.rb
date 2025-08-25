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
    
    # Allow fewer values than columns - pad with NULLs
    if values.length > table[:columns].length
      return validation_error
    end
    
    # Pad values with nil (NULL) if fewer values than columns
    padded_values = values + Array.new(table[:columns].length - values.length, nil)
    
    table[:rows] << padded_values
    ok_status
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