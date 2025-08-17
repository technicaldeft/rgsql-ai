require_relative 'tokenizer'
require_relative 'sql_error'

class Parser
  def initialize
    @tokenizer = Tokenizer.new
  end
  
  def parse(sql)
    tokens = @tokenizer.tokenize(sql)
    return { error: :parsing_error } if tokens.empty?
    
    # Check for trailing content after semicolon
    semicolon_index = tokens.find_index { |t| t.type == :semicolon }
    if semicolon_index && semicolon_index < tokens.length - 1
      return { error: :parsing_error }
    end
    
    # Remove trailing semicolon if present
    tokens.pop if tokens.last&.type == :semicolon
    
    # Check for unknown tokens
    if tokens.any? { |t| t.type == :unknown }
      return { error: :parsing_error }
    end
    
    return { error: :parsing_error } if tokens.empty?
    
    case tokens.first.value
    when 'SELECT'
      parse_select(tokens)
    else
      { error: :parsing_error }
    end
  end
  
  private
  
  def parse_select(tokens)
    tokens.shift # Remove SELECT
    
    # Handle empty SELECT
    if tokens.empty?
      return { type: :select, values: [], columns: [] }
    end
    
    values = []
    columns = []
    
    # Parse comma-separated expressions
    while !tokens.empty?
      # Parse value
      value, column = parse_select_item(tokens)
      return { error: :parsing_error } if value.nil?
      
      values << value
      columns << column
      
      # Check for comma
      break unless tokens.first&.type == :comma
      tokens.shift # Remove comma
      
      # Error if nothing after comma
      return { error: :parsing_error } if tokens.empty?
    end
    
    { type: :select, values: values, columns: columns }
  end
  
  def parse_select_item(tokens)
    return [nil, nil] if tokens.empty?
    
    # Parse the value
    value = parse_value(tokens)
    return [nil, nil] if value.nil?
    
    # Check for AS clause
    if tokens.first&.value == 'AS'
      tokens.shift # Remove AS
      
      # Get column name
      return [nil, nil] if tokens.empty?
      
      name_token = tokens.shift
      return [nil, nil] unless name_token.type == :identifier
      
      # Column names cannot start with a digit
      return [nil, nil] if name_token.value =~ /^\d/
      
      [value, name_token.value]
    else
      [value, nil]
    end
  end
  
  def parse_value(tokens)
    return nil if tokens.empty?
    
    token = tokens.shift
    
    case token.type
    when :integer, :boolean
      token.value
    else
      tokens.unshift(token) # Put it back
      nil
    end
  end
end