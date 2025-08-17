class SqlError < StandardError
  attr_reader :error_type
  
  def initialize(error_type, message = nil)
    @error_type = error_type
    super(message || error_type.to_s)
  end
end

class ParsingError < SqlError
  def initialize(message = nil)
    super(:parsing_error, message)
  end
end

class UnknownStatementError < SqlError
  def initialize(message = nil)
    super(:unknown_statement, message)
  end
end