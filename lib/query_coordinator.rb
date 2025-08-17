require_relative 'parser'
require_relative 'executor'
require_relative 'sql_error'

class QueryCoordinator
  def initialize
    @parser = Parser.new
    @executor = Executor.new
  end
  
  def process(sql)
    parsed = @parser.parse(sql)
    @executor.execute(parsed)
  rescue SqlError => e
    e
  rescue StandardError => e
    SqlError.new(:internal_error, e.message)
  end
end