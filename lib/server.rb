require 'socket'
require 'json'
require_relative 'sql_parser'
require_relative 'sql_executor'

class Server
  PORT = 3003
  MESSAGE_DELIMITER = "\0"
  
  def initialize
    @server = TCPServer.new PORT
    @socket = @server.accept
    @parser = SqlParser.new
    @executor = SqlExecutor.new
  end

  def run
    loop do
      message = @socket.gets(MESSAGE_DELIMITER)
      break if message.nil?
      sql = message.chomp(MESSAGE_DELIMITER)
    
      parsed = @parser.parse(sql)
      result = @executor.execute(parsed)
      response = format_response(result)
    
      @socket.print response
      @socket.print(MESSAGE_DELIMITER)
    end
  end
  
  private
  
  def format_response(result)
    response = build_response(result)
    response.to_json
  end
  
  def build_response(result)
    return { status: 'error', error_type: result[:error] } if result[:error]
    return { status: result[:status] } if result[:status]
    
    response = { status: 'ok' }
    
    if result[:rows]
      response[:rows] = result[:rows]
      response[:column_names] = result[:columns] if result[:columns]
    end
    
    response
  end
end