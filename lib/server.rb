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
    if result[:error]
      { status: 'error', error_type: result[:error] }.to_json
    elsif result[:status]
      { status: result[:status] }.to_json
    elsif result[:rows].nil?
      { status: 'ok' }.to_json
    elsif result[:rows].empty?
      response = { status: 'ok', rows: [] }
      response[:column_names] = result[:columns] if result[:columns]
      response.to_json
    else
      formatted = {
        status: 'ok',
        rows: result[:rows]
      }
      formatted[:column_names] = result[:columns] if result[:columns]
      formatted.to_json
    end
  end
end