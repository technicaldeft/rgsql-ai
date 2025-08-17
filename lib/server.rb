require 'socket'
require 'json'
require_relative 'parser'
require_relative 'executor'

class Server
  def initialize
    @server = TCPServer.new 3003
    @socket = @server.accept
    @parser = Parser.new
    @executor = Executor.new
  end

  def run
    loop do
      message = @socket.gets("\0")
      break if message.nil?
      sql = message.chomp("\0")
    
      response = process_query(sql)
    
      @socket.print response
      @socket.print("\0")
    end
  end
  
  private
  
  def process_query(sql)
    parsed = @parser.parse(sql)
    result = @executor.execute(parsed)
    
    format_response(result)
  end
  
  def format_response(result)
    if result[:error]
      JSON.generate({ status: 'error', error_type: result[:error] })
    elsif result[:rows]
      response = { status: 'ok', rows: result[:rows] }
      response[:column_names] = result[:columns] if result[:columns]
      
      JSON.generate(response)
    else
      JSON.generate({ status: 'ok' })
    end
  end
end