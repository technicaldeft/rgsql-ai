require 'socket'
require_relative 'query_coordinator'
require_relative 'response_formatter'

class Server
  def initialize
    @server = TCPServer.new 3003
    @socket = @server.accept
    @coordinator = QueryCoordinator.new
    @formatter = ResponseFormatter.new
  end

  def run
    loop do
      message = @socket.gets("\0")
      break if message.nil?
      
      sql = message.chomp("\0")
      result = @coordinator.process(sql)
      response = @formatter.format(result)
      
      @socket.print response
      @socket.print("\0")
    end
  end
end