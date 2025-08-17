require 'socket'
require 'json'

class Server
  def initialize
    @server = TCPServer.new 3003
    @socket = @server.accept
  end

  def run
    loop do
      message = @socket.gets("\0")
      break if message.nil?
      sql = message.chomp("\0")
    
      response = "" # TODO - run statement and build response
    
      @socket.print response
      @socket.print("\0")
    end
  end
end