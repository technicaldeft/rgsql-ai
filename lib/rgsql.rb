require 'server'

class RgSql
  def self.start_server
    Server.new.run
  end
end