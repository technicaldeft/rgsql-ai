require 'json'

class ResponseFormatter
  def format(result)
    if result.is_a?(SqlError)
      error_response(result)
    elsif result[:error]
      error_response(SqlError.new(result[:error]))
    elsif result[:rows]
      success_response(result)
    else
      JSON.generate({ status: 'ok' })
    end
  end
  
  private
  
  def error_response(error)
    JSON.generate({
      status: 'error',
      error_type: error.error_type
    })
  end
  
  def success_response(result)
    response = { 
      status: 'ok', 
      rows: result[:rows] 
    }
    
    response[:column_names] = result[:columns] if result[:columns]
    
    JSON.generate(response)
  end
end