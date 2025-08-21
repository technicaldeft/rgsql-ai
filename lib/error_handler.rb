require_relative 'sql_constants'

module ErrorHandler
  include SqlConstants
  
  def validation_error
    { error: ERROR_TYPES[:validation] }
  end
  
  def parsing_error
    { error: ERROR_TYPES[:parsing] }
  end
  
  def division_by_zero_error
    { error: ERROR_TYPES[:division_by_zero] }
  end
  
  def unknown_command_error
    { error: ERROR_TYPES[:unknown_command] }
  end
  
  def has_error?(result)
    result.is_a?(Hash) && result[:error]
  end
  
  def is_error?(result)
    has_error?(result)
  end
  
  def error_with_type(type)
    { error: type }
  end
  
  def ok_status
    { status: 'ok' }
  end
end