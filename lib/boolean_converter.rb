module BooleanConverter
  BOOLEAN_TRUE = 'TRUE'
  BOOLEAN_FALSE = 'FALSE'
  
  def self.convert(value)
    case value
    when BOOLEAN_TRUE
      true
    when BOOLEAN_FALSE
      false
    else
      value
    end
  end
end