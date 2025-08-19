module ParsingUtils
  def split_on_comma(text)
    return [] if text.strip.empty?
    
    parts = []
    current = ""
    depth = 0
    
    text.chars.each do |char|
      if char == ',' && depth == 0
        parts << current.strip
        current = ""
      else
        current += char
        depth += 1 if char == '('
        depth -= 1 if char == ')'
      end
    end
    
    parts << current.strip unless current.strip.empty?
    parts
  end
  
  def extract_parenthesized_groups(text)
    groups = []
    current_group = ""
    depth = 0
    in_group = false
    
    text.chars.each do |char|
      if char == '(' && depth == 0
        in_group = true
        depth = 1
        current_group = ""
      elsif char == '(' && in_group
        depth += 1
        current_group += char
      elsif char == ')' && depth == 1
        groups << current_group
        in_group = false
        depth = 0
      elsif char == ')' && in_group
        depth -= 1
        current_group += char
      elsif in_group
        current_group += char
      end
    end
    
    groups
  end
  
  def parse_error
    { error: PARSING_ERROR }
  end
  
  def is_error?(result)
    result.is_a?(Hash) && result[:error]
  end
end