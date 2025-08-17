class Tokenizer
  Token = Struct.new(:type, :value, :position)
  
  KEYWORDS = %w[SELECT AS TRUE FALSE].freeze
  
  def tokenize(sql)
    sql = sql.strip
    tokens = []
    position = 0
    
    while position < sql.length
      # Skip whitespace
      if sql[position].match?(/\s/)
        position += 1
        next
      end
      
      # Handle semicolon
      if sql[position] == ';'
        tokens << Token.new(:semicolon, ';', position)
        position += 1
        next
      end
      
      # Handle comma
      if sql[position] == ','
        tokens << Token.new(:comma, ',', position)
        position += 1
        next
      end
      
      # Handle negative numbers
      if sql[position] == '-' && position + 1 < sql.length && sql[position + 1].match?(/\d/)
        start = position
        position += 1
        while position < sql.length && sql[position].match?(/\d/)
          position += 1
        end
        value = sql[start...position].to_i
        tokens << Token.new(:integer, value, start)
        next
      end
      
      # Handle positive numbers
      if sql[position].match?(/\d/)
        start = position
        while position < sql.length && sql[position].match?(/\d/)
          position += 1
        end
        value = sql[start...position].to_i
        tokens << Token.new(:integer, value, start)
        next
      end
      
      # Handle identifiers and keywords
      if sql[position].match?(/[a-zA-Z_]/)
        start = position
        while position < sql.length && sql[position].match?(/[a-zA-Z0-9_]/)
          position += 1
        end
        word = sql[start...position]
        
        if KEYWORDS.include?(word.upcase)
          type = word.upcase == 'TRUE' || word.upcase == 'FALSE' ? :boolean : :keyword
          value = case word.upcase
                  when 'TRUE' then true
                  when 'FALSE' then false
                  else word.upcase
                  end
          tokens << Token.new(type, value, start)
        else
          tokens << Token.new(:identifier, word, start)
        end
        next
      end
      
      # Unknown character
      tokens << Token.new(:unknown, sql[position], position)
      position += 1
    end
    
    tokens
  end
end