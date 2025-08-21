module SqlConstants
  RESERVED_KEYWORDS = %w[
    SELECT FROM CREATE TABLE DROP INSERT INTO VALUES
    INTEGER BOOLEAN AS IF EXISTS NOT AND OR ABS MOD NULL
  ].freeze
  
  DATA_TYPES = %w[INTEGER BOOLEAN].freeze
  
  STATEMENT_TYPES = {
    select: 'SELECT',
    create: 'CREATE',
    drop: 'DROP',
    insert: 'INSERT'
  }.freeze
  
  ERROR_TYPES = {
    parsing: 'parsing_error',
    validation: 'validation_error',
    division_by_zero: 'division_by_zero_error',
    unknown_command: 'unknown_command'
  }.freeze
  
  TOKEN_TYPES = {
    lparen: :lparen,
    rparen: :rparen,
    comma: :comma,
    plus: :plus,
    minus: :minus,
    star: :star,
    slash: :slash,
    lt: :lt,
    lte: :lte,
    gt: :gt,
    gte: :gte,
    equal: :equal,
    not_equal: :not_equal,
    integer: :integer,
    boolean: :boolean,
    identifier: :identifier,
    not: :not,
    and: :and,
    or: :or,
    abs: :abs,
    mod: :mod,
    as: :as,
    null: :null,
    dot: :dot
  }.freeze
  
  OPERATORS = {
    plus: :plus,
    minus: :minus,
    star: :star,
    slash: :slash,
    lt: :lt,
    gt: :gt,
    lte: :lte,
    gte: :gte,
    equal: :equal,
    not_equal: :not_equal,
    and: :and,
    or: :or,
    not: :not
  }.freeze
  
  PATTERNS = {
    identifier: /[a-zA-Z_][a-zA-Z0-9_]*/,
    integer: /-?\d+/,
    whitespace: /\s+/,
    optional_whitespace: /\s*/,
    optional_semicolon: /;?\s*\z/
  }.freeze
end