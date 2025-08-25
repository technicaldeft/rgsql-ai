class AggregateFunctionRegistry
  class AggregateFunction
    attr_reader :name, :requires_argument, :argument_type, :return_type, :default_value
    
    def initialize(name:, requires_argument:, argument_type: nil, return_type:, default_value:)
      @name = name
      @requires_argument = requires_argument
      @argument_type = argument_type
      @return_type = return_type
      @default_value = default_value
    end
  end
  
  def self.instance
    @instance ||= new
  end
  
  def initialize
    @functions = {}
    register_default_functions
  end
  
  def register(name, **options)
    @functions[name] = AggregateFunction.new(name: name, **options)
  end
  
  def get(name)
    @functions[name]
  end
  
  def exists?(name)
    @functions.key?(name)
  end
  
  private
  
  def register_default_functions
    # COUNT can take any argument or no argument (COUNT(*))
    register(:count, 
            requires_argument: false,
            argument_type: nil,
            return_type: :integer,
            default_value: 0)
    
    # SUM requires an integer argument
    register(:sum,
            requires_argument: true,
            argument_type: :integer,
            return_type: :integer,
            default_value: nil)
    
    # Future aggregate functions can be added here:
    # register(:min,
    #         requires_argument: true,
    #         argument_type: nil,  # Can work with any comparable type
    #         return_type: nil,     # Same as input type
    #         default_value: nil)
    # 
    # register(:max,
    #         requires_argument: true,
    #         argument_type: nil,
    #         return_type: nil,
    #         default_value: nil)
    # 
    # register(:avg,
    #         requires_argument: true,
    #         argument_type: :integer,
    #         return_type: :integer,
    #         default_value: nil)
  end
end