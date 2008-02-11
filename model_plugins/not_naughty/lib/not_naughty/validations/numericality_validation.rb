module NotNaughty
  class NumericalityValidation < FormatValidation
    
    def initialize(opts, attributes)
      opts[:with] = if opts[:only_integer]
        opts[:message] ||= '#{"%s".humanize} is not an integer.'
        /^[+-]?\d+$/
      else
        opts[:message] ||= '#{"%s".humanize} is not a number.'
        /^[+-]?\d*\.?\d+$/
      end
  
      super opts, attributes
    end
    
  end
end
