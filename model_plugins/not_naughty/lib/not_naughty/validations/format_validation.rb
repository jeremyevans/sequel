module NotNaughty
  class FormatValidation < Validation
    
    def initialize(opts, attributes)
      (__format = opts[:with]).respond_to? :match or
      raise ArgumentError, "#{__format.inspect} doesn't respond to :match"
      
      __message = opts[:message] || 'Format of %s does not match.'
      
      if opts[:allow_blank] or opts[:allow_nil]
        __allow = if opts[:allow_blank] then :blank? else :nil? end
        super opts, attributes do |o, a, v|
          o.errors.add a, __message unless v.send! __allow or __format.match v
        end
      else
        super opts, attributes do |o, a, v|
          o.errors.add a, __message unless __format.match v
        end
      end
    end
    
  end
end
