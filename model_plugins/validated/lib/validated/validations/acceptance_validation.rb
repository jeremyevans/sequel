module Validated
  class AcceptanceValidation < Validation
    
    def initialize(opts, attributes)
      __message, __accept =
        opts[:message] || '#{"%s".humanize} not accepted.',
        opts[:accept] || '1'
      
      if opts[:allow_blank] or opts.fetch(:allow_nil, true)
        __allow = if opts[:allow_blank] then :blank? else :nil? end
        super opts, attributes do |o, a, v|
          o.errors.add a, __message unless v.send! __allow or __accept.eql? v
        end
      else
        super opts, attributes do |o, a, v|
          o.errors.add a, __message unless __accept.eql? v
        end
      end
    end
    
  end
end
