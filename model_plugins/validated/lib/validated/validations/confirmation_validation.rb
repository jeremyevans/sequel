module Validated
  class ConfirmationValidation < Validation
    
    def initialize(opts, attributes)
      __message = opts[:message] || '#{"%s".humanize} could not be confirmed.'  

      if opts[:allow_blank] or opts[:allow_nil]
        __allow = if opts[:allow_blank] then :blank? else :nil? end
        super opts, attributes do |o, a, v|
          o.errors.add a, __message unless
          v.send! __allow or o.send!(:"#{a}_confirmation").eql? v
        end
      else
        super opts, attributes do |o, a, v|
          o.errors.add a, __message unless
          o.send!(:"#{a}_confirmation").eql? v
        end
      end
    end
    
  end
end
