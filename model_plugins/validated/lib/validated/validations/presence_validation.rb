module Validated
  class PresenceValidation < Validation
    
    def initialize(opts, attributes)
      __message = opts[:message] || '#{"%s".humanize} is not present.'
      
      super opts, attributes do |o, a, v|
        o.errors.add a, __message if v.blank?
      end
    end
    
  end
end
