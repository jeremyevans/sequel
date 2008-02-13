module NotNaughty
  
  # == Validates presence of obj's attribute via the <tt>:blank?</tt> method.
  #
  # Unless the validation succeeds an error hash (:attribute => :message)
  # is added to the obj's instance of Errors.
  #
  # <b>Options:</b>
  # <tt>:message</tt>:: see NotNaughty::Errors for details
  # <tt>:if</tt>:: see NotNaughty::Validation::Condition for details
  # <tt>:unless</tt>:: see NotNaughty::Validation::Condition for details
  #
  # <b>Example:</b>
  #
  #   obj = '' # blank? => true
  #   def obj.errors() @errors ||= NotNauthy::Errors.new end
  #
  #   PresenceValidation.new({}, :to_s).call obj, :to_s, ''
  #   obj.errors.on(:to_s) # => ["To s is not present."]
  class PresenceValidation < Validation
    
    def initialize(opts, attributes) #:nodoc:
      __message = opts[:message] || '#{"%s".humanize} is not present.'
      
      super opts, attributes do |o, a, v|
        o.errors.add a, __message if v.blank?
      end
    end
    
  end
end
