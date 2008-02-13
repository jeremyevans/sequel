module NotNaughty
  
  # == Validates confirmaton of obj's attribute via <tt>:eql?</tt> method against the _appropiate_ confirmation attribute.
  #
  # Unless the validation succeeds an error hash (:attribute => :message)
  # is added to the obj's instance of Errors.
  #
  # <b>Options:</b>
  # <tt>:message</tt>:: see NotNaughty::Errors for details
  # <tt>:if</tt>::      see NotNaughty::Validation::Condition for details
  # <tt>:unless</tt>::  see NotNaughty::Validation::Condition for details
  #
  # <b>Example:</b>
  #
  #   obj = 'abc'
  #   def obj.errors() @errors ||= NotNauthy::Errors.new end
  #   def obj.to_s_confirmation() '123 end
  #
  #   ConfirmationValidation.new({}, :to_s).call obj, :to_s, 'abc'
  #   obj.errors.on(:to_s).any? # => true
  class ConfirmationValidation < Validation
    
    def initialize(opts, attributes) #:nodoc:
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
