module NotNaughty
  
  # == Validates acceptance of obj's attribute against a fixed value.
  #
  # Unless the validation succeeds an error hash (:attribute => :message)
  # is added to the obj's instance of Errors.
  #
  # <b>Options:</b>
  # <tt>:accept</tt>::   object that that'll check via a <tt>:match</tt> call
  # <tt>:message</tt>:: see NotNaughty::Errors for details
  # <tt>:if</tt>::      see NotNaughty::Validation::Condition for details
  # <tt>:unless</tt>::  see NotNaughty::Validation::Condition for details
  #
  # <b>Example:</b>
  #
  #   obj = 'abc'
  #   def obj.errors() @errors ||= NotNauthy::Errors.new end
  #
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
