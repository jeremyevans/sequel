module NotNaughty
  
  # == Validates format of obj's attribute via the <tt>:match</tt> method.
  #
  # Unless the validation succeeds an error hash (:attribute => :message)
  # is added to the obj's instance of Errors.
  #
  # <b>Options:</b>
  # <tt>:with</tt>::   object that that'll check via a <tt>:match</tt> call
  # <tt>:message</tt>:: see NotNaughty::Errors for details
  # <tt>:if</tt>::      see NotNaughty::Validation::Condition for details
  # <tt>:unless</tt>::  see NotNaughty::Validation::Condition for details
  #
  # <b>Example:</b>
  #
  #   obj = 'abc'
  #   def obj.errors() @errors ||= NotNauthy::Errors.new end
  #
  #   FormatValidation.new({:with => /[a-z]/}, :to_s).call obj, :to_s, 'abc'
  #   obj.errors.on(:to_s).any? # => false
  #
  #   FormatValidation.new({:with => /[A-Z]/}, :to_s).call obj, :to_s, 'abc'
  #   obj.errors.on(:to_s) # => ["Format of to_s does not match."]
  class FormatValidation < Validation
    
    def initialize(opts, attributes) #:nodoc:
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
