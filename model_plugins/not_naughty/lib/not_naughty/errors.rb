module NotNaughty
  
  # == Container for failed validations.
  #
  # ...
  class Errors
    extend Forwardable
    
    def_delegators :@errors, :empty?, :clear, :[], :each, :to_yaml
    
    include Enumerable
    
    def initialize() #:nodoc:
      @errors = Hash.new {|h, k| h[k] = []}
    end
    # Adds an error for the given attribute.
    def add(k, msg)  @errors[k] << msg end
    
    # Returns an array of fully-formatted error messages.
    def full_messages
      @errors.inject([]) do |messages, k_errors| k, errors = *k_errors
        errors.each {|e| messages << eval(e.inspect.delete('\\') % k) }
        messages
      end
    end
    
    # Returns an array of evaluated error messages for given attribute/
    def on(attribute)
      @errors[attribute].map do |message|
        eval(message.inspect.delete('\\') % attribute)
      end
    end
    
    # Returns a ValidationException with <tt>self</tt> in <tt>:errors</tt>.
    def to_exception
      ValidationException.new self
    end
  end
  
  # == Exception class for NotNaughty
  #
  # Includes the instance of Errors that caused the Exception.
  class ValidationException < RuntimeError
    extend Forwardable
    
    attr_reader :errors
    def_delegators :@errors, :on, :full_messages, :each
    
    # Returns instance of ValidationError with errors set
    def initialize(errors)
      @errors = errors

      if errors.any?
        super 'validation errors'
      else
        super 'no validation errors'
      end
    end
  end

end
