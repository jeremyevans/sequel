module NotNaughty
  
  # == Validates length of obj's attribute via the <tt>:length</tt> method.
  #
  # Unless the validation succeeds an error hash (:attribute => :message)
  # is added to the obj's instance of Errors.
  #
  # <b>Options:</b>
  # <tt>:message</tt>:: see NotNaughty::Errors for details
  # <tt>:if</tt>::      see NotNaughty::Validation::Condition for details
  # <tt>:unless</tt>::  see NotNaughty::Validation::Condition for details
  #
  # <b>Boundaries (by precendence):</b>
  # <tt>:is</tt>::      valid length
  # <tt>:within</tt>::  valid range of length
  # <tt>:minimum</tt>:: maximum length
  # <tt>:maximum</tt>:: minimum length
  #
  # If both, <tt>:minimum</tt> and <tt>:maximum</tt> are provided they're
  # converted to :within.  Each boundary type has its own default message:
  # precise:: "Length of %s is not equal to #{__length}."
  # range:: "Length of %s is not within #{__range.first} and #{__range.last}."
  # lower:: "Length of %s is smaller than #{__boundary}."
  # upper:: "Length of %s is greater than #{__boundary}."
  #
  # <b>Example:</b>
  #
  #   obj = %w[a sentence with five words] # 
  #   def obj.errors() @errors ||= NotNauthy::Errors.new end
  #
  #   LengthValidation.new({:minimum => 4}, :to_a).
  #     call obj, :to_a, %w[a sentence with five words]
  #   obj.errors.on(:to_s).any? # => false
  #
  #   LengthValidation.new({:within => 1..4}, :to_a).
  #     call obj, :to_a, %w[a sentence with five words]
  #   obj.errors.on(:to_s).any? # => true
  class LengthValidation < Validation
    
    def initialize(opts, attributes) #:nodoc:
      
      block = build_block opts
      
      if opts[:allow_blank]
        super opts, attributes do |o, a, v|
          block[o, a, v] unless v.blank?
        end
      else
        super opts, attributes do |o, a, v|
          block[o, a, v] unless v.nil?
        end
      end
    end
    
    protected
    def build_block(opts) #:nodoc:
      if __length = opts[:is]
        __message = opts[:message] ||
          "Length of %s is not equal to #{__length}."
        proc do |o, a, v|
          o.errors.add a, __message unless __length.eql? v.length
        end
      elsif opts[:within] or opts[:minimum] && opts[:maximum]
        __range   = opts[:within]
        __range ||= Range.new opts[:minimum], opts[:maximum]
        
        __message = opts[:message] ||
          "Length of %s is not within #{__range.first} and #{__range.last}."
        
        proc do |o, a, v|
          o.errors.add a, __message unless __range.include? v.length
        end
      elsif opts[:minimum]
        __boundary  = opts[:minimum]
        __message   = opts[:message] ||
          "Length of %s is smaller than #{__boundary}."
        
        proc do |o, a, v|
          o.errors.add a, __message unless __boundary <= v.length
        end
      elsif opts[:maximum]
        __boundary  = opts[:maximum]
        __message   = opts[:message] ||
          "Length of %s is greater than #{__boundary}."
        
        proc do |o, a, v|
          o.errors.add a, __message unless __boundary >= v.length
        end
      else
        raise ArgumentError, 'no boundary given'
      end
    end
    
  end
end
