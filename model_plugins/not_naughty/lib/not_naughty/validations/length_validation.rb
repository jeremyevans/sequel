module NotNaughty
  class LengthValidation < Validation
    
    def initialize(opts, attributes)
      
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
    def build_block(opts)
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
