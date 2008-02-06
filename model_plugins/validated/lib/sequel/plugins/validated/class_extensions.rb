require File.dirname(__FILE__) + '/core_extensions'
require 'delegate'

module Sequel::Plugins::Validated
  module ClassExtensions
    
    def self.extended(base) #:nodoc:
      base.def_hook_method :before_validation
      
      base.instance_variable_set :@validations,
        :create => Hash.new {|h, k| h[k] = []},
        :update => Hash.new {|h, k| h[k] = []}
      base.meta_eval { alias_method_chain :inherited, :validations }
    end
    
    def inherited_with_validations(base) #:nodoc:
      base.instance_variable_set :@validations,
        :create => @validations[:create].dup,
        :update => @validations[:update].dup
      
      inherited_without_validations base
    end
    
    # Returns the validations hash for the class.
    def validations() @validations end
    
    # Returns true if validations are defined.
    def validates(&block)
      Class.new(SimpleDelegator) do
        def method_missing(m, *a) #:nodoc:
          @_sd_obj.send!(:"validates_#{m}", *a)
        end
      end.new(self).instance_eval(&block)
    end
    
    # Returns true if validations are defined.
    def has_validations?
      @validations[:create].any? || @validations[:update].any? ||
      ( instance_methods &
        # %w[validate validate_on_create validate_on_update] ).any?
        %w[validate_on_create validate_on_update] ).any?
    end
    
    # Validates the given instance.
    def run_validations(object)
      # object.send! :validate if object.respond_to? :validate
      
      create_or_update = if object.new? then :create else :update end
      
      object.respond_to? :"validate_on_#{create_or_update}" and
      object.send! :"validate_on_#{create_or_update}"
      
      @validations[create_or_update].each do |attribute, validations|
        value = object.send! attribute
        validations.each {|validates| validates[object, attribute, value]}
      end
    end
    
    # Adds a validation for each of the given attributes using the supplied
    # block. The block must accept three arguments: instance, attribute and 
    # value, e.g.:
    #
    #   validates_each :name, :password do |object, attribute, value|
    #     object.errors[attribute] << 'is not nice' unless value.nice?
    #   end
    def validates_each(*attributes, &block)
      options = attributes.extract_options!
      
      if options[:if].is_a? Proc and options[:if].is_a? Proc
        block = proc do |o, a, v|
          block[o, a, v] if options[:if][o] or not options[:unless][o]
        end
      elsif options[:if].is_a? Proc
        block = proc {|o, a, v| block[o, a, v] if options[:if][o] }
      elsif options[:unless].is_a? Proc
        block = proc {|o, a, v| block[o, a, v] unless options[:unless][o] }
      end
      
      unless update_or_create = options[:on]
        attributes.each do |attribute|
          @validations[:create][attribute] << block
          @validations[:update][attribute] << block
        end
      else
        attributes.each {|a| @validations[update_or_create][a] << block}
      end
    end
    
    # Validates acceptance of an attribute.
    def validates_acceptance_of(*p)
      opts = p.extract_options
      __blank, __nil, __message, __accept =
        opts[:allow_blank], opts.fetch(:allow_nil, true),
        opts[:message] || 'is not accepted',
        opts[:accept] || '1'
      
      validates_each(*p) do |o, a, v|
        o.errors.on(a) << __message unless
          v.blank? && __blank or v.nil? && __nil or v == __accept
      end
    end
    
    # Validates confirmation of an attribute.
    def validates_confirmation_of(*p)
      opts = p.extract_options
      __blank, __nil, __message =
        opts[:allow_blank], opts[:allow_nil],
        opts[:message] || 'is not confirmed'
      
      validates_each(*p) do |o, a, v|
        o.errors.on(a) << __message unless
          v.blank? && __blank or
          v.nil? && __nil or
          v == o.send!(:"#{a}_confirmation")
      end
    end

    # Validates the length of an attribute.
    def validates_length_of(*p)
      opts = p.extract_options
      __too_long, __too_short, __wrong_length,
      __blank, __nil, __maximum, __minimum, __is, __within =
        opts[:message] || opts[:too_long] || 'is too long',
        opts[:message] || opts[:too_short] || 'is too short',
        opts[:message] || opts[:wrong_length] || 'is the wrong length',
        opts[:allow_blank], opts[:allow_nil],
        opts[:maximum], opts[:minimum], opts[:is], opts[:within]

      validates_each(*p) do |o, a, v|
        unless v.blank? && __blank or v.nil? && __nil
          o.errors.on(a) << __too_long if __maximum and
            !(v && v.length <= __maximum)
          o.errors.on(a) << __too_short if __minimum and
            !(v && v.length >= __minimum)
          o.errors.on(a) << __wrong_length if __is and
            !(v && v.length == __is)
          o.errors.on(a) << __wrong_length if __within and
            !(v && __within.include?(v.length))
        end
      end
    end
    
    # Validates the format of an attribute
    def validates_format_of(*p)
      opts = p.extract_options
      
      unless (__format = opts[:with]).respond_to? :match
        raise ArgumentError, "#{__format.inspect} doesn't respond to :match"
      end
      
      __blank, __nil, __message =
        opts[:allow_blank], opts[:allow_nil],
        opts[:message] || 'is invalid'
      
      validates_each(*p) do |o, a, v|
        o.errors.on(a) << __message unless
          v.blank? && __blank or v.nil? && __nil or __format.match v
      end
    end
    
    # Validates whether an attribute matches a numeric format
    def validates_numericality_of(*p)
      opts = p.extract_options!
      opts[:with] = if opts[:only_integer]
        opts[:message] ||= 'is not an integer'
        /^[+-]?\d+$/
      else
        opts[:message] ||= 'is not a number'
        /^[+-]?\d*\.?\d+$/
      end
      p << opts
      
      validates_format_of(*p)
    end
    
    # Validates the presence of an attribute.
    def validates_presence_of(*p)
      __message = p.extract_options.fetch(:message, 'is not present')
      validates_each(*p) { |o, a, v| o.errors.on(a) << __message if v.blank? }
    end
    
  end
end
