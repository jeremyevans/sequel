# The Validations module provides validation capabilities as a mixin. When
# included into a class, it enhances the class with class and instance
# methods for defining validations and validating class instances.
# 
# The Validation emulates the validation capabilities of ActiveRecord, and
# provides methods for validating acceptance, confirmation, presence, format, 
# length and numericality of attributes.
#
# To use validations, you need to include the Validation module in your
# class:
#
#   class MyClass
#     include Validation
#     validates_length_of :password, :minimum => 6
#   end
module Validation
  # Includes the Validation class methods into the including class.
  def self.included(c)
    c.extend ClassMethods
  end
  
  # Returns the validation errors associated with the object.
  def errors
    @errors ||= Errors.new
  end

  # Validates the object.
  def validate
    errors.clear
    before_validation
    self.class.validate(self)
    after_validation
  end

  # Validates the object and returns true if no errors are reported.
  def valid?
    validate
    errors.empty?
  end

  # Validation::Errors represents validation errors.
  class Errors
    # Initializes a new instance of validation errors.
    def initialize
      @errors = Hash.new {|h, k| h[k] = []}
    end
    
    # Returns true if no errors are stored.
    def empty?
      @errors.empty?
    end
    
    # Clears all errors.
    def clear
      @errors.clear
    end
    
    # Returns size of errors array
    def size
      @errors.size
    end
    
    # Iterates over errors
    def each(&block)
      @errors.each(&block)
    end
    
    # Returns the errors for the given attribute.
    def on(att)
      @errors[att]
    end
    alias_method :[], :on
    
    # Adds an error for the given attribute.
    def add(att, msg)
      @errors[att] << msg
    end
    
    # Returns an array of fully-formatted error messages.
    def full_messages
      @errors.inject([]) do |m, kv| att, errors = *kv
        errors.each {|e| m << "#{att} #{e}"}
        m
      end
    end
  end
  
  # The Generator class is used to generate validation definitions using 
  # the validates {} idiom.
  class Generator
    # Initializes a new generator.
    def initialize(receiver ,&block)
      @receiver = receiver
      instance_eval(&block)
    end

    # Delegates method calls to the receiver by calling receiver.validates_xxx.
    def method_missing(m, *args, &block)
      @receiver.send(:"validates_#{m}", *args, &block)
    end
  end
  
  # Validation class methods.
  module ClassMethods
    # Defines validations by converting a longhand block into a series of 
    # shorthand definitions. For example:
    #
    #   class MyClass
    #     include Validation
    #     validates do
    #       length_of :name, :minimum => 6
    #       length_of :password, :minimum => 8
    #     end
    #   end
    #
    # is equivalent to:
    #   class MyClass
    #     include Validation
    #     validates_length_of :name, :minimum => 6
    #     validates_length_of :password, :minimum => 8
    #   end
    def validates(&block)
      Generator.new(self, &block)
    end

    # Returns the validations hash for the class.
    def validations
      @validations ||= Hash.new {|h, k| h[k] = []}
    end

    # Returns true if validations are defined.
    def has_validations?
      !validations.empty?
    end

    # Validates the given instance.
    def validate(o)
      if superclass.respond_to?(:validate) && !@skip_superclass_validations
        superclass.validate(o)
      end
      validations.each do |att, procs|
        v = o.send(att)
        procs.each {|p| p[o, att, v]}
      end
    end
    
    def skip_superclass_validations
      @skip_superclass_validations = true
    end
    
    # Adds a validation for each of the given attributes using the supplied
    # block. The block must accept three arguments: instance, attribute and 
    # value, e.g.:
    #
    #   validates_each :name, :password do |object, attribute, value|
    #     object.errors[attribute] << 'is not nice' unless value.nice?
    #   end
    def validates_each(*atts, &block)
      atts.each {|a| validations[a] << block}
    end

    # Validates acceptance of an attribute.
    def validates_acceptance_of(*atts)
      opts = {
        :message => 'is not accepted',
        :allow_nil => true,
        :accept => '1'
      }.merge!(atts.extract_options!)
      
      validates_each(*atts) do |o, a, v|
        next if (v.nil? && opts[:allow_nil]) || (v.blank? && opts[:allow_blank])
        o.errors[a] << opts[:message] unless v == opts[:accept]
      end
    end

    # Validates confirmation of an attribute.
    def validates_confirmation_of(*atts)
      opts = {
        :message => 'is not confirmed',
      }.merge!(atts.extract_options!)
      
      validates_each(*atts) do |o, a, v|
        next if (v.nil? && opts[:allow_nil]) || (v.blank? && opts[:allow_blank])
        c = o.send(:"#{a}_confirmation")
        o.errors[a] << opts[:message] unless v == c
      end
    end

    # Validates the format of an attribute.
    def validates_format_of(*atts)
      opts = {
        :message => 'is invalid',
      }.merge!(atts.extract_options!)
      
      unless opts[:with].is_a?(Regexp)
        raise ArgumentError, "A regular expression must be supplied as the :with option of the options hash"
      end
      
      validates_each(*atts) do |o, a, v|
        next if (v.nil? && opts[:allow_nil]) || (v.blank? && opts[:allow_blank])
        o.errors[a] << opts[:message] unless v.to_s =~ opts[:with]
      end
    end

    # Validates the length of an attribute.
    def validates_length_of(*atts)
      opts = {
        :too_long     => 'is too long',
        :too_short    => 'is too short',
        :wrong_length => 'is the wrong length'
      }.merge!(atts.extract_options!)
      
      validates_each(*atts) do |o, a, v|
        next if (v.nil? && opts[:allow_nil]) || (v.blank? && opts[:allow_blank])
        if m = opts[:maximum]
          o.errors[a] << (opts[:message] || opts[:too_long]) unless v && v.size <= m
        end
        if m = opts[:minimum]
          o.errors[a] << (opts[:message] || opts[:too_short]) unless v && v.size >= m
        end
        if i = opts[:is]
          o.errors[a] << (opts[:message] || opts[:wrong_length]) unless v && v.size == i
        end
        if w = opts[:within]
          o.errors[a] << (opts[:message] || opts[:wrong_length]) unless v && w.include?(v.size)
        end
      end
    end

    NUMBER_RE = /^\d*\.{0,1}\d+$/
    INTEGER_RE = /\A[+-]?\d+\Z/

    # Validates whether an attribute is a number.
    def validates_numericality_of(*atts)
      opts = {
        :message => 'is not a number',
      }.merge!(atts.extract_options!)
      
      re = opts[:only_integer] ? INTEGER_RE : NUMBER_RE
      
      validates_each(*atts) do |o, a, v|
        next if (v.nil? && opts[:allow_nil]) || (v.blank? && opts[:allow_blank])
        o.errors[a] << opts[:message] unless v.to_s =~ re
      end
    end

    # Validates the presence of an attribute.
    def validates_presence_of(*atts)
      opts = {
        :message => 'is not present',
      }.merge!(atts.extract_options!)
      
      validates_each(*atts) do |o, a, v|
        o.errors[a] << opts[:message] unless v && !v.blank?
      end
    end

    # Validates only if the fields in the model (specified by atts) are
    # unique in the database.  You should also add a unique index in the
    # database, as this suffers from a fairly obvious race condition.
    def validates_uniqueness_of(*atts)
      opts = {
        :message => 'is already taken',
      }.merge!(atts.extract_options!)

      validates_each(*atts) do |o, a, v|
        next if v.blank? 
        num_dups = o.class.filter(a => v).count
        allow = if num_dups == 0
          # No unique value in the database
          true
        elsif num_dups > 1
          # Multiple "unique" values in the database!!
          # Someone didn't add a unique index
          false
        elsif o.new?
          # New record, but unique value already exists in the database
          false
        elsif o.class[a => v].pk == o.pk
          # Unique value exists in database, but for the same record, so the update won't cause a duplicate record
          true
        else
          false
        end
        o.errors[a] << opts[:message] unless allow
      end
    end
  end
end
