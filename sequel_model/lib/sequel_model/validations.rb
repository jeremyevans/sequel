module Sequel
  module Validatable
    class Errors
      def initialize
        @errors = Hash.new {|h, k| h[k] = []}
      end
      
      def empty?
        @errors.empty?
      end
      
      def clear
        @errors.clear
      end
      
      def on(att)
        @errors[att]
      end
      alias_method :[], :on
      
      def add(att, msg)
        @errors[att] << msg
      end
      
      def full_messages
        @errors.inject([]) do |m, kv| att, errors = *kv
          errors.each {|e| m << "#{att} #{e}"}
          m
        end
      end
    end
    
    class Generator
      def initialize(receiver ,&block)
        @receiver = receiver
        instance_eval(&block)
      end

      def method_missing(m, *args)
        @receiver.send(:"validates_#{m}", *args)
      end
    end
    
    module ClassMethods
      def validates(&block)
        Generator.new(self, &block)
      end

      def validations
        @validations ||= Hash.new {|h, k| h[k] = []}
      end
      
      def has_validations?
        !validations.empty?
      end

      def validate(o)
        validations.each do |att, procs|
          v = o.send(att)
          procs.each {|p| p[o, att, v]}
        end
      end

      def validates_each(*atts, &block)
        atts.each {|a| validations[a] << block}
      end
    end

    def self.included(c)
      c.extend ClassMethods
    end

    attr_accessor :errors

    def validate
      @errors = Errors.new
      self.class.validate(self)
    end

    def valid?
      validate
      errors.empty?
    end
    
    module ClassMethods
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

      def validates_format_of(*atts)
        opts = {
          :message => 'is invalid',
        }.merge!(atts.extract_options!)
        
        unless opts[:with].is_a?(Regexp)
          raise Sequel::Error, "A regular expression must be supplied as the :with option of the options hash"
        end
        
        validates_each(*atts) do |o, a, v|
          next if (v.nil? && opts[:allow_nil]) || (v.blank? && opts[:allow_blank])
          o.errors[a] << opts[:message] unless v =~ opts[:with]
        end
      end

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

      def validates_presence_of(*atts)
        opts = {
          :message => 'is not present',
        }.merge!(atts.extract_options!)
        
        validates_each(*atts) do |o, a, v|
          o.errors[a] << opts[:message] unless v && !v.blank?
        end
      end
    end
  end

  class Model
    include Validatable
    
    alias_method :save!, :save
    def save(*args)
      return false unless valid?
      save!(*args)
    end
  end
end
