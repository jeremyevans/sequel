module Sequel #:nodoc:
  module Plugins #:nodoc:
    module Validated
      
      require "#{__FILE__[0..-4]}/class_extensions"
      require "#{__FILE__[0..-4]}/instance_methods"
      
      def self.apply(base, *args) #:nodoc:
        base.extend ClassExtensions
      end
      
      require 'forwardable'
      
      # Thrown exception if validation fails, with instance of Errors stored
      # in errors attribute
      class ValidationError < Error
        extend Forwardable
        
        def_delegators :@errors, :on, :full_messages, :each
        attr_reader :errors
        
        # Returns instance of ValidationError with errors set
        def self.with(errors)
          instance = new
          instance.instance_variable_set :@errors, errors
          
          instance
        end
      end
      
      # Container for failed validations
      class Errors
        extend Forwardable
        
        def_delegators :@errors, :empty?, :clear, :[], :each, :to_yaml
        alias_method :on, :[]
        
        include Enumerable
        
        def initialize() #:nodoc:
          @errors = Hash.new {|h, k| h[k] = []}
        end
        # Adds an error for the given attribute.
        def add(k, msg)  @errors[k] << msg end
        
        # Returns an array of fully-formatted error messages.
        def full_messages
          @errors.inject([]) do |messages, k_errors| k, errors = *k_errors
            errors.each {|e| messages << "#{k} #{e}"}
            messages
          end
        end
      end
      
    end
  end
  
  ValidationError = Plugins::Validated::ValidationError
  @models       ||= {}
  
  def self.Model(source) #:nodoc:
    @models[source] ||= Class.new(Sequel::Model) do
      meta_def :inherited_with_dataset do |base|
        if source.is_a? Dataset
          base.set_dataset source
        else
          base.set_dataset base.db[source]
        end
        
        inherited_without_dataset base
      end
      
      meta_eval { alias_method_chain :inherited, :dataset }
    end
  end
  
end
