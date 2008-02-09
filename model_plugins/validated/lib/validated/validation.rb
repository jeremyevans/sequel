require 'observer'

module Validated
  
  # == The superclass for Validations.
  #
  # See new for more information.
  class Validation
    
    extend Observable
    def self.inherited(descendant) #:nodoc:
      changed and notify_observers(descendant)

      descendant.
      instance_variable_set :@observer_peers, @observer_peers.clone
    end
    
    # Builds validations.
    #
    # <b>Example:</b>
    #   Validated::Validation.new :temp, :if => :water? do |obj, attr, val|
    #     obj.errors.add attr, 'too hot' unless val < 100
    #   end
    #
    # <b>Like:</b>
    #   class TempValidation < Validated::Validation
    #     def initialize(opts, attributes)
    #       super opts, attributes method(:temp_validation)
    #     end
    #     def temp_validation(obj, attr, val)
    #       obj.errors.add attr, 'too hot' unless val < 100
    #     end
    #   end
    #
    #   Validation.new TempValidation, :temp, :if => :water?
    #
    # The last one also notifies all Observers of Validation (see
    # ValidationBuilder#update). If ValidationBuilder#update is called because
    # <Name>Validation is inherited from Validation the ValidationBuilder gets
    # the method validates_<name>_of and so does the classes that included the
    # ValidationBuilder.
    def self.new(*params, &block)
      attributes = if params.first.is_a? Class and params.first < self
        klass = params.shift
        klass.new(*params, &block)
      else
        options = params.extract_options!
        instance = allocate
        instance.send! :initialize, options, params.map {|p|p.to_sym}, &block
        instance
      end
    end
    
    attr_reader :attributes
    
    def initialize(opts, attributes, &block) #:nodoc:
      build_conditions opts[:if], opts[:unless]
      @attributes, @block, @opts = attributes, block, opts
    end
    
    def call_without_conditions(obj, attr, value) #:nodoc:
      @block.call obj, attr, value
    end
    alias_method :call, :call_without_conditions
    
    def call_with_conditions(obj, attr, value) #:nodoc:
      if @conditions.all? { |c| c.evaluate obj }
        call_without_conditions obj, attr, value
      end
    end
    
    protected
    def build_conditions(p, n) #:nodoc:
      @conditions = []
      [p].flatten.each {|c| @conditions << Condition.new(c) if c }
      [n].flatten.each {|c| @conditions << Condition.new(c, false) if c }
      
      (class << self; self; end).module_eval do
        alias_method :call, :call_with_conditions
      end if @conditions.any?
    end
    
    # Conditions for use in Validations are usually used with Validations
    class Condition

      # An instance of Condition accepts Symbols, UnboundMethods or anything
      # that responds to :call.
      #
      # The following examples are similiar to each other:
      #
      #   Validated::Validation::Condition.new proc {|o| o.nil?}
      #   Validated::Validation::Condition.new :nil?
      #   Validated::Validation::Condition.new Object.instance_method(:nil?)
      def self.new(condition, positive = true)
        instance = allocate
        instance.instance_variable_set :@condition, condition

        block = case condition
        when Symbol then positive ?
          proc { |o| o.send! @condition } :
          proc { |o| not o.send! @condition }
        when UnboundMethod then positive ?
          proc { |o| @condition.bind(o).call } :
          proc { |o| not @condition.bind(o).call }
        else positive ?
          proc { |o| @condition.call o } :
          proc { |o| not @condition.call o }
        end
        
        (class << instance; self; end).
        module_eval { define_method(:evaluate, &block) }
        
        instance
      end

    end
  end
end
