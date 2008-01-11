module Sequel                        #:nodoc:
  module Plugins                     #:nodoc:
    module StateMachine              #:nodoc:
      class NoInitialState < Sequel::Error; end
      
      # Configuration options are
      #
      # * +column+ - specifies the column name to use for keeping the state (default: state)
      # * +initial+ - specifies an initial state for newly created objects (required)
      def self.apply(klass, options = {})
        klass.states = Array.from_hash({}) #Array keyset by sequel
        klass.events = Array.from_hash({})
        klass.transitions = {}
        klass.state_column = options[:column] || "state"
        klass.initial_state = options[:initial] # later to be redefined by getter
        # Setup actions
        klass.before_create { set_initial_state }
        klass.after_create  { run_initial_state_actions }
      end
      
      module SupportingClasses
        class State
          attr_reader :name
          
          ACTION_CHECK_RE = /(?:^before_)|(?:^after_)/
          
          def initialize(name, actions = {})
            @name = name
            @actions = {}
            #letting use :exit instead of :after_exit
            actions.each do |action, method|
              action = :"after_#{action}" unless action.to_s =~ ACTION_CHECK_RE
              @actions[action] = method
            end
          end
          
          def fire_action(name, record)
            if action = @actions[name]
              puts action
              puts record
              Symbol === action ? record.method(action).call : action.call(record)
            end
          end
        end

        class Transition
          attr_reader :from
          attr_reader :to
          
          def initialize(specifications)
            @from = specifications[:from]
            @to   = specifications[:to] or raise "Where's the `to`?"
          end

          def perform(record)
            puts record.current_state
            puts @to
            puts record.current_state == @to
            return false if record.current_state == @to #Is it ok to return false?
            record.class.states[@to].fire_action(:before_enter, record)
            record.class.states[@from].fire_action(:before_exit, record)
            #This doesnt update attribute. Have to save record. Should i make an option?
            record[record.class.state_column] = @to.to_s
            record.class.states[@to].fire_action(:after_enter, record)
            record.class.states[@from].fire_action(:after_exit, record)
            true
          end
        end
        
        class Event
          attr_reader :name
          attr_reader :transitions

          def initialize(name, record, transitions=[], &block)
            @name = name
            @record = record
            @transitions = transitions
            instance_eval(&block) if block
          end
          
          def next_states
            if (transitions = @record.class.transitions[@name])
              transitions.select {|t| !t.from || t.from == @record.current_state }
            end
          end
          
          def fire
            return false unless states = next_states
            states.each do |transition|
              break true if transition.perform(@record)
            end
          end
          
          def transitions(options)
            (@record.transitions[@name] ||= []) << SupportingClasses::Transition.new(options)
          end
        end
      end
      
      module InstanceMethods
        def set_initial_state #:nodoc:
          self[self.class.state_column] = self.class.initial_state.to_s
        end
        
        def current_state
          self[self.class.state_column].to_sym
        end

        def run_initial_state_actions
          @states[self.class.initial_state].fire_action(:before_enter)
          @states[self.class.initial_state].fire_action(:after_enter)
        end

        def next_states_for_event(event)
          states = self.class.events[event].next_states(self)
          states.collect {|state| state.to} unless states.empty?
        end
      end

      module ClassMethods
        attr_accessor :states
        attr_accessor :transitions
        attr_accessor :events
        attr_accessor :state_column
        attr_accessor :initial_state
        
        # Define an event.  This takes a block which describes all valid transitions
        # for this event.
        #
        # Example:
        #
        # class Order < ActiveRecord::Base
        #   acts_as_state_machine :initial => :open
        #
        #   state :open
        #   state :closed
        #
        #   event :close_order do
        #     transitions :to => :closed, :from => :open
        #   end
        # end
        #
        # +transitions+ takes a hash where <tt>:to</tt> is the state to transition
        # to and <tt>:from</tt> is a state (or Array of states) from which this
        # event can be fired.
        #
        # This creates an instance method used for firing the event.  The method
        # created is the name of the event followed by an exclamation point (!).
        # Example: <tt>order.close_order!</tt>.
        def event(name, transitions={}, &block)
          event = SupportingClasses::Event.new(name, self, transitions, &block)
          @events[name] = event
          define_method("#{name}!") { event.fire(self) }
        end
        
        # Define a state of the system. +state+ can take an optional Proc object
        # which will be executed every time the system transitions into that
        # state.  The proc will be passed the current object.
        #
        # Example:
        #
        # class Order
        #   is :state_machine :initial => :open
        #
        #   state :open
        #   state :closed, :enter => Proc.new { |o| Mailer.send_notice(o) }
        # end
        def state(name, actions = {})
          initial_state = name if actions.delete(:initial)
          @states[name] = SupportingClasses::State.new(name, actions)
          define_method("#{name}?") { current_state == name }
        end
        
        def initial_state
          (initial_state ||= states.first.name)# or raise NoInitialState
        end

      end
    end
  end
end
