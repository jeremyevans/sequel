module NotNaughty

  # == Superclass for all Adapters.
  #
  # See new and get_state for more information.
  class Validator
    
    attr_reader :states
    
    # By default it comes with the :default State unless other states are
    # provided.
    #
    # <b>Example:</b>
    #   NotNaughty::Validator.new
    #     # has the :default state
    #   NotNaughty::Validator.new :create, :update
    #     # has the :create and :update states
    #
    # <em>Adapters should overwrite this method.</em>
    def initialize(*states)
      states << :default if states.empty?
      
      @states         = states.inject({}) {|m, s| m.update s => State.new(s)}
      @initial_state  = @states[states.first]
    end
    
    def clone #:nodoc:
      states = [@initial_state.name] | @states.keys
      
      clone = self.class.new(*states)
      @states.each do |n, s|
        s.validations.each { |a, v| clone.states[n].validations[a] = v.clone }
      end
      clone.instance_eval { @initial_state = @states[states.first] }
      
      clone
    end
    
    # Returns the state for the given object. By default it does return the
    # initial state.
    #
    # <em>Adapters that provide multiple states should eventually overwrite
    # this method</em>.
    def get_state(obj = nil) @initial_state end
    
    # Adds a validation to all/specified states.
    #
    # <b>Example:</b>
    #   add_validation(:firstname, :lastname, :on => :default) {|o, a, v|}
    #     # adds validation to :default state
    #   add_validation(:firstname, :lastname) {|o, a, v|}
    #     # adds validation to all states
    #   add_validation(:first, :last, :on => [:create, :update]) {|o, a, v|}
    #     # adds validation to :create and :update states
    def add_validation(*p, &b)
      options = (p.last.is_a? Hash) ? p.last : {}
      
      if states = options.delete(:on)
        @states.values_at(*states).each do |state|
          state.add_validation(*p, &b) unless state.nil?
        end
      else
        @states.each { |name, state| state.add_validation(*p, &b) }
      end
    end
    
    # Returns true if given object has validations in its current state. If
    # no object was given it returns true if any state has validations. It
    # otherwise returns false.
    def has_validations?(obj = nil)
      if obj.nil? then @states.any? { |name, state| state.has_validations? }
      else get_state(obj).has_validations? end
    end
    
    # Runs all validations on object for the object's state.
    def invoke(obj)
      get_state(obj).validations.each do |attr, validations|
        val = obj.send! attr
        validations.each { |validation| validation.call obj, attr, val }
      end
    end
    
    # == Container for attribute specific validations
    #
    # See Validator for details.
    class State
      
      attr_reader :validations, :name
      
      # Initializes the state with given name.
      def initialize(name = :default)
        @name, @validations = name, Hash.new {|h, k| h[k] = []}
      end
      
      # Adds the validation that results from <tt>params</tt> and
      # <tt>block</tt> to validated attributes (see Validation#new for
      # details).
      def add_validation(*params, &block)
        validation = Validation.new(*params, &block)
        
        validation.attributes.each do |attribute|
          @validations[attribute] << validation if attribute.is_a? Symbol
        end
      end
      
      # Returns validations for given attribute.
      def [](attribute)
        @validations[attribute]
      end
      
      # Returns true if a attributes has validations assigned, false
      # otherwise.
      def has_validations?
        @validations.any? { |attribute, validations| validations.any? }
      end
      
    end
  end
end
