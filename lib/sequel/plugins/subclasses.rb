module Sequel
  module Plugins
    # The Subclasses plugin keeps track of all subclasses of the
    # current model class.  Direct subclasses are available via the
    # subclasses method, and all descendent classes are available via the
    # descendents method.
    #
    #   c = Class.new(Sequel::Model)
    #   c.plugin :subclasses
    #   sc1 = Class.new(c)
    #   sc2 = Class.new(c)
    #   ssc1 = Class.new(sc1)
    #   c.subclasses    # [sc1, sc2]
    #   sc1.subclasses  # [ssc1]
    #   sc2.subclasses  # []
    #   ssc1.subclasses # []
    #   c.descendents   # [sc1, ssc1, sc2]
    module Subclasses
      # Initialize the subclasses instance variable for the model.
      def self.apply(model)
        model.instance_variable_set(:@subclasses, [])
      end

      module ClassMethods
        # All subclasses for the current model.  Does not
        # include the model itself.
        attr_reader :subclasses

        # All descendent classes of this model.
        def descendents
          subclasses.map{|x| [x] + x.descendents}.flatten
        end

        # Add the subclass to this model's current subclasses,
        # and initialize a new subclasses instance variable
        # in the subclass.
        def inherited(subclass)
          super
          subclasses << subclass
          subclass.instance_variable_set(:@subclasses, [])
        end
      end
    end
  end
end
