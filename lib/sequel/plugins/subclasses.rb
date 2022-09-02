# frozen-string-literal: true

module Sequel
  module Plugins
    # The subclasses plugin keeps track of all subclasses of the
    # current model class.  Direct subclasses are available via the
    # subclasses method, and all descendent classes are available via the
    # descendants method:
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
    #   c.descendants   # [sc1, ssc1, sc2]
    #
    # You can also finalize the associations and then freeze the classes
    # in all descendent classes.  Doing so is a recommended practice after
    # all models have been defined in production and testing, and this makes
    # it easier than keeping track of the classes to finalize and freeze
    # manually:
    #
    #   c.freeze_descendants
    #
    # You can provide a block when loading the plugin, and it will be called
    # with each subclass created:
    #
    #   a = []
    #   Sequel::Model.plugin(:subclasses){|sc| a << sc}
    #   class A < Sequel::Model; end
    #   class B < Sequel::Model; end
    #   a # => [A, B]
    module Subclasses
      NEED_SUBCLASSES = !Object.respond_to?(:subclasses) || Object.method(:subclasses).source_location
      private_constant :NEED_SUBCLASSES

      # Initialize the subclasses instance variable for the model.
      def self.apply(model, &block)
        # :nocov:
        model.instance_variable_set(:@subclasses, [])  if NEED_SUBCLASSES
        # :nocov:
        model.instance_variable_set(:@on_subclass, block)
      end

      module ClassMethods
        # Callable object that should be called with every descendent
        # class created.
        attr_reader :on_subclass

        # :nocov:
        if NEED_SUBCLASSES
          # All subclasses for the current model.  Does not
          # include the model itself.
          attr_reader :subclasses
        end
        # :nocov:

        # All descendent classes of this model.
        def descendants
          Sequel.synchronize{subclasses.dup}.map{|x| [x] + x.send(:descendants)}.flatten
        end

        # SEQUEL6: Remove
        alias descendents descendants

        # Freeze all descendent classes.  This also finalizes the associations for those
        # classes before freezing.
        def freeze_descendants
          descendants.each(&:finalize_associations).each(&:freeze)
        end

        # SEQUEL6: Remove
        alias freeze_descendents freeze_descendants

        Plugins.inherited_instance_variables(self, :@subclasses=>lambda{|v| []}, :@on_subclass=>nil)

        private

        # Add the subclass to this model's current subclasses,
        # and initialize a new subclasses instance variable
        # in the subclass.
        def inherited(subclass)
          super
          # :nocov:
          Sequel.synchronize{subclasses << subclass} if NEED_SUBCLASSES
          # :nocov:
          on_subclass.call(subclass) if on_subclass
        end
      end
    end
  end
end
