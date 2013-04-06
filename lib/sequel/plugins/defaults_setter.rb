module Sequel
  module Plugins
    # DefaultsSetter is a simple plugin that sets non-nil/NULL default values upon
    # initialize:
    #
    #   # column a default NULL
    #   # column b default 2
    #   album = Album.new.values # {:b => 2}
    #   album = Album.new(:a=>1, :b=>3).values # {:a => 1, :b => 3}
    # 
    # Usage:
    #
    #   # Make all model subclass instances set defaults (called before loading subclasses)
    #   Sequel::Model.plugin :defaults_setter
    #
    #   # Make the Album class set defaults 
    #   Album.plugin :defaults_setter
    module DefaultsSetter
      # Set the default values based on the model schema
      def self.configure(model)
        model.send(:set_default_values)
      end

      module ClassMethods
        # The default values to set in initialize for this model.  A hash with column symbol
        # keys and default values.  If the default values respond to +call+, it will be called
        # to get the value, otherwise the value will be used directly.  You can manually modify
        # this hash to set specific default values, by default the ones will be parsed from the database.
        attr_reader :default_values
        
        Plugins.after_set_dataset(self, :set_default_values)

        private

        # Parse the cached database schema for this model and set the default values appropriately.
        def set_default_values
          h = {}
          @db_schema.each{|k, v| h[k] = convert_default_value(v[:ruby_default]) unless v[:ruby_default].nil?} if @db_schema
          @default_values = h
        end

        # Handle the CURRENT_DATE and CURRENT_TIMESTAMP values specially by returning an appropriate Date or
        # Time/DateTime value.
        def convert_default_value(v)
          case v
          when Sequel::CURRENT_DATE
            lambda{Date.today}
          when Sequel::CURRENT_TIMESTAMP
            lambda{Sequel.datetime_class.now}
          else
            v
          end
        end
      end

      module InstanceMethods
        private

        # Set default values if they are not already set by the hash provided to initialize.
        def initialize_set(h)
          super
          model.default_values.each{|k,v| self[k] = (v.respond_to?(:call) ? v.call : v) unless values.has_key?(k)}
        end
      end
    end
  end
end
