module Sequel
  module Plugins
    # The PgTypecastOnLoad plugin exists because when you connect to PostgreSQL
    # using the do, swift, or jdbc adapter, Sequel doesn't have complete
    # control over typecasting, and may return columns as strings instead of how
    # the native postgres adapter would typecast them.  This is mostly needed for
    # the additional support that the pg_* extensions add for advanced PostgreSQL
    # types such as arrays.
    #
    # This plugin makes model loading to do the same conversion that the
    # native postgres adapter would do for all columns given.  You can either
    # specify the columns to typecast on load in the plugin call itself, or
    # afterwards using add_pg_typecast_on_load_columns:
    #
    #   # aliases => text[] column
    #   # config => hstore column
    #
    #   Album.plugin :pg_typecast_on_load, :aliases, :config
    #   # or:
    #   Album.plugin :pg_typecast_on_load
    #   Album.add_pg_typecast_on_load_columns :aliases, :config
    #
    # This plugin only handles values that the adapter returns as strings.  If
    # the adapter returns a value other than a string, this plugin will have no
    # effect.  You may be able to use the regular typecast_on_load plugin to
    # handle those cases.
    module PgTypecastOnLoad
      # Call add_pg_typecast_on_load_columns on the passed column arguments.
      def self.configure(model, *columns)
        model.instance_eval do
          @pg_typecast_on_load_columns ||= []
          add_pg_typecast_on_load_columns(*columns)
        end
      end

      module ClassMethods
        # The columns to typecast on load for this model.
        attr_reader :pg_typecast_on_load_columns

        # Add additional columns to typecast on load for this model.
        def add_pg_typecast_on_load_columns(*columns)
          @pg_typecast_on_load_columns.concat(columns)
        end

        def call(values)
          super(load_typecast_pg(values))
        end

        # Lookup the conversion proc for the column's oid in the Database
        # object, and use it to convert the value.
        def load_typecast_pg(values)
          pg_typecast_on_load_columns.each do |c|
            if (v = values[c]).is_a?(String) && (oid = db_schema[c][:oid]) && (pr = db.conversion_procs[oid])
              values[c] = pr.call(v)
            end
          end
          values
        end

        Plugins.inherited_instance_variables(self, :@pg_typecast_on_load_columns=>:dup)
      end

      module InstanceMethods
        private

        # Typecast specific columns using the conversion procs when manually refreshing.
        def _refresh_set_values(values)
          super(model.load_typecast_pg(values))
        end

        # Typecast specific columns using the conversion procs when refreshing after save.
        def _save_set_values(values)
          super(model.load_typecast_pg(values))
        end
      end
    end
  end
end
