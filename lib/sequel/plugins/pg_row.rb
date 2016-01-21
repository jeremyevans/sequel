# frozen-string-literal: true

module Sequel
  module Plugins
    # The pg_row plugin allows you to use Sequel::Model classes as composite type
    # classes, via the pg_row extension.  So if you have an address table:
    #
    #   DB.create_table(:address) do
    #     String :street
    #     String :city
    #     String :zip
    #   end
    #
    # and a company table with an address:
    #
    #   DB.create_table(:company) do
    #     String :name
    #     address :address
    #   end
    # 
    # You can create a Sequel::Model for the address table, and load the plugin,
    # which registers the row type:
    #
    #   class Address < Sequel::Model(:address)
    #     plugin :pg_row
    #   end
    #
    # Then when you select from the company table (even using a plain dataset),
    # it will return address values as instances of Address:
    #
    #   DB[:company].first
    #   # => {:name=>'MS', :address=>
    #   #  Address.load(:street=>'123 Foo St', :city=>'Bar Town', :zip=>'12345')}
    #
    # If you want a lot of your models to be used as row types, you can load the
    # plugin into Sequel::Model itself:
    #
    #   Sequel::Model.plugin :pg_row
    #
    # And then call register_row_type in the class
    #
    #   Address.register_row_type
    #
    # Note that automatic conversion only works with the native postgres adapter.
    # For other adapters that connect to PostgreSQL, you need to call the conversion
    # proc manually.
    #
    # In addition to returning row-valued/composite types as instances of Sequel::Model,
    # this also lets you use model instances in datasets when inserting, updating, and
    # filtering:
    #
    #   DB[:company].insert(:name=>'MS', :address=>
    #     Address.load(:street=>'123 Foo St', :city=>'Bar Town', :zip=>'12345'))
    module PgRow
      # When loading the extension, make sure the database has the pg_row extension
      # loaded, load the custom database extensions, and automatically register the
      # row type if the model has a dataset.
      def self.configure(model)
        model.db.extension(:pg_row)
        model.db.extend(DatabaseMethods)
        model.register_row_type if model.instance_variable_get(:@dataset)
      end

      module DatabaseMethods
        ESCAPE_RE = /("|\\)/.freeze
        ESCAPE_REPLACEMENT = '\\\\\1'.freeze
        COMMA = ','

        # Handle Sequel::Model instances in bound variables.
        def bound_variable_arg(arg, conn)
          case arg
          when Sequel::Model
            "(#{arg.values.values_at(*arg.columns).map{|v| bound_variable_array(v)}.join(COMMA)})"
          else
            super
          end
        end

        # If a Sequel::Model instance is given, return it as-is
        # instead of attempting to convert it.
        def row_type(db_type, v)
          if v.is_a?(Sequel::Model)
            v
          else
            super
          end
        end

        private

        # Handle Sequel::Model instances in bound variable arrays.
        def bound_variable_array(arg)
          case arg
          when Sequel::Model
            "\"(#{arg.values.values_at(*arg.columns).map{|v| bound_variable_array(v)}.join(COMMA).gsub(ESCAPE_RE, ESCAPE_REPLACEMENT)})\""
          else
            super
          end
        end
      end

      module ClassMethods
        # Register the model's row type with the database.
        def register_row_type
          table = dataset.first_source_table
          db.register_row_type(table, :converter=>self, :typecaster=>method(:new))
          db.instance_variable_get(:@schema_type_classes)[:"pg_row_#{table}"] = self
        end
      end

      module InstanceMethods
        ROW = 'ROW'.freeze
        CAST = '::'.freeze

        # Literalize the model instance and append it to the sql.
        def sql_literal_append(ds, sql)
          sql << ROW
          ds.literal_append(sql, values.values_at(*columns))
          sql << CAST
          ds.quote_schema_table_append(sql, model.dataset.first_source_table)
        end
      end
    end
  end
end
