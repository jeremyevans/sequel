Sequel.require 'adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << NativeException

  module JDBC
    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via JDBC.
    module Postgres
      # Methods to add to Database instances that access PostgreSQL via
      # JDBC.
      module DatabaseMethods
        include Sequel::Postgres::DatabaseMethods

        # Add the primary_keys and primary_key_sequences instance variables,
        # so we can get the correct return values for inserted rows.
        def self.extended(db)
          db.instance_eval do
            @primary_keys = {}
            @primary_key_sequences = {}
          end
        end

        private

        # Use setNull for nil arguments as the default behavior of setString
        # with nil doesn't appear to work correctly on PostgreSQL.
        def set_ps_arg(cps, arg, i)
          arg.nil? ? cps.setNull(i, JavaSQL::Types::NULL) : super
        end

        # Execute the connection configuration SQL queries on the connection.
        def setup_connection(conn)
          conn = super(conn)
          statement(conn) do |stmt|
            connection_configuration_sqls.each{|sql| log_yield(sql){stmt.execute(sql)}}
          end
          conn
        end
      end

      # Dataset subclass used for datasets that connect to PostgreSQL via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Postgres::DatasetMethods
        APOS = Dataset::APOS

        class ::Sequel::JDBC::Dataset::TYPE_TRANSLATOR
          # Convert Java::OrgPostgresqlUtil::PGobject to ruby strings
          def pg_object(v)
            v.to_string
          end
        end

        # Handle conversions of PostgreSQL array instances
        class PGArrayConverter
          # Set the method that will return the correct conversion
          # proc for elements of this array.
          def initialize(meth)
            @conversion_proc_method = meth
            @conversion_proc = nil
          end

          # Convert Java::OrgPostgresqlJdbc4::Jdbc4Array to ruby arrays
          def call(v)
            _pg_array(v.array)
          end

          private

          # Handle multi-dimensional Java arrays by recursively mapping them
          # to ruby arrays of ruby values.
          def _pg_array(v)
            v.to_ary.map do |i|
              if i.respond_to?(:to_ary)
                _pg_array(i)
              elsif i
                if @conversion_proc.nil?
                  @conversion_proc = @conversion_proc_method.call(i)
                end
                if @conversion_proc
                  @conversion_proc.call(i)
                else
                  i
                end
              else
                i
              end
            end
          end
        end

        PG_OBJECT_METHOD = TYPE_TRANSLATOR_INSTANCE.method(:pg_object)

        # Add the shared PostgreSQL prepared statement methods
        def prepare(*args)
          ps = super
          ps.extend(::Sequel::Postgres::DatasetMethods::PreparedStatementMethods)
          ps
        end

        private

        # Handle PostgreSQL array and object types. Object types are just
        # turned into strings, similarly to how the native adapter treats
        # the types.
        def convert_type_proc(v)
          case v
          when Java::OrgPostgresqlJdbc4::Jdbc4Array
            PGArrayConverter.new(method(:convert_type_proc))
          when Java::OrgPostgresqlUtil::PGobject
            PG_OBJECT_METHOD
          else
            super
          end
        end

        # Literalize strings similar to the native postgres adapter
        def literal_string_append(sql, v)
          sql << APOS << db.synchronize{|c| c.escape_string(v)} << APOS
        end
      end
    end
  end
end
