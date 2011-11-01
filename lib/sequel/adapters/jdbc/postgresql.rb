Sequel.require 'adapters/shared/postgres'

module Sequel
  Postgres::CONVERTED_EXCEPTIONS << NativeException
  
  module JDBC
    # Adapter, Database, and Dataset support for accessing a PostgreSQL
    # database via JDBC.
    module Postgres
      # Methods to add to the JDBC adapter/connection to allow it to work
      # with the shared PostgreSQL code.
      module AdapterMethods
        include Sequel::Postgres::AdapterMethods
        
        # Give the JDBC adapter a direct execute method, which creates
        # a statement with the given sql and executes it.
        def execute(sql, args=nil)
          method = block_given? ? :executeQuery : :execute
          @db.send(:statement, self) do |stmt|
            rows = @db.log_yield(sql){stmt.send(method, sql)}
            yield(rows) if block_given?
          end
        end
        
        private
        
        # JDBC specific method of getting specific values from a result set.
        def single_value(r)
          unless r.nil?
            r.next
            r.getString(1) unless r.getRow == 0
          end
        end
      end
    
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

        # Extend the adapter with the JDBC PostgreSQL AdapterMethods
        def setup_connection(conn)
          conn = super(conn)
          conn.extend(Sequel::JDBC::Postgres::AdapterMethods)
          conn.db = self
          conn.apply_connection_settings
          conn
        end
        
        # Call insert_result with the table and values specified in the opts.
        def last_insert_id(conn, opts)
          insert_result(conn, opts[:table], opts[:values])
        end
      end
      
      # Dataset subclass used for datasets that connect to PostgreSQL via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Postgres::DatasetMethods

        # Add the shared PostgreSQL prepared statement methods
        def prepare(*args)
          ps = super
          ps.extend(::Sequel::Postgres::DatasetMethods::PreparedStatementMethods)
          ps
        end
        
        # Literalize strings similar to the native postgres adapter
        def literal_string_append(sql, v)
          sql << "'" << db.synchronize{|c| c.escape_string(v)} << "'"
        end
      end
    end
  end
end
