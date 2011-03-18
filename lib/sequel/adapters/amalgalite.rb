require 'amalgalite'
Sequel.require 'adapters/shared/sqlite'

module Sequel
  # Top level module for holding all Amalgalite-related modules and classes
  # for Sequel.
  module Amalgalite
    # Type conversion map class for Sequel's use of Amalgamite
    class SequelTypeMap < ::Amalgalite::TypeMaps::DefaultMap
      methods_handling_sql_types.delete('string')
      methods_handling_sql_types.merge!(
        'datetime' => %w'datetime timestamp',
        'time' => %w'time',
        'float' => ['float', 'double', 'real', 'double precision'],
        'decimal' => %w'numeric decimal money'
      )
      
      # Return blobs as instances of Sequel::SQL::Blob instead of
      # Amalgamite::Blob
      def blob(s)
        SQL::Blob.new(s)
      end
      
      # Return numeric/decimal types as instances of BigDecimal
      # instead of Float
      def decimal(s)
        BigDecimal.new(s)
      end
      
      # Return datetime types as instances of Sequel.datetime_class
      def datetime(s)
        Sequel.database_to_application_timestamp(s)
      end
      
      # Don't raise an error if the value is a string and the declared
      # type doesn't match a known type, just return the value.
      def result_value_of(declared_type, value)
        if value.is_a?(::Amalgalite::Blob)
          SQL::Blob.new(value.to_s)
        elsif value.is_a?(String) && declared_type
          (meth = self.class.sql_to_method(declared_type.downcase)) ? send(meth, value) : value
        else
          super
        end
      end
    end
    
    # Database class for SQLite databases used with Sequel and the
    # amalgalite driver.
    class Database < Sequel::Database
      include ::Sequel::SQLite::DatabaseMethods
      
      set_adapter_scheme :amalgalite
      
      # Mimic the file:// uri, by having 2 preceding slashes specify a relative
      # path, and 3 preceding slashes specify an absolute path.
      def self.uri_to_options(uri) # :nodoc:
        { :database => (uri.host.nil? && uri.path == '/') ? nil : "#{uri.host}#{uri.path}" }
      end
      private_class_method :uri_to_options
      
      # Connect to the database.  Since SQLite is a file based database,
      # the only options available are :database (to specify the database
      # name), and :timeout, to specify how long to wait for the database to
      # be available if it is locked, given in milliseconds (default is 5000).
      def connect(server)
        opts = server_opts(server)
        opts[:database] = ':memory:' if blank_object?(opts[:database])
        db = ::Amalgalite::Database.new(opts[:database])
        db.busy_handler(::Amalgalite::BusyTimeout.new(opts.fetch(:timeout, 5000)/50, 50))
        db.type_map = SequelTypeMap.new
        db
      end
      
      # Amalgalite is just the SQLite database without a separate SQLite installation.
      def database_type
        :sqlite
      end

      # Return instance of Sequel::Amalgalite::Dataset with the given options.
      def dataset(opts = nil)
        Amalgalite::Dataset.new(self, opts)
      end
      
      # Run the given SQL with the given arguments. Returns nil.
      def execute_ddl(sql, opts={})
        _execute(sql, opts){|conn| log_yield(sql){conn.execute_batch(sql)}}
        nil
      end
      
      # Run the given SQL with the given arguments and return the number of changed rows.
      def execute_dui(sql, opts={})
        _execute(sql, opts){|conn| log_yield(sql){conn.execute_batch(sql)}; conn.row_changes}
      end
      
      # Run the given SQL with the given arguments and return the last inserted row id.
      def execute_insert(sql, opts={})
        _execute(sql, opts){|conn| log_yield(sql){conn.execute_batch(sql)}; conn.last_insert_rowid}
      end
      
      # Run the given SQL with the given arguments and yield each row.
      def execute(sql, opts={})
        _execute(sql, opts) do |conn|
          begin
            yield(stmt = log_yield(sql){conn.prepare(sql)})
          ensure
            stmt.close if stmt
          end
        end
      end
      
      # Run the given SQL with the given arguments and return the first value of the first row.
      def single_value(sql, opts={})
        _execute(sql, opts){|conn| log_yield(sql){conn.first_value_from(sql)}}
      end
      
      private
      
      # Yield an available connection.  Rescue
      # any Amalgalite::Errors and turn them into DatabaseErrors.
      def _execute(sql, opts)
        begin
          synchronize(opts[:server]){|conn| yield conn}
        rescue ::Amalgalite::Error, ::Amalgalite::SQLite3::Error => e
          raise_error(e)
        end
      end
      
      # The Amagalite adapter does not need the pool to convert exceptions.
      # Also, force the max connections to 1 if a memory database is being
      # used, as otherwise each connection gets a separate database.
      def connection_pool_default_options
        o = super.dup
        # Default to only a single connection if a memory database is used,
        # because otherwise each connection will get a separate database
        o[:max_connections] = 1 if @opts[:database] == ':memory:' || blank_object?(@opts[:database])
        o
      end
      
      # Both main error classes that Amalgalite raises
      def database_error_classes
        [::Amalgalite::Error, ::Amalgalite::SQLite3::Error]
      end

      # Disconnect given connections from the database.
      def disconnect_connection(c)
        c.close
      end
    end
    
    # Dataset class for SQLite datasets that use the amalgalite driver.
    class Dataset < Sequel::Dataset
      include ::Sequel::SQLite::DatasetMethods
      
      # Yield a hash for each row in the dataset.
      def fetch_rows(sql)
        execute(sql) do |stmt|
          @columns = cols = stmt.result_fields.map{|c| output_identifier(c)}
          col_count = cols.size
          stmt.each do |result|
            row = {}
            col_count.times{|i| row[cols[i]] = result[i]}
            yield row
          end
        end
      end

      private
      
      # Quote the string using the adapter instance method.
      def literal_string(v)
        db.synchronize{|c| c.quote(v)}
      end
    end
  end
end
