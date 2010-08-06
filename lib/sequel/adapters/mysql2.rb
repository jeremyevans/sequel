require 'mysql2' unless defined? Mysql2

Sequel.require %w'shared/mysql', 'adapters'

module Sequel
  # Module for holding all Mysql2-related classes and modules for Sequel.
  module Mysql2
    # Database class for MySQL databases used with Sequel.
    class Database < Sequel::Database
      include Sequel::MySQL::DatabaseMethods

      # Mysql::Error messages that indicate the current connection should be disconnected
      MYSQL_DATABASE_DISCONNECT_ERRORS = /\A(Commands out of sync; you can't run this command now|Can't connect to local MySQL server through socket|MySQL server has gone away)/

      set_adapter_scheme :mysql2

      # Connect to the database.  In addition to the usual database options,
      # the following options have effect:
      #
      # * :auto_is_null - Set to true to use MySQL default behavior of having
      #   a filter for an autoincrement column equals NULL to return the last
      #   inserted row.
      # * :charset - Same as :encoding (:encoding takes precendence)
      # * :compress - Set to false to not compress results from the server
      # * :config_default_group - The default group to read from the in
      #   the MySQL config file.
      # * :config_local_infile - If provided, sets the Mysql::OPT_LOCAL_INFILE
      #   option on the connection with the given value.
      # * :encoding - Set all the related character sets for this
      #   connection (connection, client, database, server, and results).
      # * :socket - Use a unix socket file instead of connecting via TCP/IP.
      # * :timeout - Set the timeout in seconds before the server will
      #   disconnect this connection.
      def connect(server)
        opts = server_opts(server)
        opts[:host] ||= 'localhost'
        opts[:username] ||= opts[:user]
        conn = ::Mysql2::Client.new(opts)

        sqls = []
        # Set encoding a slightly different way after connecting,
        # in case the READ_DEFAULT_GROUP overrode the provided encoding.
        # Doesn't work across implicit reconnects, but Sequel doesn't turn on
        # that feature.
        if encoding = opts[:encoding] || opts[:charset]
          sqls << "SET NAMES #{conn.escape(encoding.to_s)}"
        end

        # increase timeout so mysql server doesn't disconnect us
        sqls << "SET @@wait_timeout = #{opts[:timeout] || 2592000}"

        # By default, MySQL 'where id is null' selects the last inserted id
        sqls << "SET SQL_AUTO_IS_NULL=0" unless opts[:auto_is_null]

        sqls.each{|sql| log_yield(sql){conn.query(sql)}}

        conn
      end

      # Returns instance of Sequel::MySQL::Dataset with the given options.
      def dataset(opts = nil)
        Mysql2::Dataset.new(self, opts)
      end

      # Executes the given SQL using an available connection, yielding the
      # connection if the block is given.
      def execute(sql, opts={}, &block)
        if opts[:sproc]
          call_sproc(sql, opts, &block)
        else
          synchronize(opts[:server]){|conn| _execute(conn, sql, opts, &block)}
        end
      end

      # Return the version of the MySQL server two which we are connecting.
      def server_version(server=nil)
        @server_version ||= (synchronize(server){|conn| conn.server_info[:id]} || super)
      end

      private

      # Use MySQL specific syntax for engine type and character encoding
      def create_table_sql(name, generator, options = {})
        engine = options.fetch(:engine, Sequel::MySQL.default_engine)
        charset = options.fetch(:charset, Sequel::MySQL.default_charset)
        collate = options.fetch(:collate, Sequel::MySQL.default_collate)
        generator.columns.each do |c|
          if t = c.delete(:table)
            generator.foreign_key([c[:name]], t, c.merge(:name=>nil, :type=>:foreign_key))
          end
        end
        super(name, generator, options.merge(:engine => engine, :charset => charset, :collate => collate))
      end

      # Execute the given SQL on the given connection.  If the :type
      # option is :select, yield the result of the query, otherwise
      # yield the connection if a block is given.
      def _execute(conn, sql, opts)
        query_opts = {:symbolize_keys => true}
        query_opts.merge!(:database_timezone => Sequel.database_timezone) if Sequel.respond_to?(:database_timezone)
        query_opts.merge!(:application_timezone => Sequel.application_timezone) if Sequel.respond_to?(:application_timezone)
        begin
          r = log_yield(sql){conn.query(sql, query_opts)}
          if opts[:type] == :select
            yield r if r
          elsif block_given?
            yield conn
          end
        rescue ::Mysql2::Error => e
          raise_error(e, :disconnect=>MYSQL_DATABASE_DISCONNECT_ERRORS.match(e.message))
        end
      end

      # MySQL connections use the query method to execute SQL without a result
      def connection_execute_method
        :query
      end

      # The MySQL adapter main error class is Mysql2::Error
      def database_error_classes
        [::Mysql2::Error]
      end

      # The database name when using the native adapter is always stored in
      # the :database option.
      def database_name
        @opts[:database]
      end

      # Closes given database connection.
      def disconnect_connection(c)
        c.close
      end

      # Convert tinyint(1) type to boolean if convert_tinyint_to_bool is true
      def schema_column_type(db_type)
        Sequel::MySQL.convert_tinyint_to_bool && db_type == 'tinyint(1)' ? :boolean : super
      end
    end

    # Dataset class for MySQL datasets accessed via the native driver.
    class Dataset < Sequel::Dataset
      include Sequel::MySQL::DatasetMethods

      # Delete rows matching this dataset
      def delete
        execute_dui(delete_sql){|c| return c.affected_rows}
      end

      # Yield all rows matching this dataset.
      def fetch_rows(sql, &block)
        execute(sql) do |r|
          @columns = r.fields
          r.each(:cast_booleans => Sequel::MySQL.convert_tinyint_to_bool, &block)
        end
        self
      end

      # Don't allow graphing a dataset that splits multiple statements
      def graph(*)
        raise(Error, "Can't graph a dataset that splits multiple result sets") if opts[:split_multiple_result_sets]
        super
      end

      # Insert a new value into this dataset
      def insert(*values)
        execute_dui(insert_sql(*values)){|c| return c.last_id}
      end

      # Replace (update or insert) the matching row.
      def replace(*args)
        execute_dui(replace_sql(*args)){|c| return c.last_id}
      end

      # Update the matching rows.
      def update(values={})
        execute_dui(update_sql(values)){|c| return c.affected_rows}
      end

      private

      # Set the :type option to :select if it hasn't been set.
      def execute(sql, opts={}, &block)
        super(sql, {:type=>:select}.merge(opts), &block)
      end

      # Set the :type option to :dui if it hasn't been set.
      def execute_dui(sql, opts={}, &block)
        super(sql, {:type=>:dui}.merge(opts), &block)
      end

      # Handle correct quoting of strings using ::Mysql2::Client#escape.
      def literal_string(v)
        db.synchronize{|c| "'#{c.escape(v)}'"}
      end
    end
  end
end
