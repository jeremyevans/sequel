require 'mysql2'
Sequel.require %w'shared/mysql_prepared_statements', 'adapters'

module Sequel
  # Module for holding all Mysql2-related classes and modules for Sequel.
  module Mysql2
    # Database class for MySQL databases used with Sequel.
    class Database < Sequel::Database
      include Sequel::MySQL::DatabaseMethods
      include Sequel::MySQL::PreparedStatements::DatabaseMethods

      # Mysql::Error messages that indicate the current connection should be disconnected
      MYSQL_DATABASE_DISCONNECT_ERRORS = /\A(Commands out of sync; you can't run this command now|Can't connect to local MySQL server through socket|MySQL server has gone away|This connection is still waiting for a result, try again once you have the result|closed MySQL connection)/

      set_adapter_scheme :mysql2
      
      # Whether to convert tinyint columns to bool for this database
      attr_accessor :convert_tinyint_to_bool

      # Set the convert_tinyint_to_bool setting based on the default value.
      def initialize(opts={})
        super
        self.convert_tinyint_to_bool = Sequel::MySQL.convert_tinyint_to_bool
      end

      # Connect to the database.  In addition to the usual database options,
      # the following options have effect:
      #
      # * :auto_is_null - Set to true to use MySQL default behavior of having
      #   a filter for an autoincrement column equals NULL to return the last
      #   inserted row.
      # * :charset - Same as :encoding (:encoding takes precendence)
      # * :config_default_group - The default group to read from the in
      #   the MySQL config file.
      # * :config_local_infile - If provided, sets the Mysql::OPT_LOCAL_INFILE
      #   option on the connection with the given value.
      # * :connect_timeout - Set the timeout in seconds before a connection
      #   attempt is abandoned.
      # * :encoding - Set all the related character sets for this
      #   connection (connection, client, database, server, and results).
      # * :socket - Use a unix socket file instead of connecting via TCP/IP.
      # * :timeout - Set the timeout in seconds before the server will
      #   disconnect this connection (a.k.a. @@wait_timeout).
      def connect(server)
        opts = server_opts(server)
        opts[:host] ||= 'localhost'
        opts[:username] ||= opts[:user]
        opts[:flags] = ::Mysql2::Client::FOUND_ROWS if ::Mysql2::Client.const_defined?(:FOUND_ROWS)
        conn = ::Mysql2::Client.new(opts)

        sqls = mysql_connection_setting_sqls

        # Set encoding a slightly different way after connecting,
        # in case the READ_DEFAULT_GROUP overrode the provided encoding.
        # Doesn't work across implicit reconnects, but Sequel doesn't turn on
        # that feature.
        if encoding = opts[:encoding] || opts[:charset]
          sqls.unshift("SET NAMES #{conn.escape(encoding.to_s)}")
        end

        sqls.each{|sql| log_yield(sql){conn.query(sql)}}

        add_prepared_statements_cache(conn)
        conn
      end

      # Return the number of matched rows when executing a delete/update statement.
      def execute_dui(sql, opts={})
        execute(sql, opts){|c| return c.affected_rows}
      end

      # Return the last inserted id when executing an insert statement.
      def execute_insert(sql, opts={})
        execute(sql, opts){|c| return c.last_id}
      end

      # Return the version of the MySQL server two which we are connecting.
      def server_version(server=nil)
        @server_version ||= (synchronize(server){|conn| conn.server_info[:id]} || super)
      end

      private

      # Execute the given SQL on the given connection.  If the :type
      # option is :select, yield the result of the query, otherwise
      # yield the connection if a block is given.
      def _execute(conn, sql, opts)
        begin
          r = log_yield((log_sql = opts[:log_sql]) ? sql + log_sql : sql){conn.query(sql, :symbolize_keys => true, :database_timezone => timezone, :application_timezone => Sequel.application_timezone)}
          if opts[:type] == :select
            yield r if r
          elsif block_given?
            yield conn
          end
        rescue ::Mysql2::Error => e
          raise_error(e)
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

      def disconnect_error?(e, opts)
        super || (e.is_a?(::Mysql2::Error) && MYSQL_DATABASE_DISCONNECT_ERRORS.match(e.message))
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
        convert_tinyint_to_bool && db_type == 'tinyint(1)' ? :boolean : super
      end
    end

    # Dataset class for MySQL datasets accessed via the native driver.
    class Dataset < Sequel::Dataset
      include Sequel::MySQL::DatasetMethods
      include Sequel::MySQL::PreparedStatements::DatasetMethods

      Database::DatasetClass = self

      # Yield all rows matching this dataset.
      def fetch_rows(sql)
        execute(sql) do |r|
          if identifier_output_method
            cols = r.fields
            @columns = cols2 = cols.map{|c| output_identifier(c.to_s)}
            cs = cols.zip(cols2)
            r.each(:cast_booleans => db.convert_tinyint_to_bool) do |row|
              h = {}
              cs.each do |a, b|
                h[b] = row[a]
              end
              yield h
            end
          else
            @columns = r.fields
            r.each(:cast_booleans => db.convert_tinyint_to_bool){|h| yield h}
          end
        end
        self
      end

      private

      # Set the :type option to :select if it hasn't been set.
      def execute(sql, opts={}, &block)
        super(sql, {:type=>:select}.merge(opts), &block)
      end

      # Handle correct quoting of strings using ::Mysql2::Client#escape.
      def literal_string_append(sql, v)
        sql << "'" << db.synchronize{|c| c.escape(v)} << "'"
      end
    end
  end
end
