require 'mysql2'
Sequel.require %w'shared/mysql_prepared_statements', 'adapters'

module Sequel
  # Module for holding all Mysql2-related classes and modules for Sequel.
  module Mysql2
    # Database class for MySQL databases used with Sequel.
    class Database < Sequel::Database
      include Sequel::MySQL::DatabaseMethods
      include Sequel::MySQL::PreparedStatements::DatabaseMethods

      set_adapter_scheme :mysql2
      
      # Whether to convert tinyint columns to bool for this database
      attr_accessor :convert_tinyint_to_bool

      # Connect to the database.  In addition to the usual database options,
      # the following options have effect:
      #
      # :auto_is_null :: Set to true to use MySQL default behavior of having
      #                  a filter for an autoincrement column equals NULL to return the last
      #                  inserted row.
      # :charset :: Same as :encoding (:encoding takes precendence)
      # :encoding :: Set all the related character sets for this
      #              connection (connection, client, database, server, and results).
      #
      # The options hash is also passed to mysql2, and can include mysql2
      # options such as :local_infile.
      def connect(server)
        opts = server_opts(server)
        opts[:host] ||= 'localhost'
        opts[:username] ||= opts.delete(:user)
        opts[:flags] ||= 0
        opts[:flags] |= ::Mysql2::Client::FOUND_ROWS if ::Mysql2::Client.const_defined?(:FOUND_ROWS)
        opts[:encoding] ||= opts[:charset]
        conn = ::Mysql2::Client.new(opts)
        conn.query_options.merge!(:symbolize_keys=>true, :cache_rows=>false)

        sqls = mysql_connection_setting_sqls

        # Set encoding a slightly different way after connecting,
        # in case the READ_DEFAULT_GROUP overrode the provided encoding.
        # Doesn't work across implicit reconnects, but Sequel doesn't turn on
        # that feature.
        if encoding = opts[:encoding]
          sqls.unshift("SET NAMES #{conn.escape(encoding.to_s)}")
        end

        sqls.each{|sql| log_yield(sql){conn.query(sql)}}

        add_prepared_statements_cache(conn)
        conn
      end

      # Return the number of matched rows when executing a delete/update statement.
      def execute_dui(sql, opts=OPTS)
        execute(sql, opts){|c| return c.affected_rows}
      end

      # Return the last inserted id when executing an insert statement.
      def execute_insert(sql, opts=OPTS)
        execute(sql, opts){|c| return c.last_id}
      end

      # Return the version of the MySQL server to which we are connecting.
      def server_version(server=nil)
        @server_version ||= (synchronize(server){|conn| conn.server_info[:id]} || super)
      end

      private

      # Execute the given SQL on the given connection.  If the :type
      # option is :select, yield the result of the query, otherwise
      # yield the connection if a block is given.
      def _execute(conn, sql, opts)
        begin
          stream = opts[:stream]
          r = log_yield((log_sql = opts[:log_sql]) ? sql + log_sql : sql){conn.query(sql, :database_timezone => timezone, :application_timezone => Sequel.application_timezone, :stream=>stream)}
          if opts[:type] == :select
            if r
              if stream
                begin
                  r2 = yield r
                ensure
                  # If r2 is nil, it means the block did not exit normally,
                  # so the rest of the results must be drained to prevent
                  # "commands out of sync" errors.
                  r.each{} unless r2
                end
              else
                yield r
              end
            end
          elsif block_given?
            yield conn
          end
        rescue ::Mysql2::Error => e
          raise_error(e)
        end
      end

      # Set the convert_tinyint_to_bool setting based on the default value.
      def adapter_initialize
        self.convert_tinyint_to_bool = Sequel::MySQL.convert_tinyint_to_bool
      end

      # MySQL connections use the query method to execute SQL without a result
      def connection_execute_method
        :query
      end

      # The MySQL adapter main error class is Mysql2::Error
      def database_error_classes
        [::Mysql2::Error]
      end

      def database_exception_sqlstate(exception, opts)
        exception.sql_state
      end

      # If a connection object is available, try pinging it.  Otherwise, if the
      # error is a Mysql2::Error, check the SQL state and exception message for
      # disconnects.
      def disconnect_error?(e, opts)
        super ||
          ((conn = opts[:conn]) && !conn.ping) ||
          (e.is_a?(::Mysql2::Error) &&
            (e.sql_state =~ /\A08/ ||
             MYSQL_DATABASE_DISCONNECT_ERRORS.match(e.message)))
      end

      # The database name when using the native adapter is always stored in
      # the :database option.
      def database_name
        @opts[:database]
      end

      # Convert tinyint(1) type to boolean if convert_tinyint_to_bool is true
      def schema_column_type(db_type)
        convert_tinyint_to_bool && db_type =~ /\Atinyint\(1\)/ ? :boolean : super
      end
    end

    # Dataset class for MySQL datasets accessed via the native driver.
    class Dataset < Sequel::Dataset
      include Sequel::MySQL::DatasetMethods
      include Sequel::MySQL::PreparedStatements::DatasetMethods
      STREAMING_SUPPORTED = ::Mysql2::VERSION >= '0.3.12'

      Database::DatasetClass = self

      # Yield all rows matching this dataset.
      def fetch_rows(sql)
        execute(sql) do |r|
          @columns = if identifier_output_method
            r.fields.map!{|c| output_identifier(c.to_s)}
          else
            r.fields
          end
          r.each(:cast_booleans=>convert_tinyint_to_bool?){|h| yield h}
        end
        self
      end

      # Use streaming to implement paging if Mysql2 supports it.
      def paged_each(opts=OPTS, &block)
        if STREAMING_SUPPORTED
          stream.each(&block)
        else
          super
        end
      end

      # Return a clone of the dataset that will stream rows when iterating
      # over the result set, so it can handle large datasets that
      # won't fit in memory (Requires mysql 0.3.12+ to have an effect).
      def stream
        clone(:stream=>true)
      end

      private

      # Whether to cast tinyint(1) columns to integer instead of boolean.
      # By default, uses the opposite of the database's convert_tinyint_to_bool
      # setting.  Exists for compatibility with the mysql adapter.
      def convert_tinyint_to_bool?
        @db.convert_tinyint_to_bool
      end

      # Set the :type option to :select if it hasn't been set.
      def execute(sql, opts=OPTS, &block)
        super(sql, {:type=>:select, :stream=>@opts[:stream]}.merge(opts), &block)
      end

      # Handle correct quoting of strings using ::Mysql2::Client#escape.
      def literal_string_append(sql, v)
        sql << APOS << db.synchronize(@opts[:server]){|c| c.escape(v)} << APOS
      end
    end
  end
end
