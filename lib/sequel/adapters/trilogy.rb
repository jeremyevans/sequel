# frozen-string-literal: true

require 'trilogy'
require_relative 'shared/mysql'

module Sequel
  module Trilogy
    class Database < Sequel::Database
      include Sequel::MySQL::DatabaseMethods

      QUERY_FLAGS = ::Trilogy::QUERY_FLAGS_CAST | ::Trilogy::QUERY_FLAGS_CAST_BOOLEANS
      LOCAL_TIME_QUERY_FLAGS = QUERY_FLAGS | ::Trilogy::QUERY_FLAGS_LOCAL_TIMEZONE

      set_adapter_scheme :trilogy
      
      # Connect to the database.  See Trilogy documentation for options.
      def connect(server)
        opts = server_opts(server)
        opts[:username] ||= opts.delete(:user)
        opts[:found_rows] = true
        conn = ::Trilogy.new(opts)
        mysql_connection_setting_sqls.each{|sql| log_connection_yield(sql, conn){conn.query(sql)}}
        conn
      end

      def disconnect_connection(c)
        c.discard!
      rescue ::Trilogy::Error
        nil
      end

      # Execute the given SQL on the given connection and yield the result.
      def execute(sql, opts)
        r = synchronize(opts[:server]) do |conn|
          log_connection_yield((log_sql = opts[:log_sql]) ? sql + log_sql : sql, conn) do
            conn.query_with_flags(sql, timezone.nil? || timezone == :local ? LOCAL_TIME_QUERY_FLAGS : QUERY_FLAGS)
          end
        end
        yield r
      rescue ::Trilogy::Error => e
        raise_error(e)
      end

      def execute_dui(sql, opts=OPTS)
        execute(sql, opts, &:affected_rows)
      end

      def execute_insert(sql, opts=OPTS)
        execute(sql, opts, &:last_insert_id)
      end

      def freeze
        server_version
        super
      end

      # Return the version of the MySQL server to which we are connecting.
      def server_version(_server=nil)
        @server_version ||= super()
      end

      private

      def database_specific_error_class(exception, opts)
        if exception.error_code == 1205
          DatabaseLockTimeout
        else
          super
        end
      end

      def connection_execute_method
        :query
      end

      def database_error_classes
        [::Trilogy::Error]
      end

      def dataset_class_default
        Dataset
      end

      # Convert tinyint(1) type to boolean if convert_tinyint_to_bool is true
      def schema_column_type(db_type)
        db_type.start_with?("tinyint(1)") ? :boolean : super
      end
    end

    class Dataset < Sequel::Dataset
      include Sequel::MySQL::DatasetMethods

      def fetch_rows(sql)
        execute(sql) do |r|
          self.columns = r.fields.map!{|c| output_identifier(c.to_s)}
          r.each_hash{|h| yield h}
        end
        self
      end
      
      private

      def execute(sql, opts=OPTS)
        opts = Hash[opts]
        opts[:type] = :select
        super
      end

      # Handle correct quoting of strings using ::Trilogy#escape.
      def literal_string_append(sql, v)
        sql << "'" << db.synchronize(@opts[:server]){|c| c.escape(v)} << "'"
      end
    end
  end
end

