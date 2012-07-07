require 'win32ole'

module Sequel
  # The ADO adapter provides connectivity to ADO databases in Windows.
  module ADO
    class Database < Sequel::Database
      DISCONNECT_ERROR_RE = /Communication link failure/

      set_adapter_scheme :ado

      def initialize(opts)
        super
        case @opts[:conn_string]
        when /Microsoft\.(Jet|ACE)\.OLEDB/io
          Sequel.ts_require 'adapters/shared/access'
          extend Sequel::Access::DatabaseMethods
          extend_datasets(Sequel::Access::DatasetMethods)
        else
          @opts[:driver] ||= 'SQL Server'
          case @opts[:driver]
          when 'SQL Server'
            Sequel.ts_require 'adapters/ado/mssql'
            extend Sequel::ADO::MSSQL::DatabaseMethods
            @dataset_class = ADO::MSSQL::Dataset
            set_mssql_unicode_strings
          end
        end
      end

      # In addition to the usual database options,
      # the following options have an effect:
      #
      # :command_timeout :: Sets the time in seconds to wait while attempting
      #                     to execute a command before cancelling the attempt and generating
      #                     an error. Specifically, it sets the ADO CommandTimeout property.
      #                     If this property is not set, the default of 30 seconds is used.
      # :driver :: The driver to use in the ADO connection string.  If not provided, a default
      #            of "SQL Server" is used.
      # :conn_string :: The full ADO connection string.  If this is provided,
      #                 the usual options are ignored.
      # :provider :: Sets the Provider of this ADO connection (for example, "SQLOLEDB").
      #              If you don't specify a provider, the default one used by WIN32OLE
      #              has major problems, such as creating a new native database connection
      #              for every query, which breaks things such as temporary tables.
      #
      # Pay special attention to the :provider option, as without specifying a provider,
      # many things will be broken.  The SQLNCLI10 provider appears to work well if you
      # are connecting to Microsoft SQL Server, but it is not the default as that would
      # break backwards compatability.
      def connect(server)
        opts = server_opts(server)
        s = opts[:conn_string] || "driver=#{opts[:driver]};server=#{opts[:host]};database=#{opts[:database]}#{";uid=#{opts[:user]};pwd=#{opts[:password]}" if opts[:user]}"
        handle = WIN32OLE.new('ADODB.Connection')
        handle.CommandTimeout = opts[:command_timeout] if opts[:command_timeout]
        handle.Provider = opts[:provider] if opts[:provider]
        handle.Open(s)
        handle
      end

      def execute(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            r = log_yield(sql){conn.Execute(sql)}
            yield(r) if block_given?
          rescue ::WIN32OLERuntimeError => e
            raise_error(e)
          end
        end
        nil
      end
      alias do execute

      private

      # The ADO adapter's default provider doesn't support transactions, since it
      # creates a new native connection for each query.  So Sequel only attempts
      # to use transactions if an explicit :provider is given.
      def begin_transaction(conn, opts={})
        super if @opts[:provider]
      end

      def commit_transaction(conn, opts={})
        super if @opts[:provider]
      end

      def database_error_classes
        [::WIN32OLERuntimeError]
      end

      def disconnect_connection(conn)
        conn.Close
      end

      def disconnect_error?(e, opts)
        super || (e.is_a?(::WIN32OLERuntimeError) && e.message =~ DISCONNECT_ERROR_RE)
      end

      def rollback_transaction(conn, opts={})
        super if @opts[:provider]
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) do |s|
          columns = cols = s.Fields.extend(Enumerable).map{|column| output_identifier(column.Name)}
          if opts[:offset] && offset_returns_row_number_column?
            rn = row_number_column
            columns = columns.dup
            columns.delete(rn)
          end
          @columns = columns
          s.getRows.transpose.each do |r|
            row = {}
            cols.each{|c| row[c] = r.shift}
            row.delete(rn) if rn
            yield row
          end unless s.eof
        end
      end

      # ADO returns nil for all for delete and update statements.
      def provides_accurate_rows_matched?
        false
      end
    end
  end
end
