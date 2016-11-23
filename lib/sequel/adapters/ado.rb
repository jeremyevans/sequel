# frozen-string-literal: true

require 'win32ole'

module Sequel
  # The ADO adapter provides connectivity to ADO databases in Windows.
  module ADO
    class Database < Sequel::Database
      DISCONNECT_ERROR_RE = /Communication link failure/

      set_adapter_scheme :ado

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
      
      def disconnect_connection(conn)
        conn.Close
      rescue WIN32OLERuntimeError
        nil
      end

      # Just execute so it doesn't attempt to return the number of rows modified.
      def execute_ddl(sql, opts=OPTS)
        execute(sql, opts)
      end

      # Just execute so it doesn't attempt to return the number of rows modified.
      def execute_insert(sql, opts=OPTS)
        execute(sql, opts)
      end
      
      # Use pass by reference in WIN32OLE to get the number of affected rows,
      # unless is a provider is in use (since some providers don't seem to
      # return the number of affected rows, but the default provider appears
      # to).
      def execute_dui(sql, opts=OPTS)
        return super if opts[:provider]
        synchronize(opts[:server]) do |conn|
          begin
            log_connection_yield(sql, conn){conn.Execute(sql, 1)}
            WIN32OLE::ARGV[1]
          rescue ::WIN32OLERuntimeError => e
            raise_error(e)
          end
        end
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            r = log_connection_yield(sql, conn){conn.Execute(sql)}
            begin
              yield r if block_given?
            ensure
              begin
                r.close
              rescue ::WIN32OLERuntimeError
              end
            end
          rescue ::WIN32OLERuntimeError => e
            raise_error(e)
          end
        end
        nil
      end

      private
      
      def adapter_initialize
        case @opts[:conn_string]
        when /Microsoft\.(Jet|ACE)\.OLEDB/io
          Sequel.require 'adapters/ado/access'
          extend Sequel::ADO::Access::DatabaseMethods
          self.dataset_class = ADO::Access::Dataset
        else
          @opts[:driver] ||= 'SQL Server'
          case @opts[:driver]
          when 'SQL Server'
            Sequel.require 'adapters/ado/mssql'
            extend Sequel::ADO::MSSQL::DatabaseMethods
            self.dataset_class = ADO::MSSQL::Dataset
            set_mssql_unicode_strings
          end
        end
        super
      end

      # The ADO adapter's default provider doesn't support transactions, since it 
      # creates a new native connection for each query.  So Sequel only attempts
      # to use transactions if an explicit :provider is given.
      def begin_transaction(conn, opts=OPTS)
        super if @opts[:provider]
      end

      def commit_transaction(conn, opts=OPTS)
        super if @opts[:provider]
      end

      def database_error_classes
        [::WIN32OLERuntimeError]
      end

      def disconnect_error?(e, opts)
        super || (e.is_a?(::WIN32OLERuntimeError) && e.message =~ DISCONNECT_ERROR_RE)
      end

      def rollback_transaction(conn, opts=OPTS)
        super if @opts[:provider]
      end
    end
    
    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      # ADO constants (DataTypeEnum)
      # Source: https://msdn.microsoft.com/en-us/library/ms675318(v=vs.85).aspx
      AdBigInt           = 20
      AdBinary           = 128
      AdBoolean          = 11
      AdBSTR             = 8
      AdChapter          = 136
      AdChar             = 129
      AdCurrency         = 6
      AdDate             = 7
      AdDBDate           = 133
      AdDBTime           = 134
      AdDBTimeStamp      = 135
      AdDecimal          = 14
      AdDouble           = 5
      AdEmpty            = 0
      AdError            = 10
      AdFileTime         = 64
      AdGUID             = 72
      AdIDispatch        = 9
      AdInteger          = 3
      AdIUnknown         = 13
      AdLongVarBinary    = 205
      AdLongVarChar      = 201
      AdLongVarWChar     = 203
      AdNumeric          = 131
      AdPropVariant      = 138
      AdSingle           = 4
      AdSmallInt         = 2
      AdTinyInt          = 16
      AdUnsignedBigInt   = 21
      AdUnsignedInt      = 19
      AdUnsignedSmallInt = 18
      AdUnsignedTinyInt  = 17
      AdUserDefined      = 132
      AdVarBinary        = 204
      AdVarChar          = 200
      AdVariant          = 12
      AdVarNumeric       = 139
      AdVarWChar         = 202
      AdWChar            = 130

      def fetch_rows(sql)
        execute(sql) do |recordset|
          field_names = []
          field_types = []

          recordset.Fields.each do |field|
            field_names << output_identifier(field.Name)
            field_types << field.Type
          end

          self.columns = field_names
          return if recordset.EOF

          recordset.GetRows.transpose.each do |field_values|
            field_index = -1
            field_values.map! do |v|
              field_index += 1
              case field_types[field_index]
              when AdBigInt
                v && v.to_i
              when AdNumeric, AdVarNumeric
                v && BigDecimal.new(v)
              when AdDBDate
                v && Date.new(v.year, v.month, v.day)
              when AdDBTimeStamp
                v && db.to_application_timestamp([v.year, v.month, v.day, v.hour, v.min, v.sec, v.nsec])
              when AdBinary, AdVarBinary, AdLongVarBinary
                v && Sequel.blob(v.pack('c*'))
              else
                v
              end
            end
            yield Hash[field_names.zip(field_values)]
          end
        end
      end
      
      # ADO returns nil for all for delete and update statements.
      def provides_accurate_rows_matched?
        false
      end
    end
  end
end
