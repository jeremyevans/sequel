Sequel.require 'adapters/utils/date_format'
require 'win32ole'

module Sequel
  # The ADO adapter provides connectivity to ADO databases in Windows. ADO
  # databases can be opened using a URL with the ado schema:
  #
  #   DB = Sequel.connect('ado://mydb')
  # 
  # or using the Sequel.ado method:
  #
  #   DB = Sequel.ado('mydb')
  #
  module ADO
    class Database < Sequel::Database
      set_adapter_scheme :ado

      def initialize(opts)
        super(opts)
        opts[:driver] ||= 'SQL Server'
        case opts[:driver]
        when 'SQL Server'
          Sequel.require 'adapters/shared/mssql'
          extend Sequel::MSSQL::DatabaseMethods
        end
      end

      def connect(server)
        opts = server_opts(server)
        s = "driver=#{opts[:driver]};server=#{opts[:host]};database=#{opts[:database]}#{";uid=#{opts[:user]};pwd=#{opts[:password]}" if opts[:user]}"
        handle = WIN32OLE.new('ADODB.Connection')
        handle.Open(s)
        handle
      end
      
      def dataset(opts = nil)
        ADO::Dataset.new(self, opts)
      end
    
      def execute(sql, opts={})
        log_info(sql)
        synchronize(opts[:server]) do |conn|
          r = conn.Execute(sql)
          yield(r) if block_given?
          r
        end
      end
      alias_method :do, :execute

      private

      def disconnect_connection(conn)
        conn.Close
      end
    end
    
    class Dataset < Sequel::Dataset
      include Dataset::SQLStandardDateFormat

      def fetch_rows(sql)
        execute(sql) do |s|
          @columns = s.Fields.extend(Enumerable).map do |column|
            name = column.Name.empty? ? '(no column name)' : column.Name
            output_identifier(name)
          end
          
          unless s.eof
            s.moveFirst
            s.getRows.transpose.each {|r| yield hash_row(r)}
          end
        end
        self
      end
      
      private
      
      def hash_row(row)
        @columns.inject({}) do |m, c|
          m[c] = row.shift
          m
        end
      end
    end
  end
end
