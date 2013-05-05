require 'dbi'

module Sequel
  module DBI
    class Database < Sequel::Database
      set_adapter_scheme :dbi
      
      DBI_ADAPTERS = {
        :ado => "ADO",
        :db2 => "DB2",
        :frontbase => "FrontBase",
        :interbase => "InterBase",
        :msql => "Msql",
        :mysql => "Mysql",
        :odbc => "ODBC",
        :oracle => "Oracle",
        :pg => "pg",
        :proxy => "Proxy",
        :sqlite => "SQLite",
        :sqlrelay => "SQLRelay"
      }
      
      # Converts a uri to an options hash. These options are then passed
      # to a newly created database object.
      def self.uri_to_options(uri) # :nodoc:
        database = (m = /\/(.*)/.match(uri.path)) && (m[1])
        if m = /dbi-(.+)/.match(uri.scheme)
          adapter = DBI_ADAPTERS[m[1].to_sym] || m[1]
          database = "#{adapter}:dbname=#{database}"
        end
        {
          :user => uri.user,
          :password => uri.password,
          :host => uri.host,
          :port => uri.port,
          :database => database
        }
      end

      private_class_method :uri_to_options
      
      def connect(server)
        opts = server_opts(server)
        dbname = opts[:database]
        if dbname !~ /^DBI:/ then
          dbname = "DBI:#{dbname}"
          [:host, :port].each{|sym| dbname += ";#{sym}=#{opts[sym]}" unless blank_object?(opts[sym])}
        end
        ::DBI.connect(dbname, opts[:user], opts[:password])
      end

      def disconnect_connection(c)
        c.disconnect
      end
      
      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          r = log_yield(sql){conn.execute(sql)}
          yield(r) if block_given?
          r
        end
      end
      
      def execute_dui(sql, opts=OPTS)
        synchronize(opts[:server]){|conn| log_yield(sql){conn.do(sql)}}
      end

      private

      def adapter_initialize
        case @opts[:db_type]
        when 'mssql'
          Sequel.require 'adapters/shared/mssql'
          extend Sequel::MSSQL::DatabaseMethods
          extend_datasets Sequel::MSSQL::DatasetMethods
        end
      end
    end
    
    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) do |s|
          begin
            columns = cols = s.column_names.map{|c| output_identifier(c)}
            @columns = columns
            s.fetch do |r|
              row = {}
              cols.each{|c| row[c] = r.shift}
              yield row
            end
          ensure
            s.finish rescue nil
          end
        end
        self
      end
    end
  end
end
