# frozen-string-literal: true

Sequel.require 'adapters/shared/mssql'

module Sequel
  module ODBC
    Sequel.synchronize do
      DATABASE_SETUP[:mssql] = proc do |db|
        db.extend Sequel::ODBC::MSSQL::DatabaseMethods
        db.dataset_class = Sequel::ODBC::MSSQL::Dataset
        db.send(:set_mssql_unicode_strings)
      end
    end

    # Database and Dataset instance methods for MSSQL specific
    # support via ODBC.
    module MSSQL
      module DatabaseMethods
        include Sequel::MSSQL::DatabaseMethods
        LAST_INSERT_ID_SQL='SELECT SCOPE_IDENTITY()'.freeze
        Sequel::Deprecation.deprecate_constant(self, :LAST_INSERT_ID_SQL)
        
        # Return the last inserted identity value.
        def execute_insert(sql, opts=OPTS)
          synchronize(opts[:server]) do |conn|
            begin
              log_connection_yield(sql, conn){conn.do(sql)}
              begin
                last_insert_id_sql = 'SELECT SCOPE_IDENTITY()'
                s = log_connection_yield(last_insert_id_sql, conn){conn.run(last_insert_id_sql)}
                if (rows = s.fetch_all) and (row = rows.first) and (v = row.first)
                  Integer(v)
                end
              ensure
                s.drop if s
              end
            rescue ::ODBC::Error => e
              raise_error(e)
            end
          end
        end
      end
      class Dataset < ODBC::Dataset
        include Sequel::MSSQL::DatasetMethods

        # Use ODBC format, not Microsoft format, as the ODBC layer does
        # some translation.  MSSQL version is over-ridden to allow 3 millisecond decimal places        
        TIMESTAMP_FORMAT="{ts '%Y-%m-%d %H:%M:%S%N'}".freeze
        Sequel::Deprecation.deprecate_constant(self, :TIMESTAMP_FORMAT)

        private

        def default_timestamp_format
          "{ts '%Y-%m-%d %H:%M:%S%N'}"
        end

        # Use ODBC format, not Microsoft format, as the ODBC layer does
        # some translation.
        def literal_date(v)
          v.strftime("{d '%Y-%m-%d'}")
        end
      end
    end
  end
end
