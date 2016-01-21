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
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::MSSQL::DatabaseMethods
        LAST_INSERT_ID_SQL='SELECT SCOPE_IDENTITY()'.freeze
        
        # Return the last inserted identity value.
        def execute_insert(sql, opts=OPTS)
          synchronize(opts[:server]) do |conn|
            begin
              log_yield(sql){conn.do(sql)}
              begin
                s = log_yield(LAST_INSERT_ID_SQL){conn.run(LAST_INSERT_ID_SQL)}
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

        private

        def default_timestamp_format
          TIMESTAMP_FORMAT
        end

        # Use ODBC format, not Microsoft format, as the ODBC layer does
        # some translation.
        def literal_date(v)
          v.strftime(ODBC_DATE_FORMAT)
        end
      end
    end
  end
end
