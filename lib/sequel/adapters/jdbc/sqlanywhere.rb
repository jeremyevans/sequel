# frozen-string-literal: true

require_relative '../shared/sqlanywhere'
require_relative 'transactions'

module Sequel
  module JDBC
    drv = [
      lambda{Java::SybaseJdbc4Sqlanywhere::IDriver},
      lambda{Java::IanywhereMlJdbcodbcJdbc4::IDriver},
      lambda{Java::SybaseJdbcSqlanywhere::IDriver},
      lambda{Java::IanywhereMlJdbcodbcJdbc::IDriver},
      lambda{Java::ComSybaseJdbc4Jdbc::Sybdriver},
      lambda{Java::ComSybaseJdbc3Jdbc::Sybdriver}
    ].each do |class_proc|
      begin
        break class_proc.call
      rescue NameError
      end
    end
    raise(Sequel::AdapterNotFound, "no suitable SQLAnywhere JDBC driver found") unless drv

    Sequel.synchronize do
      DATABASE_SETUP[:sqlanywhere] = proc do |db|
        db.extend(Sequel::JDBC::SqlAnywhere::DatabaseMethods)
        db.convert_smallint_to_bool = true
        db.dataset_class = Sequel::JDBC::SqlAnywhere::Dataset
        drv
      end
    end

    module SqlAnywhere
      module DatabaseMethods
        include Sequel::SqlAnywhere::DatabaseMethods
        include Sequel::JDBC::Transactions

        private

        def database_exception_use_sqlstates?
          false
        end

        # Use @@IDENTITY to get the last inserted id
        def last_insert_id(conn, opts=OPTS)
          statement(conn) do |stmt|
            sql = 'SELECT @@IDENTITY'
            rs = log_connection_yield(sql, conn){stmt.executeQuery(sql)}
            rs.next
            rs.getLong(1)
          end
        end
      end

      class Dataset < JDBC::Dataset
        include Sequel::SqlAnywhere::DatasetMethods

        private

        # JDBC SQLAnywhere driver does not appear to handle fractional
        # times correctly.
        def default_time_format
          "'%H:%M:%S'"
        end

        # Set to zero to work around JDBC SQLAnywhere driver bug.
        def sqltime_precision
          0
        end

        SMALLINT_TYPE = Java::JavaSQL::Types::SMALLINT
        BOOLEAN_METHOD = Object.new
        def BOOLEAN_METHOD.call(r, i)
          v = r.getShort(i)
          v != 0 unless r.wasNull
        end

        def type_convertor(map, meta, type, i)
          if convert_smallint_to_bool && type == SMALLINT_TYPE
            BOOLEAN_METHOD
          else
            super
          end
        end
      end
    end
  end
end
