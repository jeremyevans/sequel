# frozen-string-literal: true

Sequel.require 'adapters/shared/sqlanywhere'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    drv = [
      lambda{Java::sybase.jdbc4.sqlanywhere.IDriver},
      lambda{Java::ianywhere.ml.jdbcodbc.jdbc4.IDriver},
      lambda{Java::sybase.jdbc.sqlanywhere.IDriver},
      lambda{Java::ianywhere.ml.jdbcodbc.jdbc.IDriver},
      lambda{Java::com.sybase.jdbc4.jdbc.Sybdriver},
      lambda{Java::com.sybase.jdbc3.jdbc.Sybdriver}
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
        db.dataset_class = Sequel::JDBC::SqlAnywhere::Dataset
        drv
      end
    end

    # SEQUEL5: Remove
    class Type_Convertor
      def SqlAnywhereBoolean(r, i)
        if v = Short(r, i)
          v != 0
        end
      end
    end

    module SqlAnywhere
      def self.SqlAnywhereBoolean(r, i)
        v = r.getShort(i)
        v != 0 unless r.wasNull
      end

      # Database instance methods for Sybase databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::SqlAnywhere::DatabaseMethods
        include Sequel::JDBC::Transactions

        LAST_INSERT_ID = 'SELECT @@IDENTITY'.freeze
        Sequel::Deprecation.deprecate_constant(self, :LAST_INSERT_ID)

        private

        # Get the last inserted id.
        def last_insert_id(conn, opts=OPTS)
          statement(conn) do |stmt|
            sql = 'SELECT @@IDENTITY'
            rs = log_connection_yield(sql, conn){stmt.executeQuery(sql)}
            rs.next
            rs.getLong(1)
          end
        end
      end

      #Dataset class for Sybase datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::SqlAnywhere::DatasetMethods

        private

        SMALLINT_TYPE = Java::JavaSQL::Types::SMALLINT
        BOOLEAN_METHOD = SqlAnywhere.method(:SqlAnywhereBoolean)

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
