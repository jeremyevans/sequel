Sequel.require 'adapters/shared/sqlanywhere'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    class TypeConvertor
      def SqlAnywhereBoolean(r, i)
        if v = Short(r, i)
          v != 0
        end
      end
    end

    module SqlAnywhere
      # Database instance methods for Sybase databases accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::SqlAnywhere::DatabaseMethods
        include Sequel::JDBC::Transactions

        LAST_INSERT_ID = 'SELECT @@IDENTITY'.freeze

        private

        # Get the last inserted id.
        def last_insert_id(conn, opts=OPTS)
          statement(conn) do |stmt|
            sql = LAST_INSERT_ID
            rs = log_yield(sql){stmt.executeQuery(sql)}
            rs.next
            rs.getInt(1)
          end
        end

        def setup_type_convertor_map
          super
          @type_convertor_map[:SqlAnywhereBoolean] = TypeConvertor::INSTANCE.method(:SqlAnywhereBoolean)
        end
      end

      #Dataset class for Sybase datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::SqlAnywhere::DatasetMethods

        private

        SMALLINT_TYPE = Java::JavaSQL::Types::SMALLINT

        def type_convertor(map, meta, type, i)
          if convert_smallint_to_bool && type == SMALLINT_TYPE
            map[:SqlAnywhereBoolean]
          else
            super
          end
        end
      end
    end
  end
end
