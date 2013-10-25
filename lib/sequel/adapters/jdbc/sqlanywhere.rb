Sequel.require 'adapters/shared/sqlanywhere'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
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
      end

      #Dataset class for Sybase datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::SqlAnywhere::DatasetMethods

        private

        class ::Sequel::JDBC::Dataset::TYPE_TRANSLATOR
          def sqla_boolean(i) i != 0 end
        end

        BOOLEAN_METHOD =  TYPE_TRANSLATOR_INSTANCE.method(:sqla_boolean)

        def convert_type_proc(v, ctn=nil)
          case
          when ctn && ctn =~ SqlAnywhere::DatabaseMethods::SMALLINT_RE
            BOOLEAN_METHOD
          else
            super(v, ctn)
          end
        end

        # SQLAnywhere needs the column info if it is converting smallint to bool,
        # since the JDBC adapter always returns smallint as integer.
        def convert_type_proc_uses_column_info?
          convert_smallint_to_bool
        end
      end
    end
  end
end
