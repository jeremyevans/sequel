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
      end
    end
  end
end
