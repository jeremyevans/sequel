Sequel::JDBC.load_driver('com.foundationdb.sql.jdbc.Driver')
Sequel.require 'adapters/shared/fdbsql'

module Sequel
  Fdbsql::CONVERTED_EXCEPTIONS << NativeException

  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:fdbsql] = proc do |db|
        db.extend(Sequel::JDBC::Fdbsql::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Fdbsql::Dataset
        com.foundationdb.sql.jdbc.Driver
      end
    end

    # Adapter, Database, and Dataset support for accessing the FoundationDB SQL Layer
    # via JDBC
    module Fdbsql
      # Methods to add to Database instances that access Fdbsql via
      # JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::Fdbsql::DatabaseMethods

        # Add the primary_keys and primary_key_sequences instance variables,
        # so we can get the correct return values for inserted rows.
        def self.extended(db)
          super
          db.send(:adapter_initialize)
        end

        private

        DISCONNECT_ERROR_RE = /\A(?:This connection has been closed|An I\/O error occurred while sending to the backend)/
        def disconnect_error?(exception, opts)
          super || exception.message =~ DISCONNECT_ERROR_RE
        end

        def database_exception_sqlstate(exception, opts)
          if exception.respond_to?(:sql_state)
            exception.sql_state
          end
        end
      end

      # Methods to add to Dataset instances that access the FoundationDB SQL Layer via
      # JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::Fdbsql::DatasetMethods

        # Add the shared Fdbsql prepared statement methods
        def prepare(type, name=nil, *values)
          ps = to_prepared_statement(type, values)
          ps.extend(JDBC::Dataset::PreparedStatementMethods)
          ps.extend(::Sequel::Fdbsql::DatasetMethods::PreparedStatementMethods)
          if name
            ps.prepared_statement_name = name
            db.set_prepared_statement(name, ps)
          end
          ps
        end
      end
    end
  end
end
