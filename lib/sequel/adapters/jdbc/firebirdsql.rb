# frozen-string-literal: true

Sequel::JDBC.load_driver('org.firebirdsql.jdbc.FBDriver')
Sequel.require 'adapters/shared/firebird'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:firebirdsql] = proc do |db|
        db.extend(Sequel::JDBC::Firebird::DatabaseMethods)
        db.extend_datasets Sequel::Firebird::DatasetMethods
        org.firebirdsql.jdbc.FBDriver
      end
    end

    # Database and Dataset instance methods for Firebird specific
    # support via JDBC.
    module Firebird
      # Database instance methods for Firebird databases accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::Firebird::DatabaseMethods
        include Sequel::JDBC::Transactions
        
        # Add the primary_keys and primary_key_sequences instance variables,
        # so we can get the correct return values for inserted rows.
        def self.extended(db)
          db.instance_eval do
            @primary_keys = {}
          end
        end
      end
    end
  end
end
