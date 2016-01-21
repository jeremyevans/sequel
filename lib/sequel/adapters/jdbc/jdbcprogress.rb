# frozen-string-literal: true

Sequel::JDBC.load_driver('com.progress.sql.jdbc.JdbcProgressDriver')
Sequel.require 'adapters/shared/progress'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:jdbcprogress] = proc do |db|
        db.extend(Sequel::JDBC::Progress::DatabaseMethods)
        db.extend_datasets Sequel::Progress::DatasetMethods
        com.progress.sql.jdbc.JdbcProgressDriver
      end
    end

    # Database and Dataset instance methods for Progress v9 specific
    # support via JDBC.
    module Progress
      # Database instance methods for Progress databases accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::Progress::DatabaseMethods
        include Sequel::JDBC::Transactions

        # Progress DatabaseMetaData doesn't even implement supportsSavepoints()
        def supports_savepoints?
          false
        end
      end
    end
  end
end
