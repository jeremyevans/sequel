Sequel.require 'adapters/shared/openedge'
Sequel.require 'adapters/jdbc/transactions'

module Sequel
  module JDBC
    # Database and Dataset instance methods for OpenEdge v9 specific
    # support via JDBC.
    module OpenEdge
      # Database instance methods for OpenEdge databases accessed via JDBC.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::OpenEdge::DatabaseMethods
        include Sequel::JDBC::Transactions
      end
    end
  end
end
