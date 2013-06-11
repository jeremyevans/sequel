require 'swift/db/sqlite3'
Sequel.require 'adapters/shared/sqlite'

module Sequel
  module Swift
    # Database and Dataset instance methods for SQLite specific
    # support via Swift.
    module SQLite
      # Database instance methods for SQLite databases accessed via Swift.
      module DatabaseMethods
        extend Sequel::Database::ResetIdentifierMangling
        include Sequel::SQLite::DatabaseMethods

        # Set the correct pragmas on the connection.
        def connect(opts)
          c = super
          connection_pragmas.each{|s| log_yield(s){c.execute(s)}}
          c
        end
      end
      
      # Dataset class for SQLite datasets accessed via Swift.
      class Dataset < Swift::Dataset
        include Sequel::SQLite::DatasetMethods
        
        private
        
        # Use Swift's escape method for quoting.
        def literal_string_append(sql, s)
          sql << APOS << db.synchronize(@opts[:server]){|c| c.escape(s)} << APOS
        end
      end
    end
  end
end
