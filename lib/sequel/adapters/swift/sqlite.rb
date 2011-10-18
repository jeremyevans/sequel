Sequel.require 'adapters/shared/sqlite'

module Sequel
  module Swift
    # Database and Dataset instance methods for SQLite specific
    # support via Swift.
    module SQLite
      # Database instance methods for SQLite databases accessed via Swift.
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods

        # Set the correct pragmas on the connection.
        def connect(opts)
          c = super
          connection_pragmas.each{|s| log_yield(s){c.execute(s)}}
          c
        end
        
        # Return instance of Sequel::Swift::SQL::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::Swift::SQLite::Dataset.new(self, opts)
        end
      end
      
      # Dataset class for SQLite datasets accessed via Swift.
      class Dataset < Swift::Dataset
        include Sequel::SQLite::DatasetMethods
        
        private
        
        # Use Swift's escape method for quoting.
        def literal_string(s)
          db.synchronize{|c| "#{c.escape(s)}"}
        end
      end
    end
  end
end
