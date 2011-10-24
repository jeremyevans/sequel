Sequel.require 'adapters/shared/mysql'

module Sequel
  module Swift
    # Database and Dataset instance methods for MySQL specific
    # support via Swift.
    module MySQL
      # Database instance methods for MySQL databases accessed via Swift.
      module DatabaseMethods
        include Sequel::MySQL::DatabaseMethods
        
        private
        
        # The database name for the given database.
        def database_name
          opts[:database]
        end

        # Consider tinyint(1) columns as boolean.
        def schema_column_type(db_type)
          db_type == 'tinyint(1)' ? :boolean : super
        end
      
        # By default, MySQL 'where id is null' selects the last inserted id.
        # Turn that off unless explicitly enabled.
        def setup_connection(conn)
          super
          sql = "SET SQL_AUTO_IS_NULL=0"
          log_yield(sql){conn.execute(sql)} unless opts[:auto_is_null]
          conn
        end
      end
      
      # Dataset class for MySQL datasets accessed via Swift.
      class Dataset < Swift::Dataset
        include Sequel::MySQL::DatasetMethods
        
        # Use execute_insert to execute the replace_sql.
        def replace(*args)
          execute_insert(replace_sql(*args))
        end
        
        private
        
        # Use Swift's escape method for quoting.
        def literal_string(s)
          db.synchronize{|c| "'#{c.escape(s)}'"}
        end
      end
    end
  end
end
