Sequel.require 'adapters/shared/mysql'

module Sequel
  module JDBC
    # Database and Dataset instance methods for MySQL specific
    # support via JDBC.
    module MySQL
      # Database instance methods for MySQL databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::MySQL::DatabaseMethods
        LAST_INSERT_ID = 'SELECT LAST_INSERT_ID()'.freeze
        
        private
        
        # The database name for the given database.  Need to parse it out
        # of the connection string, since the JDBC does no parsing on the
        # given connection string by default.
        def database_name
          u = URI.parse(uri.sub(/\Ajdbc:/, ''))
          (m = /\/(.*)/.match(u.path)) && m[1]
        end
        
        # Get the last inserted id using LAST_INSERT_ID().
        def last_insert_id(conn, opts={})
          if stmt = opts[:stmt]
            rs = stmt.getGeneratedKeys
            begin
              if rs.next
                rs.getInt(1)
              else
                0
              end
            ensure
              rs.close
            end
          else
            statement(conn) do |stmt|
              rs = stmt.executeQuery(LAST_INSERT_ID)
              rs.next
              rs.getInt(1)
            end
          end
        end

        # MySQL 5.1.12 JDBC adapter requires this to be true,
        # and previous versions don't mind.
        def requires_return_generated_keys?
          true
        end
      
        # Convert tinyint(1) type to boolean
        def schema_column_type(db_type)
          db_type == 'tinyint(1)' ? :boolean : super
        end
      
        # By default, MySQL 'where id is null' selects the last inserted id.
        # Turn that off unless explicitly enabled.
        def setup_connection(conn)
          super
          sql = "SET SQL_AUTO_IS_NULL=0"
          statement(conn){|s| log_yield(sql){s.execute(sql)}} unless opts[:auto_is_null]
          conn
        end
      end
      
      # Dataset class for MySQL datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::MySQL::DatasetMethods
      end
    end
  end
end
