Sequel.require 'adapters/shared/mysql'

module Sequel
  module JDBC
    # Database and Dataset instance methods for MySQL specific
    # support via JDBC.
    module MySQL
      # Database instance methods for MySQL databases accessed via JDBC.
      module DatabaseMethods
        include Sequel::MySQL::DatabaseMethods
        
        # Return instance of Sequel::JDBC::MySQL::Dataset with the given opts.
        def dataset(opts=nil)
          Sequel::JDBC::MySQL::Dataset.new(self, opts)
        end
        
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
              rs = stmt.executeQuery('SELECT LAST_INSERT_ID()')
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
      end
      
      # Dataset class for MySQL datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::MySQL::DatasetMethods
        
        # Use execute_insert to execute the insert_sql.
        def insert(*values)
          execute_insert(insert_sql(*values))
        end
        
        # Use execute_insert to execute the replace_sql.
        def replace(*args)
          execute_insert(replace_sql(*args))
        end
        
        # MySQL on JDBC does provides an accurate number of rows matched.
        def provides_accurate_rows_matched?
          true
        end
      end
    end
  end
end
