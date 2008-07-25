require 'sequel_core/adapters/shared/mysql'

module Sequel
  module JDBC
    module MySQL
      module DatabaseMethods
        include Sequel::MySQL::DatabaseMethods
        
        def dataset(opts=nil)
          Sequel::JDBC::MySQL::Dataset.new(self, opts)
        end
        
        def execute_insert(sql)
          begin
            log_info(sql)
            @pool.hold do |conn|
              stmt = conn.createStatement
              begin
                stmt.executeUpdate(sql)
                rs = stmt.executeQuery('SELECT LAST_INSERT_ID()')
                rs.next
                rs.getInt(1)
              rescue NativeException, JavaSQL::SQLException => e
                raise Error, e.message
              ensure
                stmt.close
              end
            end
          rescue NativeException, JavaSQL::SQLException => e
            raise Error, "#{sql}\r\n#{e.message}"
          end
        end
        
        private
        
        def database_name
          u = URI.parse(uri.sub(/\Ajdbc:/, ''))
          (m = /\/(.*)/.match(u.path)) && m[1]
        end
      end
    
      class Dataset < JDBC::Dataset
        include Sequel::MySQL::DatasetMethods
        
        def insert(*values)
          @db.execute_insert(insert_sql(*values))
        end
        
        def replace(*args)
          @db.execute_insert(replace_sql(*args))
        end
      end
    end
  end
end
