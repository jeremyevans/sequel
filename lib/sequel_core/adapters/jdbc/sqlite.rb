require 'sequel_core/adapters/shared/sqlite'

module Sequel
  module JDBC
    module SQLite
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods
        
        def dataset(opts=nil)
          Sequel::JDBC::SQLite::Dataset.new(self, opts)
        end
        
        def execute_insert(sql)
          begin
            log_info(sql)
            @pool.hold do |conn|
              stmt = conn.createStatement
              begin
                stmt.executeUpdate(sql)
                rs = stmt.executeQuery('SELECT last_insert_rowid()')
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
        
        def transaction
          @pool.hold do |conn|
            @transactions ||= []
            return yield(conn) if @transactions.include?(Thread.current)
            stmt = conn.createStatement
            begin
              log_info(Sequel::Database::SQL_BEGIN)
              stmt.execute(Sequel::Database::SQL_BEGIN)
              @transactions << Thread.current
              yield(conn)
            rescue Exception => e
              log_info(Sequel::Database::SQL_ROLLBACK)
              stmt.execute(Sequel::Database::SQL_ROLLBACK)
              raise e unless Error::Rollback === e
            ensure
              unless e
                log_info(Sequel::Database::SQL_COMMIT)
                stmt.execute(Sequel::Database::SQL_COMMIT)
              end
              stmt.close
              @transactions.delete(Thread.current)
            end
          end
        end
        
        private
        
        def connection_pool_default_options
          o = super
          uri == 'jdbc:sqlite::memory:' ? o.merge(:max_connections=>1) : o
        end
      end
    
      class Dataset < JDBC::Dataset
        include Sequel::SQLite::DatasetMethods
      end
    end
  end
end
