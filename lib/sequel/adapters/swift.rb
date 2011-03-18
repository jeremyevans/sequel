require 'swift'

module Sequel
  # Module holding the Swift support for Sequel.  Swift is a
  # ruby front-end for dbic++, a fast database access library
  # written in C++.
  #
  # The Swift adapter currently supports PostgreSQL and MySQL:
  #
  #   Sequel.connect('swift://user:password@host/database?db_type=postgres')
  #   Sequel.connect('swift://user:password@host/database?db_type=mysql')
  module Swift
    # Contains procs keyed on sub adapter type that extend the
    # given database object so it supports the correct database type.
    DATABASE_SETUP = {:postgres=>proc do |db|
        Sequel.ts_require 'adapters/swift/postgres'
        db.extend(Sequel::Swift::Postgres::DatabaseMethods)
        db.swift_class = ::Swift::DB::Postgres
      end,
      :mysql=>proc do |db|
        Sequel.ts_require 'adapters/swift/mysql'
        db.extend(Sequel::Swift::MySQL::DatabaseMethods)
        db.swift_class = ::Swift::DB::Mysql
      end,
      :sqlite=>proc do |db|
        Sequel.ts_require 'adapters/swift/sqlite'
        db.extend(Sequel::Swift::SQLite::DatabaseMethods)
        db.swift_class = ::Swift::DB::Sqlite3
      end,
    }
      
    class Database < Sequel::Database
      set_adapter_scheme :swift

      # The Swift adapter class being used by this database.  Connections
      # in this database's connection pool will be instances of this class.
      attr_accessor :swift_class
      
      # Call the DATABASE_SETUP proc directly after initialization,
      # so the object always uses sub adapter specific code.  Also,
      # raise an error immediately if the connection doesn't have a
      # db_type specified, since one is required to include the correct
      # subadapter.
      def initialize(opts)
        super
        if db_type = opts[:db_type] and !db_type.to_s.empty? 
          if prok = DATABASE_SETUP[db_type.to_s.to_sym]
            prok.call(self)
          else
            raise(Error, "No :db_type option specified")
          end
        else
          raise(Error, ":db_type option not valid, should be postgres, mysql, or sqlite")
        end
      end
      
      # Create an instance of swift_class for the given options.
      def connect(server)
        setup_connection(swift_class.new(server_opts(server)))
      end
      
      # Return a Sequel::Swift::Dataset object for this database.
      def dataset(opts = nil)
        Swift::Dataset.new(self, opts)
      end
    
      # Execute the given SQL, yielding a Swift::Result if a block is given.
      def execute(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            res = log_yield(sql){conn.execute(sql)}
            yield res if block_given?
            nil
          rescue SwiftError => e
            raise_error(e)
          end
        end
      end
      
      # Execute the SQL on the this database, returning the number of affected
      # rows.
      def execute_dui(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            log_yield(sql){conn.execute(sql).rows}
          rescue SwiftError => e
            raise_error(e)
          end
        end
      end
      
      # Execute the SQL on this database, returning the primary key of the
      # table being inserted to.
      def execute_insert(sql, opts={})
        synchronize(opts[:server]) do |conn|
          begin
            log_yield(sql){conn.execute(sql).insert_id}
          rescue SwiftError => e
            raise_error(e)
          end
        end
      end
      
      private
      
      # Method to call on a statement object to execute SQL that does
      # not return any rows.
      def connection_execute_method
        :execute
      end
      
      # Close the given database connection.
      def disconnect_connection(c)
      end
      
      # Execute SQL on the connection
      def log_connection_execute(conn, sql)
        log_yield(sql){conn.execute(sql)}
      end
      
      # Set the :db entry to the same as the :database entry, since
      # Swift uses :db.
      def server_opts(o)
        o = super
        o[:db] ||= o[:database]
        o
      end
      
      # Allow extending the given connection when it is first created.
      # By default, just returns the connection.
      def setup_connection(conn)
        conn
      end
    end
    
    class Dataset < Sequel::Dataset
      # Set the columns and yield the hashes to the block.
      def fetch_rows(sql, &block)
        execute(sql) do |res|
          @columns = res.fields
          res.each(&block)
        end
        self
      end
    end
  end
end
