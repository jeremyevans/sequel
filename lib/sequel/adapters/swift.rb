# frozen-string-literal: true

module Sequel
  # Module holding the Swift DB support for Sequel.  Swift DB is a
  # collection of drivers used in Swift ORM.
  #
  # The Swift adapter currently supports PostgreSQL, MySQL and SQLite3
  #
  #   Sequel.connect('swift://user:password@host/database?db_type=postgres')
  #   Sequel.connect('swift://user:password@host/database?db_type=mysql')
  module Swift
    # Contains procs keyed on sub adapter type that extend the
    # given database object so it supports the correct database type.
    DATABASE_SETUP = {:postgres=>proc do |db|
        Sequel.require 'adapters/swift/postgres'
        db.extend(Sequel::Swift::Postgres::DatabaseMethods)
        db.extend_datasets Sequel::Postgres::DatasetMethods
        db.swift_class = ::Swift::DB::Postgres
      end,
      :mysql=>proc do |db|
        Sequel.require 'adapters/swift/mysql'
        db.extend(Sequel::Swift::MySQL::DatabaseMethods)
        db.dataset_class = Sequel::Swift::MySQL::Dataset
        db.swift_class = ::Swift::DB::Mysql
      end,
      :sqlite=>proc do |db|
        Sequel.require 'adapters/swift/sqlite'
        db.extend(Sequel::Swift::SQLite::DatabaseMethods)
        db.dataset_class = Sequel::Swift::SQLite::Dataset
        db.swift_class = ::Swift::DB::Sqlite3
        db.set_integer_booleans
      end,
    }
      
    class Database < Sequel::Database
      set_adapter_scheme :swift

      # The Swift adapter class being used by this database.  Connections
      # in this database's connection pool will be instances of this class.
      attr_accessor :swift_class
      
      # Create an instance of swift_class for the given options.
      def connect(server)
        opts = server_opts(server)
        opts[:pass] = opts[:password]
        setup_connection(swift_class.new(opts))
      end
      
      # Execute the given SQL, yielding a Swift::Result if a block is given.
      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            res = log_yield(sql){conn.execute(sql)}
            yield res if block_given?
            nil
          rescue ::Swift::Error => e
            raise_error(e)
          end
        end
      end
      
      # Execute the SQL on the this database, returning the number of affected
      # rows.
      def execute_dui(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            log_yield(sql){conn.execute(sql).affected_rows}
          rescue ::Swift::Error => e
            raise_error(e)
          end
        end
      end
      
      # Execute the SQL on this database, returning the primary key of the
      # table being inserted to.
      def execute_insert(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          begin
            log_yield(sql){conn.execute(sql).insert_id}
          rescue ::Swift::Error => e
            raise_error(e)
          end
        end
      end
      
      private
      
      # Call the DATABASE_SETUP proc directly after initialization,
      # so the object always uses sub adapter specific code.  Also,
      # raise an error immediately if the connection doesn't have a
      # db_type specified, since one is required to include the correct
      # subadapter.
      def adapter_initialize
        if db_type = @opts[:db_type] and !db_type.to_s.empty? 
          if prok = DATABASE_SETUP[db_type.to_s.to_sym]
            prok.call(self)
          else
            raise(Error, "No :db_type option specified")
          end
        else
          raise(Error, ":db_type option not valid, should be postgres, mysql, or sqlite")
        end
      end
      
      # Method to call on a statement object to execute SQL that does
      # not return any rows.
      def connection_execute_method
        :execute
      end
      
      def database_error_classes
        [::Swift::Error]
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
      Database::DatasetClass = self

      # Set the columns and yield the hashes to the block.
      def fetch_rows(sql)
        execute(sql) do |res|
          col_map = {}
          @columns = res.fields.map do |c|
            col_map[c] = output_identifier(c)
          end
          tz = db.timezone if Sequel.application_timezone
          res.each do |r|
            h = {}
            r.each do |k, v|
              h[col_map[k]] = case v
              when StringIO
                SQL::Blob.new(v.read)
              when DateTime
                tz ? Sequel.database_to_application_timestamp(Sequel.send(:convert_input_datetime_no_offset, v, tz)) : v
              else
                v
              end
            end
            yield h
          end
        end
        self
      end
    end
  end
end
