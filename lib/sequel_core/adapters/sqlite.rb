require 'sqlite3'
require 'sequel_core/adapters/shared/sqlite'

module Sequel
  module SQLite
    class Database < Sequel::Database
      include ::Sequel::SQLite::DatabaseMethods
      
      set_adapter_scheme :sqlite
      
      def self.uri_to_options(uri) # :nodoc:
        { :database => (uri.host.nil? && uri.path == '/') ? nil : "#{uri.host}#{uri.path}" }
      end

      private_class_method :uri_to_options

      def connect
        @opts[:database] = ':memory:' if @opts[:database].blank?
        db = ::SQLite3::Database.new(@opts[:database])
        db.busy_timeout(@opts.fetch(:timeout, 5000))
        db.type_translation = true
        # fix for timestamp translation
        db.translator.add_translator("timestamp") do |t, v|
          v =~ /^\d+$/ ? Time.at(v.to_i) : Time.parse(v) 
        end 
        db
      end
      
      def dataset(opts = nil)
        SQLite::Dataset.new(self, opts)
      end
      
      def disconnect
        @pool.disconnect {|c| c.close}
      end
    
      def execute(sql)
        begin
          log_info(sql)
          @pool.hold {|conn| conn.execute_batch(sql); conn.changes}
        rescue SQLite3::Exception => e
          raise Error::InvalidStatement, "#{sql}\r\n#{e.message}"
        end
      end
      
      def execute_insert(sql)
        begin
          log_info(sql)
          @pool.hold {|conn| conn.execute(sql); conn.last_insert_row_id}
        rescue SQLite3::Exception => e
          raise Error::InvalidStatement, "#{sql}\r\n#{e.message}"
        end
      end
      
      def single_value(sql)
        begin
          log_info(sql)
          @pool.hold {|conn| conn.get_first_value(sql)}
        rescue SQLite3::Exception => e
          raise Error::InvalidStatement, "#{sql}\r\n#{e.message}"
        end
      end
      
      def execute_select(sql, &block)
        begin
          log_info(sql)
          @pool.hold {|conn| conn.query(sql, &block)}
        rescue SQLite3::Exception => e
          raise Error::InvalidStatement, "#{sql}\r\n#{e.message}"
        end
      end
      
      def transaction(&block)
        @pool.hold do |conn|
          if conn.transaction_active?
            return yield(conn)
          end
          begin
            result = nil
            conn.transaction {result = yield(conn)}
            result
          rescue ::Exception => e
            raise (SQLite3::Exception === e ? Error.new(e.message) : e) unless Error::Rollback === e
          end
        end
      end
      
      private
      
      def connection_pool_default_options
        o = super.merge(:pool_convert_exceptions=>false)
        # Default to only a single connection if a memory database is used,
        # because otherwise each connection will get a separate database
        o[:max_connections] = 1 if @opts[:database] == ':memory:' || @opts[:database].blank?
        o
      end
    end
    
    class Dataset < Sequel::Dataset
      include ::Sequel::SQLite::DatasetMethods
      
      EXPLAIN = 'EXPLAIN %s'.freeze
      
      def explain
        res = []
        @db.result_set(EXPLAIN % select_sql(opts), nil) {|r| res << r}
        res
      end

      def fetch_rows(sql)
        @db.execute_select(sql) do |result|
          @columns = result.columns.map {|c| c.to_sym}
          column_count = @columns.size
          result.each do |values|
            row = {}
            column_count.times {|i| row[@columns[i]] = values[i]}
            yield row
          end
        end
      end

      def literal(v)
        case v
        when LiteralString
          v
        when String
          "'#{::SQLite3::Database.quote(v)}'"
        when Time
          literal(v.iso8601)
        when Date, DateTime
          literal(v.to_s)
        else
          super
        end
      end
    end
  end
end
