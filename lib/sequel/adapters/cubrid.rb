require 'cubrid'
Sequel.require 'adapters/shared/cubrid'

module Sequel
  module Cubrid
     CUBRID_TYPE_PROCS = {
       ::Cubrid::DATE => lambda{|t| Date.new(t.year, t.month, t.day)},
       ::Cubrid::TIME => lambda{|t| SQLTime.create(t.hour, t.min, t.sec)},
       21 => lambda{|s| s.to_i}
     }

    class Database < Sequel::Database
      include Sequel::Cubrid::DatabaseMethods

      ROW_COUNT = "SELECT ROW_COUNT()".freeze
      LAST_INSERT_ID = "SELECT LAST_INSERT_ID()".freeze

      set_adapter_scheme :cubrid
      
      def connect(server)
        opts = server_opts(server)
        conn = ::Cubrid.connect(
          opts[:database],
          opts[:host] || 'localhost',
          opts[:port] || 30000,
          opts[:user] || 'public',
          opts[:password] || ''
        )
        conn.auto_commit = true
        conn
      end

      def server_version
        @server_version ||= synchronize{|c| c.server_version}
      end
      
      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          r = log_yield(sql) do
            begin
              conn.query(sql)
            rescue => e
              raise_error(e)
            end
          end
          if block_given?
            yield(r)
          else
            begin
              case opts[:type]
              when :dui
                # This is cubrid's API, but it appears to be completely broken,
                # giving StandardError: ERROR: CCI, -18, Invalid request handle
                #r.affected_rows

                # Work around bugs by using the ROW_COUNT function.
                begin
                  r2 = conn.query(ROW_COUNT)
                  r2.each{|a| return a.first.to_i}
                ensure
                  r2.close if r2
                end
              when :insert
                begin
                  r2 = conn.query(LAST_INSERT_ID)
                  r2.each{|a| return a.first.to_i}
                ensure
                  r2.close if r2
                end
              end
            ensure
              r.close
            end
          end
        end
      end

      def execute_ddl(sql, opts=OPTS)
        execute(sql, opts.merge(:type=>:ddl))
      end

      def execute_dui(sql, opts=OPTS)
        execute(sql, opts.merge(:type=>:dui))
      end

      def execute_insert(sql, opts=OPTS)
        execute(sql, opts.merge(:type=>:insert))
      end

      private

      def begin_transaction(conn, opts=OPTS)
        log_yield(TRANSACTION_BEGIN){conn.auto_commit = false}
      end
      
      def commit_transaction(conn, opts=OPTS)
        log_yield(TRANSACTION_COMMIT){conn.commit}
      end

      def database_error_classes
        [StandardError]
      end

      def remove_transaction(conn, committed)
        conn.auto_commit = true
      ensure
        super
      end
      
      # This doesn't actually work, as the cubrid ruby driver
      # does not implement transactions correctly.
      def rollback_transaction(conn, opts=OPTS)
        log_yield(TRANSACTION_ROLLBACK){conn.rollback}
      end
    end
    
    class Dataset < Sequel::Dataset
      include Sequel::Cubrid::DatasetMethods
      COLUMN_INFO_NAME = "name".freeze
      COLUMN_INFO_TYPE = "type_name".freeze

      Database::DatasetClass = self
      
      def fetch_rows(sql)
        execute(sql) do |stmt|
          begin
            cols = stmt.column_info.map{|c| [output_identifier(c[COLUMN_INFO_NAME]), CUBRID_TYPE_PROCS[c[COLUMN_INFO_TYPE]]]}
            @columns = cols.map{|c| c.first}
            stmt.each do |r|
              row = {}
              cols.zip(r).each{|(k, p), v| row[k] = (v && p) ? p.call(v) : v}
              yield row
            end
          ensure
            stmt.close
          end
        end
        self
      end
    end
  end
end
