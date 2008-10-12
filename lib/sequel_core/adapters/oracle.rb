require 'oci8'
require 'sequel_core/adapters/shared/oracle'

module Sequel
  module Oracle
    class Database < Sequel::Database
      include DatabaseMethods
      set_adapter_scheme :oracle
      
      def connect(server)
        opts = server_opts(server)
        if opts[:database]
          dbname = opts[:host] ? \
            "//#{opts[:host]}#{":#{opts[:port]}" if opts[:port]}/#{opts[:database]}" : opts[:database]
        else
          dbname = opts[:host]
        end
        conn = OCI8.new(opts[:user], opts[:password], dbname, opts[:privilege])
        conn.autocommit = true
        conn.non_blocking = true
        conn
      end
      
      def disconnect
        @pool.disconnect {|c| c.logoff}
      end
    
      def dataset(opts = nil)
        Oracle::Dataset.new(self, opts)
      end
    
      def execute(sql, opts={})
        log_info(sql)
        synchronize(opts[:server]) do |conn|
          r = conn.exec(sql)
          yield(r) if block_given?
          r
        end
      end
      alias_method :do, :execute
      
      def transaction(server=nil)
        synchronize(server) do |conn|
          return yield(conn) if @transactions.include?(Thread.current)
          
          conn.autocommit = false
          begin
            @transactions << Thread.current
            yield(conn)
          rescue => e
            conn.rollback
            raise e unless Error::Rollback === e
          ensure
            conn.commit unless e
            conn.autocommit = true
            @transactions.delete(Thread.current)
          end
        end
      end
    end
    
    class Dataset < Sequel::Dataset
      include DatasetMethods

      def literal(v)
        case v
        when OraDate
          literal(Time.local(*v.to_a))
        else
          super
        end
      end

      def fetch_rows(sql, &block)
        execute(sql) do |cursor|
          begin
            @columns = cursor.get_col_names.map {|c| c.downcase.to_sym}
            while r = cursor.fetch
              row = {}
              r.each_with_index {|v, i| row[columns[i]] = v unless columns[i] == :raw_rnum_}
              yield row
            end
          ensure
            cursor.close
          end
        end
        self
      end
    end
  end
end
