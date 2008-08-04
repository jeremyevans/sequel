require 'openbase'

module Sequel
  module OpenBase
    class Database < Sequel::Database
      set_adapter_scheme :openbase
      
      def connect(server)
        opts = server_opts(server)
        OpenBase.new(
          opts[:database],
          opts[:host] || 'localhost',
          opts[:user],
          opts[:password]
        )
      end
      
      def disconnect
        # would this work?
        @pool.disconnect {|c| c.disconnect}
      end
    
      def dataset(opts = nil)
        OpenBase::Dataset.new(self, opts)
      end
    
      def execute(sql, opts={})
        log_info(sql)
        synchronize(opts[:server]) do |conn|
          r = conn.execute(sql)
          yield(r) if block_given?
          r
        end
      end
      alias_method :do, :execute
    end
    
    class Dataset < Sequel::Dataset
      def literal(v)
        case v
        when Time
          literal(v.iso8601)
        when Date, DateTime
          literal(v.to_s)
        else
          super
        end
      end

      def fetch_rows(sql)
        execute(sql) do |result|
          begin
            @columns = result.column_infos.map {|c| c.name.to_sym}
            result.each do |r|
              row = {}
              r.each_with_index {|v, i| row[@columns[i]] = v}
              yield row
            end
          ensure
            # result.close
          end
        end
        self
      end
    end
  end
end
