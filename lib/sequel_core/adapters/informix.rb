require 'informix'

module Sequel
  module Informix
    class Database < Sequel::Database
      set_adapter_scheme :informix
      
      # AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      # 
      # def auto_increment_sql
      #   AUTO_INCREMENT
      # end
      
      def connect(server)
        opts = server_opts(server)
        ::Informix.connect(opts[:database], opts[:user], opts[:password])
      end
      
      def disconnect
        @pool.disconnect{|c| c.close}
      end
    
      def dataset(opts = nil)
        Sequel::Informix::Dataset.new(self, opts)
      end
      
      # Returns number of rows affected
      def execute_dui(sql, opts={})
        log_info(sql)
        synchronize(opts[:server]){|c| c.immediate(sql)}
      end
      alias_method :do, :execute_dui
      
      def execute(sql, opts={})
        log_info(sql)
        synchronize(opts[:server]){|c| yield c.cursor(sql)}
      end
      alias_method :query, :execute
    end
    
    class Dataset < Sequel::Dataset
      SELECT_CLAUSE_ORDER = %w'limit distinct columns from join where group having order union intersect except'.freeze

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

      def fetch_rows(sql, &block)
        execute(sql) do |cursor|
          begin
            cursor.open.each_hash(&block)
          ensure
            cursor.drop
          end
        end
        self
      end

      private

      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      def select_limit_sql(sql, opts)
        sql << " SKIP #{opts[:offset]}" if opts[:offset]
        sql << " FIRST #{opts[:limit]}" if opts[:limit]
      end
    end
  end
end
