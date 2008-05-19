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
      
      def connect
        ::Informix.connect(@opts[:database], @opts[:user], @opts[:password])
      end
      
      def disconnect
        @pool.disconnect {|c| c.close}
      end
    
      def dataset(opts = nil)
        Sequel::Informix::Dataset.new(self, opts)
      end
      
      # Returns number of rows affected
      def execute(sql)
        log_info(sql)
        @pool.hold {|c| c.immediate(sql)}
      end
      alias_method :do, :execute
      
      def query(sql, &block)
        log_info(sql)
        @pool.hold {|c| block[c.cursor(sql)]}
      end
    end
    
    class Dataset < Sequel::Dataset
      def literal(v)
        case v
        when Time
          literal(v.iso8601)
        else
          super
        end
      end

      def select_sql(opts = nil)
        limit = opts.delete(:limit)
        offset = opts.delete(:offset)
        sql = super
        if limit
          limit = "FIRST #{limit}"
          offset = offset ? "SKIP #{offset}" : ""
          sql.sub!(/^select /i,"SELECT #{offset} #{limit} ")
        end
        sql
      end
      
      def fetch_rows(sql, &block)
        @db.synchronize do
          @db.query(sql) do |cursor|
            begin
              cursor.open.each_hash(&block)
            ensure
              cursor.drop
            end
          end
        end
        self
      end
      
      def insert(*values)
        @db.do insert_sql(*values)
      end
    
      def update(*args, &block)
        @db.do update_sql(*args, &block)
      end
    
      def delete(opts = nil)
        @db.do delete_sql(opts)
      end
    end
  end
end
