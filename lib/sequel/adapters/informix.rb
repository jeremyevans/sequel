require 'informix'

module Sequel
  module Informix
    class Database < Sequel::Database
      set_adapter_scheme :informix
      
      TEMPORARY = 'TEMP '.freeze
      
      def connect(server)
        opts = server_opts(server)
        ::Informix.connect(opts[:database], opts[:user], opts[:password])
      end
    
      def dataset(opts = nil)
        Sequel::Informix::Dataset.new(self, opts)
      end
      
      # Returns number of rows affected
      def execute_dui(sql, opts={})
        synchronize(opts[:server]){|c| log_yield(sql){c.immediate(sql)}}
      end
      alias_method :do, :execute_dui
      
      def execute(sql, opts={})
        synchronize(opts[:server]){|c| yield log_yield(sql){c.cursor(sql)}}
      end
      alias_method :query, :execute
      
      private

      def disconnect_connection(c)
        c.close
      end
    end
    
    class Dataset < Sequel::Dataset
      SELECT_CLAUSE_METHODS = clause_methods(:select, %w'limit distinct columns from join where having group compounds order')

      def fetch_rows(sql)
        execute(sql) do |cursor|
          begin
            col_map = nil
            cursor.open.each_hash do |h|
              unless col_map
                col_map = {}
                @columns = h.keys.map{|k| col_map[k] = output_identifier(k)}
              end
              h2 = {}
              h.each{|k,v| h2[col_map[k]||k] = v}
              yield h2
            end
          ensure
            cursor.drop
          end
        end
        self
      end

      private

      # Informix does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      def select_limit_sql(sql)
        sql << " SKIP #{@opts[:offset]}" if @opts[:offset]
        sql << " FIRST #{@opts[:limit]}" if @opts[:limit]
      end
    end
  end
end
