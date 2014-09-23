require 'informix'
Sequel.require 'adapters/shared/informix'

module Sequel
  module Informix
    class Database < Sequel::Database
      include DatabaseMethods

      set_adapter_scheme :informix
      
      def connect(server)
        opts = server_opts(server)
        ::Informix.connect(opts[:database], opts[:user], opts[:password])
      end
    
      def transaction(opts=OPTS)
        if @opts[:nolog]
          yield
        else
          super
        end
      end

      # Returns number of rows affected
      def execute_dui(sql, opts=OPTS)
        synchronize(opts[:server]){|c| log_yield(sql){c.immediate(sql)}}
      end
      
      def execute_insert(sql, opts=OPTS)
        synchronize(opts[:server]){|c|
          log_yield(sql){c.immediate(sql)}
          c.cursor(%q{select first 1 dbinfo('sqlca.sqlerrd1') from systables}).open.fetch
        }
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]){|c| yield log_yield(sql){c.cursor(sql)}}
      end
    end
    
    class Dataset < Sequel::Dataset
      include DatasetMethods

      Database::DatasetClass = self

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
            cursor.respond_to?(:free) ? cursor.free : cursor.drop
          end
        end
        self
      end
    end
  end
end
