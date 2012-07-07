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

      def execute(sql, opts={})
        synchronize(opts[:server]) do |conn|
          r = log_yield(sql){conn.execute(sql)}
          yield(r) if block_given?
          r
        end
      end
      alias_method :do, :execute

      private

      def disconnect_connection(c)
        c.disconnect
      end
    end

    class Dataset < Sequel::Dataset
      SELECT_CLAUSE_METHODS = clause_methods(:select, %w'select distinct columns from join where group having compounds order limit')

      Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) do |result|
          begin
            @columns = result.column_infos.map{|c| output_identifier(c.name)}
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

      private

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
