Sequel.require 'adapters/shared/cassandra'
require 'cql'

module Sequel
  # Top level module for holding all Cassandra modules and classes
  # for Sequel.
  module Cassandra
    class Database < Sequel::Database
      include Sequel::Cassandra::DatabaseMethods
      set_adapter_scheme :cassandra
      
      def connect(server)
        opts = server_opts(server)

        client = Cql::Client.connect(opts)
        client.use(opts[:default_keyspace])
        client
      end

      def disconnect_connection(c)
        c.close
      end

      def execute(sql, opts={})
        consistency = {consistency: opts[:read_consistency] || server_opts(opts)[:default_read_consistency]}

        synchronize do |conn|
          r = log_yield(sql){conn.execute(sql, consistency)}
          yield(r) if block_given?
          r
        end
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      def fetch_rows(sql)
        puts sql
        execute(sql) do |results|
          results.each_row do |result|
            @columns ||= result.keys

            yield result
          end
        end
        self
      end
    end
  end
end