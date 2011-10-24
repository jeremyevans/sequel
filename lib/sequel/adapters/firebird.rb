require 'fb'
Sequel.require 'adapters/shared/firebird'

module Sequel
  # The Sequel Firebird adapter requires the ruby fb driver located at
  # http://github.com/wishdev/fb.
  module Firebird
    class Database < Sequel::Database
      include Sequel::Firebird::DatabaseMethods

      set_adapter_scheme :firebird

      DISCONNECT_ERRORS = /Unsuccessful execution caused by a system error that precludes successful execution of subsequent statements/

      # Add the primary_keys instance variables.
      # so we can get the correct return values for inserted rows.
      def initialize(*args)
        super
        @primary_keys = {}
      end

      def connect(server)
        opts = server_opts(server)

        Fb::Database.new(
          :database => "#{opts[:host]}:#{opts[:database]}",
          :username => opts[:user],
          :password => opts[:password]).connect
      end

      def execute(sql, opts={})
        begin
          synchronize(opts[:server]) do |conn|
            if conn.transaction_started && !@transactions.has_key?(conn)
              conn.rollback
              raise DatabaseDisconnectError, "transaction accidently left open, rolling back and disconnecting"
            end
            r = log_yield(sql){conn.execute(sql)}
            yield(r) if block_given?
            r
          end
        rescue Fb::Error => e
          raise_error(e, :disconnect=>DISCONNECT_ERRORS.match(e.message))
        end
      end

      private

      def begin_transaction(conn, opts={})
        log_yield(TRANSACTION_BEGIN) do
          begin
            conn.transaction
          rescue Fb::Error => e
            conn.rollback
            raise_error(e, :disconnect=>true) 
          end
        end
      end

      def commit_transaction(conn, opts={})
        log_yield(TRANSACTION_COMMIT){conn.commit}
      end
      
      def database_error_classes
        [Fb::Error]
      end

      def disconnect_connection(c)
        c.close
      end

      def rollback_transaction(conn, opts={})
        log_yield(TRANSACTION_ROLLBACK){conn.rollback}
      end
    end

    # Dataset class for Firebird datasets
    class Dataset < Sequel::Dataset
      include Sequel::Firebird::DatasetMethods

      Database::DatasetClass = self

      # Yield all rows returned by executing the given SQL and converting
      # the types.
      def fetch_rows(sql)
        execute(sql) do |s|
          begin
            @columns = columns = s.fields.map{|c| output_identifier(c.name)}
            s.fetchall.each do |r|
              h = {}
              r.zip(columns).each{|v, c| h[c] = v}
              yield h
            end
          ensure
            s.close
          end
        end
        self
      end
    end
  end
end
