#
# FoundationDB SQL Layer Sequel Adapter
# Copyright (c) 2013-2014 FoundationDB, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#


# FoundationDB SQL Layer currently uses the Postgres protocol
require 'pg'

module Sequel
  module Fdbsql

    class Connection
      CONNECTION_OK = -1
      DISCONNECT_ERROR_RE = /\A(?:could not receive data from server|no connection to the server|connection not open|connection is closed)/

      # These sql states are used to indicate that fdbvql should automatically
      # retry the statement if it's not in a transaction
      RETRY_SQLSTATES = %w'40002'.freeze.each{|s| s.freeze}

      NUMBER_OF_NOT_COMMITTED_RETRIES = 10

      attr_accessor :in_transaction

      attr_accessor :prepared_statements

      def initialize(db, opts)
        @db = db
        @config = opts
        @connection_hash = {
          :host => @config[:host],
          :port => @config[:port],
          :dbname => @config[:database],
          :user => @config[:username],
          :password => @config[:password]
        }
        @prepared_statements = {}
        connect
      end

      def close
        # Just like postgres, ignore any errors here
        begin
          @connection.close
        rescue PGError, IOError
        end
      end

      def query(sql, args=nil)
        args = args.map{|v| @db.bound_variable_arg(v, self)} if args
        check_disconnect_errors do
          retry_on_not_committed do
            @connection.query(sql, args)
          end
        end
      end

      # Execute the given SQL with this connection.  If a block is given,
      # yield the results, otherwise, return the number of changed rows.
      def execute(sql, args=nil)
        q = query(sql, args)
        block_given? ? yield(q) : q.cmd_tuples
      end

      def prepare(name, sql)
        check_disconnect_errors do
          retry_on_not_committed do
            @connection.prepare(name, sql)
          end
        end
      end

      def execute_prepared_statement(name, args)
        check_disconnect_errors do
          retry_on_not_committed do
            @connection.exec_prepared(name, args)
          end
        end
      end

      private

      def connect
        @connection = PG::Connection.new(@connection_hash)
        # Swallow warnings
        @connection.set_notice_receiver { |proc| }
        check_version
      end

      def check_version
        ver = execute('SELECT VERSION()') do |res|
          version = res.first['_SQL_COL_1']
          m = version.match('^.* (\d+)\.(\d+)\.(\d+)')
          if m.nil?
            raise "No match when checking FDB SQL Layer version: #{version}"
          end
          m
        end

        # Combine into single number, two digits per part: 1.9.3 => 10903
        @sql_layer_version = (100 * ver[1].to_i + ver[2].to_i) * 100 + ver[3].to_i
        if @sql_layer_version < 10906
          raise "Unsupported FDB SQL Layer version: #{@sql_layer_version} (#{ver[0]})"
        end
      end

      def status
        CONNECTION_OK
      end

      def database_exception_sqlstate(exception, opts)
        if exception.respond_to?(:result) && (result = exception.result)
          result.error_field(::PGresult::PG_DIAG_SQLSTATE)
        end
      end

      def retry_on_not_committed
        retries = NUMBER_OF_NOT_COMMITTED_RETRIES
        begin
          yield
        rescue PG::TRIntegrityConstraintViolation => e
          if (!in_transaction and RETRY_SQLSTATES.include? database_exception_sqlstate(e, :classes=>CONVERTED_EXCEPTIONS))
            retry if (retries -= 1) > 0
          end
          raise
        end
      end

      # Raise a Sequel::DatabaseDisconnectError if a PGError is raised and
      # the connection status cannot be determined or it is not OK.
      def check_disconnect_errors
        begin
          yield
        rescue PGError => e
          disconnect = false
          begin
            s = status
          rescue PGError
            disconnect = true
          end
          status_ok = (s == CONNECTION_OK)
          disconnect ||= !status_ok
          disconnect ||= e.message =~ DISCONNECT_ERROR_RE
          disconnect ? raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError)) : raise
        rescue IOError, Errno::EPIPE, Errno::ECONNRESET => e
          disconnect = true
          raise(Sequel.convert_exception_class(e, Sequel::DatabaseDisconnectError))
        end
      end

    end
  end
end
