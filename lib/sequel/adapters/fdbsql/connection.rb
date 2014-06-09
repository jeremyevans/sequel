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

      def initialize(opts)
        @config = opts
        @connection_hash = {
          :host => @config[:host],
          :port => @config[:port],
          :dbname => @config[:database],
          :user => @config[:username],
          :password => @config[:password]
        }
        # the below comments were in the activerecord adapter, not sure what's needed here
        # @prepared_statements = config.fetch(:prepared_statements) { true }
        # if @prepared_statements
        #   @visitor = Arel::Visitors::FdbSql.new self
        # else
        #   @visitor = BindSubstitution.new self
        # end
        connect
        # @statements = FdbSqlStatementPool.new(@connection, config.fetch(:statement_limit) { 1000 })
      end

      def close
        @connection.close
      end

      def query(sql)
        @connection.query(sql)
      end

      # Execute the given SQL with this connection.  If a block is given,
      # yield the results, otherwise, return the number of changed rows.
      def execute(sql, args=nil)
        raise 'fdbsql Connection.execute args are not supported' unless args.nil?
        block_given? ? yield(query(sql)) : query(sql)
      end


      private

      def connect
        @connection = PG::Connection.new(@connection_hash)
        configure_connection
      end

      def configure_connection
        # TODO this exists in activerecord adapter, go back and see what's needed here
      end
    end
  end
end
