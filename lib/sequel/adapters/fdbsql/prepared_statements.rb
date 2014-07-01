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

require 'sequel/dataset/prepared_statements.rb'

module Sequel
  module Fdbsql
    module DatasetPreparedStatements
      PREPARED_ARG_PLACEHOLDER = LiteralString.new('$').freeze

      # FDBSQL specific argument mapper used for mapping the named
      # argument hash to a array with numbered arguments.
      module ArgumentMapper
        include Sequel::Dataset::ArgumentMapper

        protected

        # An array of bound variable values for this query, in the correct order.
        def map_to_prepared_args(hash)
          prepared_args.map{|k| hash[k.to_sym]}
        end

        private

        def prepared_arg(k)
          y = k
          if i = prepared_args.index(y)
            i += 1
          else
            prepared_args << y
            i = prepared_args.length
          end
          LiteralString.new("#{prepared_arg_placeholder}#{i}")
        end

        # Always assume a prepared argument.
        def prepared_arg?(k)
          true
        end
      end

      # Allow use of bind arguments for FDBSQL using the pg driver.
      module BindArgumentMethods

        include ArgumentMapper

        # Override insert action to use RETURNING if the server supports it.
        def run
          if @prepared_type == :insert
            fetch_rows(prepared_sql){|r| return r.values.first}
          else
            super
          end
        end

        def prepared_sql
          return @prepared_sql if @prepared_sql
          @opts[:returning] = insert_pk if @prepared_type == :insert
          super
          @prepared_sql
        end

        private

        # Execute the given SQL with the stored bind arguments.
        def execute(sql, opts=OPTS, &block)
          super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
        end

        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts=OPTS, &block)
          super(sql, {:arguments=>bind_arguments}.merge(opts), &block)
        end
      end

      # Allow use of server side prepared statements for FDBSQL using the
      # pg driver.
      module PreparedStatementMethods
        include BindArgumentMethods

        # Raise a more obvious error if you attempt to call a unnamed prepared statement.
        def call(*)
          raise Error, "Cannot call prepared statement without a name" if prepared_statement_name.nil?
          super
        end

        private

        # Execute the stored prepared statement name and the stored bind
        # arguments instead of the SQL given.
        def execute(sql, opts=OPTS, &block)
          super(prepared_statement_name, opts, &block)
        end

        # Same as execute, explicit due to intricacies of alias and super.
        def execute_dui(sql, opts=OPTS, &block)
          super(prepared_statement_name, opts, &block)
        end
      end

      # Execute the given type of statement with the hash of values.
      def call(type, bind_vars=OPTS, *values, &block)
        ps = to_prepared_statement(type, values)
        ps.extend(BindArgumentMethods)
        ps.call(bind_vars, &block)
      end

      # Prepare the given type of statement with the given name, and store
      # it in the database to be called later.
      def prepare(type, name=nil, *values)
        ps = to_prepared_statement(type, values)
        ps.extend(PreparedStatementMethods)
        if name
          ps.prepared_statement_name = name
          db.set_prepared_statement(name, ps)
        end
        ps
      end

      private

      # PostgreSQL uses $N for placeholders instead of ?, so use a $
      # as the placeholder.
      def prepared_arg_placeholder
        PREPARED_ARG_PLACEHOLDER
      end
    end


    module DatabasePreparedStatements

      def execute_prepared_statement(conn, name, opts=OPTS, &block)
        statement = prepared_statement(name)
        sql = statement.prepared_sql
        ps_name = name.to_s
        if args = opts[:arguments]
          args = args.map{|arg| bound_variable_arg(arg, conn)}
        end
        # create prepared statement if it doesn't exist, or has new sql
        unless conn.prepared_statements[ps_name] == sql
          conn.execute("DEALLOCATE #{ps_name}") if conn.prepared_statements.include?(ps_name)
          log_yield("PREPARE #{ps_name} AS #{sql}"){conn.prepare(ps_name, sql)}
          conn.prepared_statements[ps_name] = sql
        end
        raise "TODO #{ps_name}, #{args}, #{sql}"
      end
    end


  end
end

