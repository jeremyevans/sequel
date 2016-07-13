# frozen-string-literal: true

module Sequel
  module MySQL
    module PreparedStatements
      module DatabaseMethods
        private

        # Executes a prepared statement on an available connection.  If the
        # prepared statement already exists for the connection and has the same
        # SQL, reuse it, otherwise, prepare the new statement.  Because of the
        # usual MySQL stupidity, we are forced to name arguments via separate
        # SET queries.  Use @sequel_arg_N (for N starting at 1) for these
        # arguments.
        def execute_prepared_statement(ps_name, opts, &block)
          args = opts[:arguments]
          ps = prepared_statement(ps_name)
          sql = ps.prepared_sql
          synchronize(opts[:server]) do |conn|
            unless conn.prepared_statements[ps_name] == sql
              _execute(conn, "PREPARE #{ps_name} FROM #{literal(sql)}", opts)
              conn.prepared_statements[ps_name] = sql
            end
            i = 0
            _execute(conn, "SET " + args.map {|arg| "@sequel_arg_#{i+=1} = #{literal(arg)}"}.join(", "), opts) unless args.empty?
            opts = opts.merge(:log_sql=>" (#{sql})") if ps.log_sql
            _execute(conn, "EXECUTE #{ps_name}#{" USING #{(1..i).map{|j| "@sequel_arg_#{j}"}.join(', ')}" unless i == 0}", opts, &block)
          end
        end
      end

      module DatasetMethods
        # Methods to add to MySQL prepared statement calls without using a
        # real database prepared statement and bound variables.
        module CallableStatementMethods
          # Extend given dataset with this module so subselects inside subselects in
          # prepared statements work.
          def subselect_sql_append(sql, ds)
            ps = ds.to_prepared_statement(:select).clone(:append_sql => sql)
            ps.extend(CallableStatementMethods)
            ps = ps.bind(@opts[:bind_vars]) if @opts[:bind_vars]
            ps.prepared_args = prepared_args
            ps.prepared_sql
          end
        end
        
        PreparedStatementMethods = Sequel::Dataset.send(:prepared_statements_module,
          :prepare_bind,
          Sequel::Dataset::UnnumberedArgumentMapper) do
            # Raise a more obvious error if you attempt to call a unnamed prepared statement.
            def call(*)
              raise Error, "Cannot call prepared statement without a name" if prepared_statement_name.nil?
              super
            end
        end
        
        # MySQL is different in that it supports prepared statements but not bound
        # variables outside of prepared statements.  The default implementation
        # breaks the use of subselects in prepared statements, so extend the
        # temporary prepared statement that this creates with a module that
        # fixes it.
        def call(type, bind_arguments={}, *values, &block)
          ps = to_prepared_statement(type, values)
          ps.extend(CallableStatementMethods)
          ps.call(bind_arguments, &block)
        end
        
        # Store the given type of prepared statement in the associated database
        # with the given name.
        def prepare(type, name=nil, *values)
          ps = to_prepared_statement(type, values)
          ps.extend(PreparedStatementMethods)
          if name
            ps.prepared_statement_name = name
            db.set_prepared_statement(name, ps)
          end
          ps
        end
      end
    end
  end
end
