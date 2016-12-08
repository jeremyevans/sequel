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
            ds.clone(:append_sql=>sql, :prepared_args=>prepared_args, :bind_vars=>@opts[:bind_vars]).
              send(:to_prepared_statement, :select, nil, :extend=>bound_variable_modules).
              prepared_sql
          end
        end
        
        PreparedStatementMethods = Sequel::Dataset.send(:prepared_statements_module, :prepare_bind, Sequel::Dataset::UnnumberedArgumentMapper)
        
        private

        def bound_variable_modules
          [CallableStatementMethods]
        end

        def prepared_statement_modules
          [PreparedStatementMethods]
        end
      end
    end
  end
end
