# frozen-string-literal: true

module Sequel 
  class Dataset
    # ---------------------
    # :section: 8 - Methods related to prepared statements or bound variables
    # On some adapters, these use native prepared statements and bound variables, on others
    # support is emulated.  For details, see the {"Prepared Statements/Bound Variables" guide}[rdoc-ref:doc/prepared_statements.rdoc].
    # ---------------------
    
    PREPARED_ARG_PLACEHOLDER = LiteralString.new('?').freeze

    DEFAULT_PREPARED_STATEMENT_MODULE_METHODS = %w'execute execute_dui execute_insert'.freeze.each(&:freeze)
    PREPARED_STATEMENT_MODULE_CODE = {
      :bind => "opts = Hash[opts]; opts[:arguments] = bind_arguments".freeze,
      :prepare => "sql = prepared_statement_name".freeze,
      :prepare_bind => "sql = prepared_statement_name; opts = Hash[opts]; opts[:arguments] = bind_arguments".freeze
    }.freeze

    def self.prepared_statements_module(code, mods, meths=DEFAULT_PREPARED_STATEMENT_MODULE_METHODS, &block)
      code = PREPARED_STATEMENT_MODULE_CODE[code] || code

      Module.new do 
        Sequel.set_temp_name(self){"Sequel::Dataset::_PreparedStatementsModule(#{block.source_location.join(':') if block})"}
        Array(mods).each do |mod|
          include mod
        end

        if block
          module_eval(&block)
        end

        meths.each do |meth|
          module_eval("def #{meth}(sql, opts=Sequel::OPTS) #{code}; super end", __FILE__, __LINE__)
        end
        private(*meths)
      end
    end
    private_class_method :prepared_statements_module

    # Default implementation of the argument mapper to allow
    # native database support for bind variables and prepared
    # statements (as opposed to the emulated ones used by default).
    module ArgumentMapper
      # The name of the prepared statement, if any.
      def prepared_statement_name
        @opts[:prepared_statement_name]
      end

      # The bind arguments to use for running this prepared statement
      def bind_arguments
        @opts[:bind_arguments]
      end

      # Set the bind arguments based on the hash and call super.
      def call(bind_vars=OPTS, &block)
        sql = prepared_sql
        prepared_args.freeze
        ps = bind(bind_vars)
        ps.clone(:bind_arguments=>ps.map_to_prepared_args(ps.opts[:bind_vars]), :sql=>sql, :prepared_sql=>sql).run(&block)
      end
        
      # Override the given *_sql method based on the type, and
      # cache the result of the sql.
      def prepared_sql
        if sql = @opts[:prepared_sql] || cache_get(:_prepared_sql)
          return sql
        end
        cache_set(:_prepared_sql, super)
      end

      private

      # Report that prepared statements are not emulated, since
      # all adapters that use this use native prepared statements.
      def emulate_prepared_statements?
        false
      end
    end

    # Backbone of the prepared statement support.  Grafts bind variable
    # support into datasets by hijacking #literal and using placeholders.
    # By default, emulates prepared statements and bind variables by
    # taking the hash of bind variables and directly substituting them
    # into the query, which works on all databases, as it is no different
    # from using the dataset without bind variables.
    module PreparedStatementMethods
      # Whether to log the full SQL query.  By default, just the prepared statement
      # name is generally logged on adapters that support native prepared statements.
      def log_sql
        @opts[:log_sql]
      end
      
      # The type of SQL to generate for the prepared statement.  Generally
      # the same as #prepared_type, but can be different.
      def prepared_sql_type
        @opts[:prepared_sql_type] || prepared_type
      end
      
      # The type of prepared statement, which controls how the prepared statement
      # handles results from the database.
      def prepared_type
        @opts[:prepared_type]
      end
      
      # The array/hash of bound variable placeholder names.
      def prepared_args
        @opts[:prepared_args]
      end
      
      # The dataset that created this prepared statement.
      def orig_dataset
        @opts[:orig_dataset]
      end
      
      # The argument to supply to insert and update, which may use
      # placeholders specified by prepared_args
      def prepared_modify_values
        @opts[:prepared_modify_values]
      end

      # Sets the prepared_args to the given hash and runs the
      # prepared statement.
      def call(bind_vars=OPTS, &block)
        bind(bind_vars).run(&block)
      end

      # Raise an error if attempting to call prepare on an already
      # prepared statement.
      def prepare(*)
        raise Error, "cannot prepare an already prepared statement" unless allow_preparing_prepared_statements?
        super
      end

      # Send the columns to the original dataset, as calling it
      # on the prepared statement can cause problems.
      def columns
        orig_dataset.columns
      end
      
      # Disallow use of delayed evaluations in prepared statements.
      def delayed_evaluation_sql_append(sql, delay)
        raise Error, "delayed evaluations cannot be used in prepared statements" if @opts[:no_delayed_evaluations]
        super
      end

      # Returns the SQL for the prepared statement, depending on
      # the type of the statement and the prepared_modify_values.
      def prepared_sql
        case prepared_sql_type
        when :select, :all, :each
          # Most common scenario, so listed first.
          select_sql
        when :first, :single_value
          clone(:limit=>1).select_sql
        when :insert_select
          insert_select_sql(*prepared_modify_values)
        when :insert, :insert_pk
          insert_sql(*prepared_modify_values)
        when :update
          update_sql(*prepared_modify_values)
        when :delete
          delete_sql
        else
          select_sql
        end
      end
      
      # Changes the values of symbols if they start with $ and
      # prepared_args is present.  If so, they are considered placeholders,
      # and they are substituted using prepared_arg.
      def literal_symbol_append(sql, v)
        if @opts[:bind_vars] && /\A\$(.*)\z/ =~ v
          literal_append(sql, prepared_arg($1.to_sym))
        else
          super
        end
      end
      
      # Programmer friendly string showing this is a prepared statement,
      # with the prepared SQL it represents (which in general won't have
      # substituted variables).
      def inspect
        "<#{visible_class_name}/PreparedStatement #{prepared_sql.inspect}>"
      end
      
      protected
      
      # Run the method based on the type of prepared statement.
      def run(&block)
        case type = prepared_type
        when :select, :all
          with_sql_all(prepared_sql, &block)
        when :each
          with_sql_each(prepared_sql, &block)
        when :insert_select, :first
          with_sql_first(prepared_sql)
        when :insert, :update, :delete
          if opts[:returning] && supports_returning?(prepared_type)
            returning_fetch_rows(prepared_sql)
          elsif type == :delete
            with_sql_delete(prepared_sql)
          else
            force_prepared_sql.public_send(type, *prepared_modify_values)
          end
        when :insert_pk, :single_value
          with_sql_single_value(prepared_sql)
        when Array
          # :nocov:
          case type[0]
          # :nocov:
          when :map, :as_hash, :to_hash, :to_hash_groups
            force_prepared_sql.public_send(*type, &block) 
          end
        else
          raise Error, "unsupported prepared statement type used: #{prepared_type.inspect}"
        end
      end
      
      private
      
      # If the prepared_sql_type does not match the prepared statement, return a clone that
      # with the prepared SQL, to ensure the prepared_sql_type is respected.
      def force_prepared_sql
        if prepared_sql_type != prepared_type
          with_sql(prepared_sql)
        else
          self
        end
      end
      
      # Returns the value of the prepared_args hash for the given key.
      def prepared_arg(k)
        @opts[:bind_vars][k]
      end

      # The symbol cache should always be skipped, since placeholders are symbols.
      def skip_symbol_cache?
        true
      end

      # Use a clone of the dataset extended with prepared statement
      # support and using the same argument hash so that you can use
      # bind variables/prepared arguments in subselects.
      def subselect_sql_append(sql, ds)
        subselect_sql_dataset(sql, ds).prepared_sql
      end

      def subselect_sql_dataset(sql, ds)
        super.clone(:prepared_args=>prepared_args, :bind_vars=>@opts[:bind_vars]).
          send(:to_prepared_statement, :select, nil, :extend=>prepared_statement_modules)
      end
    end
    
    # Default implementation for an argument mapper that uses
    # unnumbered SQL placeholder arguments.  Keeps track of which
    # arguments have been used, and allows arguments to
    # be used more than once.
    module UnnumberedArgumentMapper
      include ArgumentMapper
      
      protected
      
      # Returns a single output array mapping the values of the input hash.
      # Keys in the input hash that are used more than once in the query
      # have multiple entries in the output array.
      def map_to_prepared_args(bind_vars)
        prepared_args.map{|v| bind_vars[v]}
      end
      
      private
      
      # Associates the argument with name k with the next position in
      # the output array.
      def prepared_arg(k)
        prepared_args << k
        prepared_arg_placeholder
      end
    end
    
    # Prepared statements emulation support for adapters that don't
    # support native prepared statements.  Uses a placeholder
    # literalizer to hold the prepared sql with the ability to
    # interpolate arguments to prepare the final SQL string.
    module EmulatePreparedStatementMethods
      include UnnumberedArgumentMapper

      def run(&block)
        if @opts[:prepared_sql_frags]
          sql = literal(Sequel::SQL::PlaceholderLiteralString.new(@opts[:prepared_sql_frags], @opts[:bind_arguments], false))
          clone(:prepared_sql_frags=>nil, :sql=>sql, :prepared_sql=>sql).run(&block)
        else
          super
        end
      end

      private
      
      # Turn emulation of prepared statements back on, since ArgumentMapper
      # turns it off.
      def emulate_prepared_statements?
        true
      end
        
      def emulated_prepared_statement(type, name, values)
        prepared_sql, frags = Sequel::Dataset::PlaceholderLiteralizer::Recorder.new.send(:prepared_sql_and_frags, self, prepared_args) do |pl, ds|
          ds = ds.clone(:recorder=>pl)

          sql_type = prepared_sql_type || type
          case sql_type
          when :first, :single_value
            ds.limit(1)
          when :update, :insert, :insert_select, :delete
            ds.with_sql(:"#{sql_type}_sql", *values)
          when :insert_pk
            ds.with_sql(:insert_sql, *values)
          else
            ds
          end
        end

        prepared_args.freeze
        clone(:prepared_sql_frags=>frags, :prepared_sql=>prepared_sql, :sql=>prepared_sql)
      end

      # Associates the argument with name k with the next position in
      # the output array.
      def prepared_arg(k)
        prepared_args << k
        @opts[:recorder].arg
      end

      def subselect_sql_dataset(sql, ds)
        super.clone(:recorder=>@opts[:recorder]).
          with_extend(EmulatePreparedStatementMethods)
      end
    end
    
    # Set the bind variables to use for the call.  If bind variables have
    # already been set for this dataset, they are updated with the contents
    # of bind_vars.
    #
    #   DB[:table].where(id: :$id).bind(id: 1).call(:first)
    #   # SELECT * FROM table WHERE id = ? LIMIT 1 -- (1)
    #   # => {:id=>1}
    def bind(bind_vars=OPTS)
      bind_vars = if bv = @opts[:bind_vars]
        bv.merge(bind_vars).freeze
      else
        if bind_vars.frozen?
          bind_vars
        else
          Hash[bind_vars]
        end
      end

      clone(:bind_vars=>bind_vars)
    end
    
    # For the given type, run the sql with the bind variables specified in the hash.
    # +values+ is a hash passed to insert or update (if one of those types is used),
    # which may contain placeholders.
    #
    # The following types are supported:
    # 
    # * :select, :all, :each, :first, :single_value, :insert, :insert_select, :insert_pk, :update, :delete
    # * Array where first element is :map, :as_hash, :to_hash, :to_hash_groups (remaining elements
    #   are passed to the related method)
    #
    #   DB[:table].where(id: :$id).call(:first, id: 1)
    #   # SELECT * FROM table WHERE id = ? LIMIT 1 -- (1)
    #   # => {:id=>1}
    #
    #   DB[:table].where(id: :$id).call(:update, {c: 1, id: 2}, col: :$c)
    #   # UPDATE table WHERE id = ? SET col = ? -- (2, 1)
    #   # => 1
    def call(type, bind_variables=OPTS, *values, &block)
      to_prepared_statement(type, values, :extend=>bound_variable_modules).call(bind_variables, &block)
    end
    
    # Prepare an SQL statement for later execution.  Takes a type similar to #call,
    # and the +name+ symbol of the prepared statement.
    #
    # This returns a clone of the dataset extended with PreparedStatementMethods,
    # which you can +call+ with the hash of bind variables to use.
    # The prepared statement is also stored in
    # the associated Database, where it can be called by name.
    # The following usage is identical:
    #
    #   ps = DB[:table].where(name: :$name).prepare(:first, :select_by_name)
    #
    #   ps.call(name: 'Blah')
    #   # SELECT * FROM table WHERE name = ? -- ('Blah')
    #   # => {:id=>1, :name=>'Blah'}
    #
    #   DB.call(:select_by_name, name: 'Blah') # Same thing
    #
    # +values+ given are passed to +insert+ or +update+ if they are used:
    #
    #   ps = DB[:table].where(id: :$i).prepare(:update, :update_name, name: :$n)
    #
    #   ps.call(i: 1, n: 'Blah')
    #   # UPDATE table WHERE id = ? SET name = ? -- (1, 'Blah')
    #   # => 1
    def prepare(type, name, *values)
      ps = to_prepared_statement(type, values, :name=>name, :extend=>prepared_statement_modules, :no_delayed_evaluations=>true)

      ps = if ps.send(:emulate_prepared_statements?)
        ps = ps.with_extend(EmulatePreparedStatementMethods)
        ps.send(:emulated_prepared_statement, type, name, values)
      else
        sql = ps.prepared_sql
        ps.prepared_args.freeze
        ps.clone(:prepared_sql=>sql, :sql=>sql)
      end

      db.set_prepared_statement(name, ps)
      ps
    end
    
    # Set the type of SQL to use for prepared statements based on this
    # dataset.  Prepared statements default to using the same SQL type
    # as the type that is passed to #prepare/#call, but there are cases
    # where it is helpful to use a different SQL type.
    #
    # Available types are: :select, :first, :single_value, :update,
    # :delete, :insert, :insert_select, :insert_pk
    #
    # Other types are treated as :select.
    def prepare_sql_type(type)
      clone(:prepared_sql_type => type)
    end

    protected
    
    # Return a cloned copy of the current dataset extended with
    # PreparedStatementMethods, setting the type and modify values.
    def to_prepared_statement(type, values=nil, opts=OPTS)
      mods = opts[:extend] || []
      mods += [PreparedStatementMethods]

      bind.
        clone(:prepared_statement_name=>opts[:name], :prepared_type=>type, :prepared_modify_values=>values, :orig_dataset=>self, :no_cache_sql=>true, :prepared_args=>@opts[:prepared_args]||[], :no_delayed_evaluations=>opts[:no_delayed_evaluations]).
        with_extend(*mods)
    end

    private
    
    # Don't allow preparing prepared statements by default.
    def allow_preparing_prepared_statements?
      false
    end

    def bound_variable_modules
      prepared_statement_modules
    end

    # Whether prepared statements should be emulated.  True by
    # default so that adapters have to opt in.
    def emulate_prepared_statements?
      true
    end

    def prepared_statement_modules
      []
    end

    # The argument placeholder.  Most databases used unnumbered
    # arguments with question marks, so that is the default.
    def prepared_arg_placeholder
      PREPARED_ARG_PLACEHOLDER
    end
  end
end
