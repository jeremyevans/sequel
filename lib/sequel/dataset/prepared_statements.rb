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

    def self.def_deprecated_opts_setter(mod, *meths)
      meths.each do |meth|
        mod.send(:define_method, :"#{meth}=") do |v|
          # :nocov:
          Sequel::Deprecation.deprecate("Dataset##{meth}=", "The API has changed, and this value should now be passed in as an option via Dataset#clone.")
          @opts[meth] = v
          # :nocov:
        end
      end
    end
    
    # Default implementation of the argument mapper to allow
    # native database support for bind variables and prepared
    # statements (as opposed to the emulated ones used by default).
    module ArgumentMapper
      Dataset.def_deprecated_opts_setter(self, :prepared_statement_name, :bind_arguments)
      
      # The name of the prepared statement, if any.
      def prepared_statement_name
        @opts[:prepared_statement_name]
      end

      # The bind arguments to use for running this prepared statement
      def bind_arguments
        @opts[:bind_arguments]
      end

      # Set the bind arguments based on the hash and call super.
      def call(bind_vars={}, &block)
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
    end

    # Backbone of the prepared statement support.  Grafts bind variable
    # support into datasets by hijacking #literal and using placeholders.
    # By default, emulates prepared statements and bind variables by
    # taking the hash of bind variables and directly substituting them
    # into the query, which works on all databases, as it is no different
    # from using the dataset without bind variables.
    module PreparedStatementMethods
      Dataset.def_deprecated_opts_setter(self, :log_sql, :prepared_type, :prepared_args, :orig_dataset, :prepared_modify_values)

      PLACEHOLDER_RE = /\A\$(.*)\z/
      
      # Whether to log the full SQL query.  By default, just the prepared statement
      # name is generally logged on adapters that support native prepared statements.
      def log_sql
        @opts[:log_sql]
      end
      
      # The type of prepared statement, should be one of :select, :first,
      # :insert, :update, or :delete
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
      def call(bind_vars={}, &block)
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
      
      # Returns the SQL for the prepared statement, depending on
      # the type of the statement and the prepared_modify_values.
      def prepared_sql
        case prepared_type
        when :select, :all, :each
          # Most common scenario, so listed first.
          select_sql
        when :first
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
        if @opts[:bind_vars] and match = PLACEHOLDER_RE.match(v.to_s)
          s = match[1].to_sym
          if prepared_arg?(s)
            literal_append(sql, prepared_arg(s))
          else
            sql << v.to_s
          end
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
      
      # Run the method based on the type of prepared statement, with
      # :select running #all to get all of the rows, and the other
      # types running the method with the same name as the type.
      def run(&block)
        case prepared_type
        when :select, :all
          # Most common scenario, so listed first
          all(&block)
        when :each
          each(&block)
        when :insert_select
          with_sql(prepared_sql).first
        when :first
          first
        when :insert, :update, :delete
          if opts[:returning] && supports_returning?(prepared_type)
            returning_fetch_rows(prepared_sql)
          elsif prepared_type == :delete
            delete
          else
            send(prepared_type, *prepared_modify_values)
          end
        when :insert_pk
          fetch_rows(prepared_sql){|r| return r.values.first}
        when Array
          case prepared_type.at(0)
          when :map, :to_hash, :to_hash_groups
            send(*prepared_type, &block) 
          end
        else
          all(&block)
        end
      end
      
      private
      
      # Returns the value of the prepared_args hash for the given key.
      def prepared_arg(k)
        @opts[:bind_vars][k]
      end

      # Whether there is a bound value for the given key.
      def prepared_arg?(k)
        @opts[:bind_vars].has_key?(k)
      end

      # The symbol cache should always be skipped, since placeholders
      # are symbols.
      def skip_symbol_cache?
        true
      end

      # Use a clone of the dataset extended with prepared statement
      # support and using the same argument hash so that you can use
      # bind variables/prepared arguments in subselects.
      def subselect_sql_append(sql, ds)
        ds.clone(:append_sql=>sql, :prepared_args=>prepared_args, :bind_vars=>@opts[:bind_vars]).
          send(:to_prepared_statement, :select, nil, :extend=>prepared_statement_modules).
          prepared_sql
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
      
      # Always assume there is a prepared arg in the argument mapper.
      def prepared_arg?(k)
        true
      end
    end
    
    # Set the bind variables to use for the call.  If bind variables have
    # already been set for this dataset, they are updated with the contents
    # of bind_vars.
    #
    #   DB[:table].where(:id=>:$id).bind(:id=>1).call(:first)
    #   # SELECT * FROM table WHERE id = ? LIMIT 1 -- (1)
    #   # => {:id=>1}
    def bind(bind_vars={})
      bind_vars = if bv = @opts[:bind_vars]
        Hash[bv].merge!(bind_vars).freeze
      else
        if bind_vars.frozen?
          bind_vars
        else
          Hash[bind_vars]
        end
      end

      clone(:bind_vars=>bind_vars)
    end
    
    # For the given type (:select, :first, :insert, :insert_select, :update, or :delete),
    # run the sql with the bind variables specified in the hash.  +values+ is a hash passed to
    # insert or update (if one of those types is used), which may contain placeholders.
    #
    #   DB[:table].where(:id=>:$id).call(:first, :id=>1)
    #   # SELECT * FROM table WHERE id = ? LIMIT 1 -- (1)
    #   # => {:id=>1}
    def call(type, bind_variables={}, *values, &block)
      to_prepared_statement(type, values, :extend=>bound_variable_modules).call(bind_variables, &block)
    end
    
    # Prepare an SQL statement for later execution.  Takes a type similar to #call,
    # and the +name+ symbol of the prepared statement.  While +name+ defaults to +nil+,
    # it should always be provided as a symbol for the name of the prepared statement,
    # as some databases require that prepared statements have names.
    #
    # This returns a clone of the dataset extended with PreparedStatementMethods,
    # which you can +call+ with the hash of bind variables to use.
    # The prepared statement is also stored in
    # the associated database, where it can be called by name.
    # The following usage is identical:
    #
    #   ps = DB[:table].where(:name=>:$name).prepare(:first, :select_by_name)
    #
    #   ps.call(:name=>'Blah')
    #   # SELECT * FROM table WHERE name = ? -- ('Blah')
    #   # => {:id=>1, :name=>'Blah'}
    #
    #   DB.call(:select_by_name, :name=>'Blah') # Same thing
    def prepare(type, name=nil, *values)
      ps = to_prepared_statement(type, values, :name=>name, :extend=>prepared_statement_modules)

      if name
        ps.prepared_sql
        db.set_prepared_statement(name, ps)
      else
        # :nocov:
        Sequel::Deprecation.deprecate("Dataset#prepare will change to requiring a name argument in Sequel 5, please update your code.") unless name
        # :nocov:
      end

      ps
    end
    
    protected
    
    # Return a cloned copy of the current dataset extended with
    # PreparedStatementMethods, setting the type and modify values.
    def to_prepared_statement(type, values=nil, opts=OPTS)
      mods = opts[:extend] || []
      mods += [PreparedStatementMethods]

      bind.
        clone(:prepared_statement_name=>opts[:name], :prepared_type=>type, :prepared_modify_values=>values, :orig_dataset=>self, :no_cache_sql=>true, :prepared_args=>@opts[:prepared_args]||[]).
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
