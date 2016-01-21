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
    
    # Default implementation of the argument mapper to allow
    # native database support for bind variables and prepared
    # statements (as opposed to the emulated ones used by default).
    module ArgumentMapper
      # The name of the prepared statement, if any.
      attr_accessor :prepared_statement_name
      
      # The bind arguments to use for running this prepared statement
      attr_accessor :bind_arguments

      # Set the bind arguments based on the hash and call super.
      def call(bind_vars={}, &block)
        ds = bind(bind_vars)
        ds.prepared_sql
        ds.bind_arguments = ds.map_to_prepared_args(ds.opts[:bind_vars])
        ds.run(&block)
      end
        
      # Override the given *_sql method based on the type, and
      # cache the result of the sql.
      def prepared_sql
        return @prepared_sql if @prepared_sql
        @prepared_args ||= []
        @prepared_sql = super
        @opts[:sql] = @prepared_sql
        @prepared_sql
      end
    end

    # Backbone of the prepared statement support.  Grafts bind variable
    # support into datasets by hijacking #literal and using placeholders.
    # By default, emulates prepared statements and bind variables by
    # taking the hash of bind variables and directly substituting them
    # into the query, which works on all databases, as it is no different
    # from using the dataset without bind variables.
    module PreparedStatementMethods
      PLACEHOLDER_RE = /\A\$(.*)\z/
      
      # Whether to log the full SQL query.  By default, just the prepared statement
      # name is generally logged on adapters that support native prepared statements.
      attr_accessor :log_sql
      
      # The type of prepared statement, should be one of :select, :first,
      # :insert, :update, or :delete
      attr_accessor :prepared_type
      
      # The array/hash of bound variable placeholder names.
      attr_accessor :prepared_args
      
      # The dataset that created this prepared statement.
      attr_accessor :orig_dataset
      
      # The argument to supply to insert and update, which may use
      # placeholders specified by prepared_args
      attr_accessor :prepared_modify_values

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
        case @prepared_type
        when :select, :all, :each
          # Most common scenario, so listed first.
          select_sql
        when :first
          clone(:limit=>1).select_sql
        when :insert_select
          insert_select_sql(*@prepared_modify_values)
        when :insert
          insert_sql(*@prepared_modify_values)
        when :update
          update_sql(*@prepared_modify_values)
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
        case @prepared_type
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
          if opts[:returning] && supports_returning?(@prepared_type)
            returning_fetch_rows(prepared_sql)
          elsif @prepared_type == :delete
            delete
          else
            send(@prepared_type, *@prepared_modify_values)
          end
        when Array
          case @prepared_type.at(0)
          when :map, :to_hash, :to_hash_groups
            send(*@prepared_type, &block) 
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
        ps = ds.clone(:append_sql=>sql).prepare(:select)
        ps = ps.bind(@opts[:bind_vars]) if @opts[:bind_vars]
        ps.prepared_args = prepared_args
        ps.prepared_sql
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
    #   DB[:table].filter(:id=>:$id).bind(:id=>1).call(:first)
    #   # SELECT * FROM table WHERE id = ? LIMIT 1 -- (1)
    #   # => {:id=>1}
    def bind(bind_vars={})
      clone(:bind_vars=>@opts[:bind_vars] ? Hash[@opts[:bind_vars]].merge!(bind_vars) : bind_vars)
    end
    
    # For the given type (:select, :first, :insert, :insert_select, :update, or :delete),
    # run the sql with the bind variables specified in the hash.  +values+ is a hash passed to
    # insert or update (if one of those types is used), which may contain placeholders.
    #
    #   DB[:table].filter(:id=>:$id).call(:first, :id=>1)
    #   # SELECT * FROM table WHERE id = ? LIMIT 1 -- (1)
    #   # => {:id=>1}
    def call(type, bind_variables={}, *values, &block)
      prepare(type, nil, *values).call(bind_variables, &block)
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
    #   ps = DB[:table].filter(:name=>:$name).prepare(:first, :select_by_name)
    #
    #   ps.call(:name=>'Blah')
    #   # SELECT * FROM table WHERE name = ? -- ('Blah')
    #   # => {:id=>1, :name=>'Blah'}
    #
    #   DB.call(:select_by_name, :name=>'Blah') # Same thing
    def prepare(type, name=nil, *values)
      ps = to_prepared_statement(type, values)
      db.set_prepared_statement(name, ps) if name
      ps
    end
    
    protected
    
    # Return a cloned copy of the current dataset extended with
    # PreparedStatementMethods, setting the type and modify values.
    def to_prepared_statement(type, values=nil)
      ps = bind
      ps.extend(PreparedStatementMethods)
      ps.orig_dataset = self
      ps.prepared_type = type
      ps.prepared_modify_values = values
      ps
    end

    private
    
    # Don't allow preparing prepared statements by default.
    def allow_preparing_prepared_statements?
      false
    end

    # The argument placeholder.  Most databases used unnumbered
    # arguments with question marks, so that is the default.
    def prepared_arg_placeholder
      PREPARED_ARG_PLACEHOLDER
    end
  end
end
