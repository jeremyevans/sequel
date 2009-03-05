module Sequel 
  class Dataset
    PREPARED_ARG_PLACEHOLDER = LiteralString.new('?').freeze
    
    # Default implementation of the argument mapper to allow
    # native database support for bind variables and prepared
    # statements (as opposed to the emulated ones used by default).
    module ArgumentMapper
      SQL_QUERY_TYPE = Hash.new{|h,k| h[k] = k}
      SQL_QUERY_TYPE[:first] = SQL_QUERY_TYPE[:all] = :select
      
      # The name of the prepared statement, if any.
      attr_accessor :prepared_statement_name
      
      # The bind arguments to use for running this prepared statement
      attr_accessor :bind_arguments
      
      # Set the bind arguments based on the hash and call super.
      def call(hash, &block)
        ds = clone
        ds.prepared_sql
        ds.bind_arguments = ds.map_to_prepared_args(hash)
        ds.prepared_args = hash
        ds.run(&block)
      end
        
      # Override the given *_sql method based on the type, and
      # cache the result of the sql.
      def prepared_sql
        return @prepared_sql if @prepared_sql
        @prepared_args ||= []
        @prepared_sql = super
        meta_def("#{sql_query_type}_sql"){|*args| prepared_sql}
        @prepared_sql
      end
      
      private
      
      # The type of query (:select, :insert, :delete, :update).
      def sql_query_type
        SQL_QUERY_TYPE[@prepared_type]
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
      
      # The type of prepared statement, should be one of :select, :first,
      # :insert, :update, or :delete
      attr_accessor :prepared_type
      
      # The bind variable hash to use when substituting
      attr_accessor :prepared_args
      
      # The argument to supply to insert and update, which may use
      # placeholders specified by prepared_args
      attr_accessor :prepared_modify_values
      
      # Sets the prepared_args to the given hash and runs the
      # prepared statement.
      def call(hash, &block)
        ds = clone
        ds.prepared_args = hash
        ds.run(&block)
      end
      
      # Returns the SQL for the prepared statement, depending on
      # the type of the statement and the prepared_modify_values.
      def prepared_sql
        case @prepared_type
        when :select, :all
          select_sql
        when :first
          clone(:limit=>1).select_sql
        when :insert
          insert_sql(@prepared_modify_values)
        when :update
          update_sql(@prepared_modify_values)
        when :delete
          delete_sql
        end
      end
      
      # Changes the values of symbols if they start with $ and
      # prepared_args is present.  If so, they are considered placeholders,
      # and they are substituted using prepared_arg.
      def literal(v)
        case v
        when Symbol
          if match = PLACEHOLDER_RE.match(v.to_s) and @prepared_args
            super(prepared_arg(match[1].to_sym))
          else
            super
          end
        else
          super
        end
      end
      
      # Programmer friendly string showing this is a prepared statement,
      # with the prepared SQL it represents (which in general won't have
      # substituted variables).
      def inspect
        "<#{self.class.name}/PreparedStatement #{prepared_sql.inspect}>"
      end
      
      protected
      
      # Run the method based on the type of prepared statement, with
      # :select running #all to get all of the rows, and the other
      # types running the method with the same name as the type.
      def run(&block)
        case @prepared_type
        when :select, :all
          all(&block)
        when :first
          first
        when :insert
          insert(@prepared_modify_values)
        when :update
          update(@prepared_modify_values)
        when :delete
          delete
        end
      end
      
      private
      
      # Returns the value of the prepared_args hash for the given key.
      def prepared_arg(k)
        @prepared_args[k]
      end

      # Use a clone of the dataset extended with prepared statement
      # support and using the same argument hash so that you can use
      # bind variables/prepared arguments in subselects.
      def subselect_sql(ds)
        ps = ds.prepare(:select)
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
      def map_to_prepared_args(hash)
        @prepared_args.map{|v| hash[v]}
      end
      
      private
      
      # Associates the argument with name k with the next position in
      # the output array.
      def prepared_arg(k)
        @prepared_args << k
        prepared_arg_placeholder
      end
    end
    
    # For the given type (:select, :insert, :update, or :delete),
    # run the sql with the bind variables
    # specified in the hash.  values is a hash of passed to
    # insert or update (if one of those types is used),
    # which may contain placeholders.
    def call(type, bind_variables={}, values=nil)
      prepare(type, nil, values).call(bind_variables)
    end
    
    # Prepare an SQL statement for later execution. This returns
    # a clone of the dataset extended with PreparedStatementMethods,
    # on which you can call call with the hash of bind variables to
    # do substitution.  The prepared statement is also stored in
    # the associated database.  The following usage is identical:
    #
    #   ps = prepare(:select, :select_by_name)
    #   ps.call(:name=>'Blah')
    #   db.call(:select_by_name, :name=>'Blah')
    def prepare(type, name=nil, values=nil)
      ps = to_prepared_statement(type, values)
      db.prepared_statements[name] = ps if name
      ps
    end
    
    protected
    
    # Return a cloned copy of the current dataset extended with
    # PreparedStatementMethods, setting the type and modify values.
    def to_prepared_statement(type, values=nil)
      ps = clone
      ps.extend(PreparedStatementMethods)
      ps.prepared_type = type
      ps.prepared_modify_values = values
      ps
    end

    private
    
    # The argument placeholder.  Most databases used unnumbered
    # arguments with question marks, so that is the default.
    def prepared_arg_placeholder
      PREPARED_ARG_PLACEHOLDER
    end
  end
end
