module Sequel
  class Dataset

    # Given a type (e.g. select) and an array of clauses,
    # return an array of methods to call to build the SQL string.
    def self.clause_methods(type, clauses)
      clauses.map{|clause| :"#{type}_#{clause}_sql"}.freeze
    end

    # These symbols have _join methods created (e.g. inner_join) that
    # call join_table with the symbol, passing along the arguments and
    # block from the method call.
    CONDITIONED_JOIN_TYPES = [:inner, :full_outer, :right_outer, :left_outer, :full, :right, :left]

    # These symbols have _join methods created (e.g. natural_join) that
    # call join_table with the symbol.  They only accept a single table
    # argument which is passed to join_table, and they raise an error
    # if called with a block.
    UNCONDITIONED_JOIN_TYPES = [:natural, :natural_left, :natural_right, :natural_full, :cross]

    AND_SEPARATOR = " AND ".freeze
    BOOL_FALSE = "'f'".freeze
    BOOL_TRUE = "'t'".freeze
    COLUMN_REF_RE1 = /\A([\w ]+)__([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE2 = /\A([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE3 = /\A([\w ]+)__([\w ]+)\z/.freeze
    COUNT_FROM_SELF_OPTS = [:distinct, :group, :sql, :limit, :compounds]
    DATASET_ALIAS_BASE_NAME = 't'.freeze
    IS_LITERALS = {nil=>'NULL'.freeze, true=>'TRUE'.freeze, false=>'FALSE'.freeze}.freeze
    IS_OPERATORS = ::Sequel::SQL::ComplexExpression::IS_OPERATORS
    N_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::N_ARITY_OPERATORS
    NULL = "NULL".freeze
    QUALIFY_KEYS = [:select, :where, :having, :order, :group]
    QUESTION_MARK = '?'.freeze
    DELETE_CLAUSE_METHODS = clause_methods(:delete, %w'from where')
    INSERT_CLAUSE_METHODS = clause_methods(:insert, %w'into columns values')
    SELECT_CLAUSE_METHODS = clause_methods(:select, %w'with distinct columns from join where group having compounds order limit')
    UPDATE_CLAUSE_METHODS = clause_methods(:update, %w'table set where')
    TIMESTAMP_FORMAT = "'%Y-%m-%d %H:%M:%S%N%z'".freeze
    STANDARD_TIMESTAMP_FORMAT = "TIMESTAMP #{TIMESTAMP_FORMAT}".freeze
    TWO_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::TWO_ARITY_OPERATORS
    WILDCARD = '*'.freeze
    SQL_WITH = "WITH ".freeze

    # SQL fragment for the aliased expression
    def aliased_expression_sql(ae)
      as_sql(literal(ae.expression), ae.aliaz)
    end

    # SQL fragment for the SQL array.
    def array_sql(a)
      a.empty? ? '(NULL)' : "(#{expression_list(a)})"     
    end

    # SQL fragment for BooleanConstants
    def boolean_constant_sql(constant)
      literal(constant)
    end

    # SQL fragment for specifying given CaseExpression.
    def case_expression_sql(ce)
      sql = '(CASE '
      sql << "#{literal(ce.expression)} " if ce.expression
      ce.conditions.collect{ |c,r|
        sql << "WHEN #{literal(c)} THEN #{literal(r)} "
      }
      sql << "ELSE #{literal(ce.default)} END)"
    end

    # SQL fragment for the SQL CAST expression.
    def cast_sql(expr, type)
      "CAST(#{literal(expr)} AS #{db.cast_type_literal(type)})"
    end

    # SQL fragment for specifying all columns in a given table.
    def column_all_sql(ca)
      "#{quote_schema_table(ca.table)}.*"
    end

    # SQL fragment for complex expressions
    def complex_expression_sql(op, args)
      case op
      when *IS_OPERATORS
        r = args.at(1)
        if r.nil? || supports_is_true?
          raise(InvalidOperation, 'Invalid argument used for IS operator') unless v = IS_LITERALS[r]
          "(#{literal(args.at(0))} #{op} #{v})"
        elsif op == :IS
          complex_expression_sql(:"=", args)
        else
          complex_expression_sql(:OR, [SQL::BooleanExpression.new(:"!=", *args), SQL::BooleanExpression.new(:IS, args.at(0), nil)])
        end
      when :IN, :"NOT IN"
        cols = args.at(0)
        if !supports_multiple_column_in? && cols.is_a?(Array)
          expr = SQL::BooleanExpression.new(:OR, *args.at(1).to_a.map{|vals| SQL::BooleanExpression.from_value_pairs(cols.zip(vals).map{|col, val| [col, val]})})
          literal(op == :IN ? expr : ~expr)
        else
          "(#{literal(cols)} #{op} #{literal(args.at(1))})"
        end
      when *TWO_ARITY_OPERATORS
        "(#{literal(args.at(0))} #{op} #{literal(args.at(1))})"
      when *N_ARITY_OPERATORS
        "(#{args.collect{|a| literal(a)}.join(" #{op} ")})"
      when :NOT
        "NOT #{literal(args.at(0))}"
      when :NOOP
        literal(args.at(0))
      when :'B~'
        "~#{literal(args.at(0))}"
      else
        raise(InvalidOperation, "invalid operator #{op}")
      end
    end
    
    # SQL fragment for constants
    def constant_sql(constant)
      constant.to_s
    end

    # Returns the number of records in the dataset.
    def count
      aggregate_dataset.get{COUNT(:*){}.as(count)}.to_i
    end

    # Formats a DELETE statement using the given options and dataset options.
    # 
    #   dataset.filter{|o| o.price >= 100}.delete_sql #=>
    #     "DELETE FROM items WHERE (price >= 100)"
    def delete_sql
      return static_sql(opts[:sql]) if opts[:sql]
      check_modification_allowed!
      clause_sql(:delete)
    end

    # Returns an EXISTS clause for the dataset as a LiteralString.
    #
    #   DB.select(1).where(DB[:items].exists).sql
    #   #=> "SELECT 1 WHERE (EXISTS (SELECT * FROM items))"
    def exists
      LiteralString.new("EXISTS (#{select_sql})")
    end

    # The first source (primary table) for this dataset.  If the dataset doesn't
    # have a table, raises an error.  If the table is aliased, returns the aliased name.
    def first_source_alias
      source = @opts[:from]
      if source.nil? || source.empty?
        raise Error, 'No source specified for query'
      end
      case s = source.first
      when SQL::AliasedExpression
        s.aliaz
      when Symbol
        sch, table, aliaz = split_symbol(s)
        aliaz ? aliaz.to_sym : s
      else
        s
      end
    end
    alias first_source first_source_alias

    # SQL fragment specifying an SQL function call
    def function_sql(f)
      args = f.args
      "#{f.f}#{args.empty? ? '()' : literal(args)}"
    end

    # Inserts multiple values. If a block is given it is invoked for each
    # item in the given array before inserting it.  See #multi_insert as
    # a possible faster version that inserts multiple records in one
    # SQL statement.
    def insert_multiple(array, &block)
      if block
        array.each {|i| insert(block[i])}
      else
        array.each {|i| insert(i)}
      end
    end

    # Formats an INSERT statement using the given values. The API is a little
    # complex, and best explained by example:
    #
    #   # Default values
    #   DB[:items].insert_sql #=> 'INSERT INTO items DEFAULT VALUES'
    #   DB[:items].insert_sql({}) #=> 'INSERT INTO items DEFAULT VALUES'
    #   # Values without columns
    #   DB[:items].insert_sql(1,2,3) #=> 'INSERT INTO items VALUES (1, 2, 3)'
    #   DB[:items].insert_sql([1,2,3]) #=> 'INSERT INTO items VALUES (1, 2, 3)'
    #   # Values with columns
    #   DB[:items].insert_sql([:a, :b], [1,2]) #=> 'INSERT INTO items (a, b) VALUES (1, 2)'
    #   DB[:items].insert_sql(:a => 1, :b => 2) #=> 'INSERT INTO items (a, b) VALUES (1, 2)'
    #   # Using a subselect
    #   DB[:items].insert_sql(DB[:old_items]) #=> 'INSERT INTO items SELECT * FROM old_items
    #   # Using a subselect with columns
    #   DB[:items].insert_sql([:a, :b], DB[:old_items]) #=> 'INSERT INTO items (a, b) SELECT * FROM old_items
    def insert_sql(*values)
      return static_sql(@opts[:sql]) if @opts[:sql]

      check_modification_allowed!

      columns = []

      case values.size
      when 0
        return insert_sql({})
      when 1
        case vals = values.at(0)
        when Hash
          vals = @opts[:defaults].merge(vals) if @opts[:defaults]
          vals = vals.merge(@opts[:overrides]) if @opts[:overrides]
          values = []
          vals.each do |k,v| 
            columns << k
            values << v
          end
        when Dataset, Array, LiteralString
          values = vals
        else
          if vals.respond_to?(:values) && (v = vals.values).is_a?(Hash)
            return insert_sql(v) 
          end
        end
      when 2
        if (v0 = values.at(0)).is_a?(Array) && ((v1 = values.at(1)).is_a?(Array) || v1.is_a?(Dataset) || v1.is_a?(LiteralString))
          columns, values = v0, v1
          raise(Error, "Different number of values and columns given to insert_sql") if values.is_a?(Array) and columns.length != values.length
        end
      end

      columns = columns.map{|k| literal(String === k ? k.to_sym : k)}
      clone(:columns=>columns, :values=>values)._insert_sql
    end

    # SQL fragment specifying a JOIN clause without ON or USING.
    def join_clause_sql(jc)
      table = jc.table
      table_alias = jc.table_alias
      table_alias = nil if table == table_alias
      tref = table_ref(table)
      " #{join_type_sql(jc.join_type)} #{table_alias ? as_sql(tref, table_alias) : tref}"
    end

    # SQL fragment specifying a JOIN clause with ON.
    def join_on_clause_sql(jc)
      "#{join_clause_sql(jc)} ON #{literal(filter_expr(jc.on))}"
    end

    # SQL fragment specifying a JOIN clause with USING.
    def join_using_clause_sql(jc)
      "#{join_clause_sql(jc)} USING (#{column_list(jc.using)})"
    end

    # Returns a joined dataset.  Uses the following arguments:
    #
    # * type - The type of join to do (e.g. :inner)
    # * table - Depends on type:
    #   * Dataset - a subselect is performed with an alias of tN for some value of N
    #   * Model (or anything responding to :table_name) - table.table_name
    #   * String, Symbol: table
    # * expr - specifies conditions, depends on type:
    #   * Hash, Array with all two pairs - Assumes key (1st arg) is column of joined table (unless already
    #     qualified), and value (2nd arg) is column of the last joined or primary table (or the
    #     :implicit_qualifier option).
    #     To specify multiple conditions on a single joined table column, you must use an array.
    #     Uses a JOIN with an ON clause.
    #   * Array - If all members of the array are symbols, considers them as columns and 
    #     uses a JOIN with a USING clause.  Most databases will remove duplicate columns from
    #     the result set if this is used.
    #   * nil - If a block is not given, doesn't use ON or USING, so the JOIN should be a NATURAL
    #     or CROSS join. If a block is given, uses a ON clause based on the block, see below.
    #   * Everything else - pretty much the same as a using the argument in a call to filter,
    #     so strings are considered literal, symbols specify boolean columns, and blockless
    #     filter expressions can be used. Uses a JOIN with an ON clause.
    # * options - a hash of options, with any of the following keys:
    #   * :table_alias - the name of the table's alias when joining, necessary for joining
    #     to the same table more than once.  No alias is used by default.
    #   * :implicit_qualifier - The name to use for qualifying implicit conditions.  By default,
    #     the last joined or primary table is used.
    # * block - The block argument should only be given if a JOIN with an ON clause is used,
    #   in which case it yields the table alias/name for the table currently being joined,
    #   the table alias/name for the last joined (or first table), and an array of previous
    #   SQL::JoinClause.
    def join_table(type, table, expr=nil, options={}, &block)
      using_join = expr.is_a?(Array) && !expr.empty? && expr.all?{|x| x.is_a?(Symbol)}
      if using_join && !supports_join_using?
        h = {}
        expr.each{|s| h[s] = s}
        return join_table(type, table, h, options)
      end

      if [Symbol, String].any?{|c| options.is_a?(c)}
        table_alias = options
        last_alias = nil 
      else
        table_alias = options[:table_alias]
        last_alias = options[:implicit_qualifier]
      end
      if Dataset === table
        if table_alias.nil?
          table_alias_num = (@opts[:num_dataset_sources] || 0) + 1
          table_alias = dataset_alias(table_alias_num)
        end
        table_name = table_alias
      else
        table = table.table_name if table.respond_to?(:table_name)
        table_name = table_alias || table
      end

      join = if expr.nil? and !block_given?
        SQL::JoinClause.new(type, table, table_alias)
      elsif using_join
        raise(Sequel::Error, "can't use a block if providing an array of symbols as expr") if block_given?
        SQL::JoinUsingClause.new(expr, type, table, table_alias)
      else
        last_alias ||= @opts[:last_joined_table] || first_source_alias
        if Sequel.condition_specifier?(expr)
          expr = expr.collect do |k, v|
            k = qualified_column_name(k, table_name) if k.is_a?(Symbol)
            v = qualified_column_name(v, last_alias) if v.is_a?(Symbol)
            [k,v]
          end
        end
        if block_given?
          expr2 = yield(table_name, last_alias, @opts[:join] || [])
          expr = expr ? SQL::BooleanExpression.new(:AND, expr, expr2) : expr2
        end
        SQL::JoinOnClause.new(expr, type, table, table_alias)
      end

      opts = {:join => (@opts[:join] || []) + [join], :last_joined_table => table_name}
      opts[:num_dataset_sources] = table_alias_num if table_alias_num
      clone(opts)
    end

    # Returns a literal representation of a value to be used as part
    # of an SQL expression. 
    # 
    #   dataset.literal("abc'def\\") #=> "'abc''def\\\\'"
    #   dataset.literal(:items__id) #=> "items.id"
    #   dataset.literal([1, 2, 3]) => "(1, 2, 3)"
    #   dataset.literal(DB[:items]) => "(SELECT * FROM items)"
    #   dataset.literal(:x + 1 > :y) => "((x + 1) > y)"
    #
    # If an unsupported object is given, an exception is raised.
    def literal(v)
      case v
      when String
        return v if v.is_a?(LiteralString)
        v.is_a?(SQL::Blob) ? literal_blob(v) : literal_string(v)
      when Symbol
        literal_symbol(v)
      when Integer
        literal_integer(v)
      when Hash
        literal_hash(v)
      when SQL::Expression
        literal_expression(v)
      when Float
        literal_float(v)
      when BigDecimal
        literal_big_decimal(v)
      when NilClass
        NULL
      when TrueClass
        literal_true
      when FalseClass
        literal_false
      when Array
        literal_array(v)
      when Time
        literal_time(v)
      when DateTime
        literal_datetime(v)
      when Date
        literal_date(v)
      when Dataset
        literal_dataset(v)
      else
        literal_other(v)
      end
    end

    # Returns an array of insert statements for inserting multiple records.
    # This method is used by #multi_insert to format insert statements and
    # expects a keys array and and an array of value arrays.
    #
    # This method should be overridden by descendants if the support
    # inserting multiple records in a single SQL statement.
    def multi_insert_sql(columns, values)
      values.map{|r| insert_sql(columns, r)}
    end
    
    # SQL fragment for NegativeBooleanConstants
    def negative_boolean_constant_sql(constant)
      "NOT #{boolean_constant_sql(constant)}"
    end

    # SQL fragment for the ordered expression, used in the ORDER BY
    # clause.
    def ordered_expression_sql(oe)
      "#{literal(oe.expression)} #{oe.descending ? 'DESC' : 'ASC'}"
    end

    # SQL fragment for a literal string with placeholders
    def placeholder_literal_string_sql(pls)
      args = pls.args
      s = if args.is_a?(Hash)
        re = /:(#{args.keys.map{|k| Regexp.escape(k.to_s)}.join('|')})\b/
        pls.str.gsub(re){literal(args[$1.to_sym])}
      else
        i = -1
        pls.str.gsub(QUESTION_MARK){literal(args.at(i+=1))}
      end
      s = "(#{s})" if pls.parens
      s
    end

    # SQL fragment for the qualifed identifier, specifying
    # a table and a column (or schema and table).
    def qualified_identifier_sql(qcr)
      [qcr.table, qcr.column].map{|x| [SQL::QualifiedIdentifier, SQL::Identifier, Symbol].any?{|c| x.is_a?(c)} ? literal(x) : quote_identifier(x)}.join('.')
    end

    # Qualify to the given table, or first source if not table is given.
    def qualify(table=first_source)
      qualify_to(table)
    end

    # Return a copy of the dataset with unqualified identifiers in the
    # SELECT, WHERE, GROUP, HAVING, and ORDER clauses qualified by the
    # given table. If no columns are currently selected, select all
    # columns of the given table.
    def qualify_to(table)
      o = @opts
      return clone if o[:sql]
      h = {}
      (o.keys & QUALIFY_KEYS).each do |k|
        h[k] = qualified_expression(o[k], table)
      end
      h[:select] = [SQL::ColumnAll.new(table)] if !o[:select] || o[:select].empty?
      clone(h)
    end
    
    # Qualify the dataset to its current first source.  This is useful
    # if you have unqualified identifiers in the query that all refer to
    # the first source, and you want to join to another table which
    # has columns with the same name as columns in the current dataset.
    # See qualify_to.
    def qualify_to_first_source
      qualify_to(first_source)
    end

    # Adds quoting to identifiers (columns and tables). If identifiers are not
    # being quoted, returns name as a string.  If identifiers are being quoted
    # quote the name with quoted_identifier.
    def quote_identifier(name)
      return name if name.is_a?(LiteralString)
      name = name.value if name.is_a?(SQL::Identifier)
      name = input_identifier(name)
      name = quoted_identifier(name) if quote_identifiers?
      name
    end

    # Separates the schema from the table and returns a string with them
    # quoted (if quoting identifiers)
    def quote_schema_table(table)
      schema, table = schema_and_table(table)
      "#{"#{quote_identifier(schema)}." if schema}#{quote_identifier(table)}"
    end

    # This method quotes the given name with the SQL standard double quote. 
    # should be overridden by subclasses to provide quoting not matching the
    # SQL standard, such as backtick (used by MySQL and SQLite).
    def quoted_identifier(name)
      "\"#{name.to_s.gsub('"', '""')}\""
    end

    # Split the schema information from the table
    def schema_and_table(table_name)
      sch = db.default_schema if db
      case table_name
      when Symbol
        s, t, a = split_symbol(table_name)
        [s||sch, t]
      when SQL::QualifiedIdentifier
        [table_name.table, table_name.column]
      when SQL::Identifier
        [sch, table_name.value]
      when String
        [sch, table_name]
      else
        raise Error, 'table_name should be a Symbol, SQL::QualifiedIdentifier, SQL::Identifier, or String'
      end
    end

    # Formats a SELECT statement
    #
    #   dataset.select_sql # => "SELECT * FROM items"
    def select_sql
      return static_sql(@opts[:sql]) if @opts[:sql]
      clause_sql(:select)
    end

    # Same as select_sql, not aliased directly to make subclassing simpler.
    def sql
      select_sql
    end

    # SQL fragment for specifying subscripts (SQL arrays)
    def subscript_sql(s)
      "#{literal(s.f)}[#{expression_list(s.sub)}]"
    end
    
    # SQL query to truncate the table
    def truncate_sql
      if opts[:sql]
        static_sql(opts[:sql])
      else
        check_modification_allowed!
        raise(InvalidOperation, "Can't truncate filtered datasets") if opts[:where]
        _truncate_sql(source_list(opts[:from]))
      end
    end

    # Formats an UPDATE statement using the given values.
    #
    #   dataset.update_sql(:price => 100, :category => 'software') #=>
    #     "UPDATE items SET price = 100, category = 'software'"
    #
    # Raises an error if the dataset is grouped or includes more
    # than one table.
    def update_sql(values = {})
      return static_sql(opts[:sql]) if opts[:sql]
      check_modification_allowed!
      clone(:values=>values)._update_sql
    end

    # Add a condition to the WHERE clause.  See #filter for argument types.
    #
    #   dataset.group(:a).having(:a).filter(:b) # SELECT * FROM items GROUP BY a HAVING a AND b
    #   dataset.group(:a).having(:a).where(:b) # SELECT * FROM items WHERE b GROUP BY a HAVING a
    def where(*cond, &block)
      _filter(:where, *cond, &block)
    end

    # The SQL fragment for the given window's options.
    def window_sql(opts)
      raise(Error, 'This dataset does not support window functions') unless supports_window_functions?
      window = literal(opts[:window]) if opts[:window]
      partition = "PARTITION BY #{expression_list(Array(opts[:partition]))}" if opts[:partition]
      order = "ORDER BY #{expression_list(Array(opts[:order]))}" if opts[:order]
      frame = case opts[:frame]
        when nil
          nil
        when :all
          "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
        when :rows
          "ROWS UNBOUNDED PRECEDING"
        else
          raise Error, "invalid window frame clause, should be :all, :rows, or nil"
      end
      "(#{[window, partition, order, frame].compact.join(' ')})"
    end

    # The SQL fragment for the given window function's function and window.
    def window_function_sql(function, window)
      "#{literal(function)} OVER #{literal(window)}"
    end
    
    # Add a simple common table expression (CTE) with the given name and a dataset that defines the CTE.
    # A common table expression acts as an inline view for the query.
    # Options:
    # * :args - Specify the arguments/columns for the CTE, should be an array of symbols.
    # * :recursive - Specify that this is a recursive CTE
    def with(name, dataset, opts={})
      raise(Error, 'This datatset does not support common table expressions') unless supports_cte?
      clone(:with=>(@opts[:with]||[]) + [opts.merge(:name=>name, :dataset=>dataset)])
    end

    # Add a recursive common table expression (CTE) with the given name, a dataset that
    # defines the nonrecursive part of the CTE, and a dataset that defines the recursive part
    # of the CTE.  Options:
    # * :args - Specify the arguments/columns for the CTE, should be an array of symbols.
    # * :union_all - Set to false to use UNION instead of UNION ALL combining the nonrecursive and recursive parts.
    def with_recursive(name, nonrecursive, recursive, opts={})
      raise(Error, 'This datatset does not support common table expressions') unless supports_cte?
      clone(:with=>(@opts[:with]||[]) + [opts.merge(:recursive=>true, :name=>name, :dataset=>nonrecursive.union(recursive, {:all=>opts[:union_all] != false, :from_self=>false}))])
    end

    # Returns a copy of the dataset with the static SQL used.  This is useful if you want
    # to keep the same row_proc/graph, but change the SQL used to custom SQL.
    #
    #   dataset.with_sql('SELECT * FROM foo') # SELECT * FROM foo
    def with_sql(sql, *args)
      sql = SQL::PlaceholderLiteralString.new(sql, args) unless args.empty?
      clone(:sql=>sql)
    end

    CONDITIONED_JOIN_TYPES.each do |jtype|
      class_eval("def #{jtype}_join(*args, &block); join_table(:#{jtype}, *args, &block) end", __FILE__, __LINE__)
    end
    UNCONDITIONED_JOIN_TYPES.each do |jtype|
      class_eval("def #{jtype}_join(table); raise(Sequel::Error, '#{jtype}_join does not accept join table blocks') if block_given?; join_table(:#{jtype}, table) end", __FILE__, __LINE__)
    end
    alias join inner_join

    protected

    # Formats in INSERT statement using the stored columns and values.
    def _insert_sql
      clause_sql(:insert)
    end

    # Formats an UPDATE statement using the stored values.
    def _update_sql
      clause_sql(:update)
    end

    # Return a from_self dataset if an order or limit is specified, so it works as expected
    # with UNION, EXCEPT, and INTERSECT clauses.
    def compound_from_self
      (@opts[:limit] || @opts[:order]) ? from_self : self
    end
    
    private

    # Formats the truncate statement.  Assumes the table given has already been
    # literalized.
    def _truncate_sql(table)
      "TRUNCATE TABLE #{table}"
    end

    # Clone of this dataset usable in aggregate operations.  Does
    # a from_self if dataset contains any parameters that would
    # affect normal aggregation, or just removes an existing
    # order if not.
    def aggregate_dataset
      options_overlap(COUNT_FROM_SELF_OPTS) ? from_self : unordered
    end

    # Do a simple join of the arguments (which should be strings or symbols) separated by commas
    def argument_list(args)
      args.join(COMMA_SEPARATOR)
    end

    # SQL fragment for specifying an alias.  expression should already be literalized.
    def as_sql(expression, aliaz)
      "#{expression} AS #{quote_identifier(aliaz)}"
    end
    
    # Raise an InvalidOperation exception if deletion is not allowed
    # for this dataset
    def check_modification_allowed!
      raise(InvalidOperation, "Grouped datasets cannot be modified") if opts[:group]
      raise(InvalidOperation, "Joined datasets cannot be modified") if (opts[:from].is_a?(Array) && opts[:from].size > 1) || opts[:join]
    end

    # Prepare an SQL statement by calling all clause methods for the given statement type.
    def clause_sql(type)
      sql = type.to_s.upcase
      send("#{type}_clause_methods").each{|x| send(x, sql)}
      sql
    end

    # Converts an array of column names into a comma seperated string of 
    # column names. If the array is empty, a wildcard (*) is returned.
    def column_list(columns)
      (columns.nil? || columns.empty?) ? WILDCARD : expression_list(columns)
    end
    
    # The alias to use for datasets, takes a number to make sure the name is unique.
    def dataset_alias(number)
      :"#{DATASET_ALIAS_BASE_NAME}#{number}"
    end
    
    # The order of methods to call to build the DELETE SQL statement
    def delete_clause_methods
      DELETE_CLAUSE_METHODS
    end

    # Converts an array of expressions into a comma separated string of
    # expressions.
    def expression_list(columns)
      columns.map{|i| literal(i)}.join(COMMA_SEPARATOR)
    end
    
    # The strftime format to use when literalizing the time.
    def default_timestamp_format
      requires_sql_standard_datetimes? ? STANDARD_TIMESTAMP_FORMAT : TIMESTAMP_FORMAT
    end
    
    # Format the timestamp based on the default_timestamp_format, with a couple
    # of modifiers.  First, allow %N to be used for fractions seconds (if the
    # database supports them), and override %z to always use a numeric offset
    # of hours and minutes.
    def format_timestamp(v)
      v2 = Sequel.application_to_database_timestamp(v)
      fmt = default_timestamp_format.gsub(/%[Nz]/) do |m|
        if m == '%N'
          format_timestamp_usec(v.is_a?(DateTime) ? v.sec_fraction*86400000000 : v.usec) if supports_timestamp_usecs?
        else
          if supports_timestamp_timezones?
            # Would like to just use %z format, but it doesn't appear to work on Windows
            # Instead, the offset fragment is constructed manually
            minutes = (v2.is_a?(DateTime) ? v2.offset * 1440 : v2.utc_offset/60).to_i
            format_timestamp_offset(*minutes.divmod(60))
          end
        end
      end
      v2.strftime(fmt)
    end
    
    # Return the SQL timestamp fragment to use for the timezone offset.
    def format_timestamp_offset(hour, minute)
      sprintf("%+03i%02i", hour, minute)
    end

    # Return the SQL timestamp fragment to use for the fractional time part.
    # Should start with the decimal point.  Uses 6 decimal places by default.
    def format_timestamp_usec(usec)
      sprintf(".%06d", usec)
    end

    # SQL fragment specifying a list of identifiers
    # SQL fragment specifying a list of identifiers
    def identifier_list(columns)
      columns.map{|i| quote_identifier(i)}.join(COMMA_SEPARATOR)
    end

    # SQL fragment specifying the table to insert INTO
    def insert_into_sql(sql)
      sql << " INTO #{source_list(@opts[:from])}"
    end

    # The order of methods to call to build the INSERT SQL statement
    def insert_clause_methods
      INSERT_CLAUSE_METHODS
    end

    # SQL fragment specifying the columns to insert into
    def insert_columns_sql(sql)
      columns = opts[:columns]
      sql << " (#{columns.join(COMMA_SEPARATOR)})" if columns && !columns.empty?
    end

    # SQL fragment specifying the values to insert.
    def insert_values_sql(sql)
      case values = opts[:values]
      when Array
        sql << (values.empty? ? " DEFAULT VALUES" : " VALUES #{literal(values)}")
      when Dataset
        sql << " #{subselect_sql(values)}"
      when LiteralString
        sql << " #{values}"
      else
        raise Error, "Unsupported INSERT values type, should be an Array or Dataset: #{values.inspect}"
      end
    end

    # SQL fragment specifying a JOIN type, converts underscores to
    # spaces and upcases.
    def join_type_sql(join_type)
      "#{join_type.to_s.gsub('_', ' ').upcase} JOIN"
    end

    # SQL fragment for Array.  Treats as an expression if an array of all two pairs, or as a SQL array otherwise.
    def literal_array(v)
      Sequel.condition_specifier?(v) ? literal_expression(SQL::BooleanExpression.from_value_pairs(v)) : array_sql(v)
    end

    # SQL fragment for BigDecimal
    def literal_big_decimal(v)
      d = v.to_s("F")
      v.nan? || v.infinite? ?  "'#{d}'" : d
    end

    # SQL fragment for SQL::Blob
    def literal_blob(v)
      literal_string(v)
    end

    # SQL fragment for Dataset.  Does a subselect inside parantheses.
    def literal_dataset(v)
      "(#{subselect_sql(v)})"
    end

    # SQL fragment for Date, using the ISO8601 format.
    def literal_date(v)
      requires_sql_standard_datetimes? ? v.strftime("DATE '%Y-%m-%d'") : "'#{v}'"
    end

    # SQL fragment for DateTime
    def literal_datetime(v)
      format_timestamp(v)
    end

    # SQL fragment for SQL::Expression, result depends on the specific type of expression.
    def literal_expression(v)
      v.to_s(self)
    end

    # SQL fragment for false
    def literal_false
      BOOL_FALSE
    end

    # SQL fragment for Float
    def literal_float(v)
      v.to_s
    end

    # SQL fragment for Hash, treated as an expression
    def literal_hash(v)
      literal_expression(SQL::BooleanExpression.from_value_pairs(v))
    end

    # SQL fragment for Integer
    def literal_integer(v)
      v.to_s
    end

    # SQL fragment for a type of object not handled by Dataset#literal.
    # Calls sql_literal if object responds to it, otherwise raises an error.
    # Classes implementing sql_literal should call a class-specific method on the dataset
    # provided and should add that method to Sequel::Dataset, allowing for adapters
    # to provide customized literalizations.
    # If a database specific type is allowed, this should be overriden in a subclass.
    def literal_other(v)
      if v.respond_to?(:sql_literal)
        v.sql_literal(self)
      else
        raise Error, "can't express #{v.inspect} as a SQL literal"
      end
    end

    # SQL fragment for String.  Doubles \ and ' by default.
    def literal_string(v)
      "'#{v.gsub(/\\/, "\\\\\\\\").gsub(/'/, "''")}'"
    end

    # Converts a symbol into a column name. This method supports underscore
    # notation in order to express qualified (two underscores) and aliased
    # (three underscores) columns:
    #
    #   dataset.literal(:abc) #=> "abc"
    #   dataset.literal(:abc___a) #=> "abc AS a"
    #   dataset.literal(:items__abc) #=> "items.abc"
    #   dataset.literal(:items__abc___a) #=> "items.abc AS a"
    def literal_symbol(v)
      c_table, column, c_alias = split_symbol(v)
      qc = "#{"#{quote_identifier(c_table)}." if c_table}#{quote_identifier(column)}"
      c_alias ? as_sql(qc, c_alias) : qc
    end

    # SQL fragment for Time
    def literal_time(v)
      format_timestamp(v)
    end

    # SQL fragment for true
    def literal_true
      BOOL_TRUE
    end

    # Returns a qualified column name (including a table name) if the column
    # name isn't already qualified.
    def qualified_column_name(column, table)
      if Symbol === column 
        c_table, column, c_alias = split_symbol(column)
        unless c_table
          case table
          when Symbol
            schema, table, t_alias = split_symbol(table)
            t_alias ||= Sequel::SQL::QualifiedIdentifier.new(schema, table) if schema
          when Sequel::SQL::AliasedExpression
            t_alias = table.aliaz
          end
          c_table = t_alias || table
        end
        ::Sequel::SQL::QualifiedIdentifier.new(c_table, column)
      else
        column
      end
    end
    
    # Qualify the given expression e to the given table.
    def qualified_expression(e, table)
      case e
      when Symbol
        t, column, aliaz = split_symbol(e)
        if t
          e
        elsif aliaz
          SQL::AliasedExpression.new(SQL::QualifiedIdentifier.new(table, SQL::Identifier.new(column)), aliaz)
        else
          SQL::QualifiedIdentifier.new(table, e)
        end
      when Array
        e.map{|a| qualified_expression(a, table)}
      when Hash
        h = {}
        e.each{|k,v| h[qualified_expression(k, table)] = qualified_expression(v, table)}
        h
      when SQL::Identifier
        SQL::QualifiedIdentifier.new(table, e)
      when SQL::OrderedExpression
        SQL::OrderedExpression.new(qualified_expression(e.expression, table), e.descending)
      when SQL::AliasedExpression
        SQL::AliasedExpression.new(qualified_expression(e.expression, table), e.aliaz)
      when SQL::CaseExpression
        SQL::CaseExpression.new(qualified_expression(e.conditions, table), qualified_expression(e.default, table), qualified_expression(e.expression, table))
      when SQL::Cast
        SQL::Cast.new(qualified_expression(e.expr, table), e.type)
      when SQL::Function
        SQL::Function.new(e.f, *qualified_expression(e.args, table))
      when SQL::ComplexExpression 
        SQL::ComplexExpression.new(e.op, *qualified_expression(e.args, table))
      when SQL::SQLArray 
        SQL::SQLArray.new(qualified_expression(e.array, table))
      when SQL::Subscript 
        SQL::Subscript.new(qualified_expression(e.f, table), qualified_expression(e.sub, table))
      when SQL::WindowFunction
        SQL::WindowFunction.new(qualified_expression(e.function, table), qualified_expression(e.window, table))
      when SQL::Window
        o = e.opts.dup
        o[:partition] = qualified_expression(o[:partition], table) if o[:partition]
        o[:order] = qualified_expression(o[:order], table) if o[:order]
        SQL::Window.new(o)
      when SQL::PlaceholderLiteralString
        args = if e.args.is_a?(Hash)
          h = {}
          e.args.each{|k,v| h[k] = qualified_expression(v, table)}
          h
        else
          qualified_expression(e.args, table)
        end
        SQL::PlaceholderLiteralString.new(e.str, args, e.parens)
      else
        e
      end
    end

    # The order of methods to call to build the SELECT SQL statement
    def select_clause_methods
      SELECT_CLAUSE_METHODS
    end

    # Modify the sql to add the columns selected
    def select_columns_sql(sql)
      sql << " #{column_list(@opts[:select])}"
    end

    # Modify the sql to add the DISTINCT modifier
    def select_distinct_sql(sql)
      if distinct = @opts[:distinct]
        sql << " DISTINCT#{" ON (#{expression_list(distinct)})" unless distinct.empty?}"
      end
    end

    # Modify the sql to add a dataset to the via an EXCEPT, INTERSECT, or UNION clause.
    # This uses a subselect for the compound datasets used, because using parantheses doesn't
    # work on all databases.  I consider this an ugly hack, but can't I think of a better default.
    def select_compounds_sql(sql)
      return unless @opts[:compounds]
      @opts[:compounds].each do |type, dataset, all|
        compound_sql = subselect_sql(dataset)
        sql << " #{type.to_s.upcase}#{' ALL' if all} #{compound_sql}"
      end
    end

    # Modify the sql to add the list of tables to select FROM
    def select_from_sql(sql)
      sql << " FROM #{source_list(@opts[:from])}" if @opts[:from]
    end
    alias delete_from_sql select_from_sql

    # Modify the sql to add the expressions to GROUP BY
    def select_group_sql(sql)
      sql << " GROUP BY #{expression_list(@opts[:group])}" if @opts[:group]
    end

    # Modify the sql to add the filter criteria in the HAVING clause
    def select_having_sql(sql)
      sql << " HAVING #{literal(@opts[:having])}" if @opts[:having]
    end

    # Modify the sql to add the list of tables to JOIN to
    def select_join_sql(sql)
      @opts[:join].each{|j| sql << literal(j)} if @opts[:join]
    end

    # Modify the sql to limit the number of rows returned and offset
    def select_limit_sql(sql)
      sql << " LIMIT #{@opts[:limit]}" if @opts[:limit]
      sql << " OFFSET #{@opts[:offset]}" if @opts[:offset]
    end

    # Modify the sql to add the expressions to ORDER BY
    def select_order_sql(sql)
      sql << " ORDER BY #{expression_list(@opts[:order])}" if @opts[:order]
    end
    alias delete_order_sql select_order_sql
    alias update_order_sql select_order_sql

    # Modify the sql to add the filter criteria in the WHERE clause
    def select_where_sql(sql)
      sql << " WHERE #{literal(@opts[:where])}" if @opts[:where]
    end
    alias delete_where_sql select_where_sql
    alias update_where_sql select_where_sql
    
    # SQL Fragment specifying the WITH clause
    def select_with_sql(sql)
      ws = opts[:with]
      return if !ws || ws.empty?
      sql.replace("#{select_with_sql_base}#{ws.map{|w| "#{quote_identifier(w[:name])}#{"(#{argument_list(w[:args])})" if w[:args]} AS #{literal_dataset(w[:dataset])}"}.join(COMMA_SEPARATOR)} #{sql}")
    end
    
    # The base keyword to use for the SQL WITH clause
    def select_with_sql_base
      SQL_WITH
    end

    # Converts an array of source names into into a comma separated list.
    def source_list(source)
      raise(Error, 'No source specified for query') if source.nil? || source.empty?
      source.map{|s| table_ref(s)}.join(COMMA_SEPARATOR)
    end
    
    # Splits the symbol into three parts.  Each part will
    # either be a string or nil.
    #
    # For columns, these parts are the table, column, and alias.
    # For tables, these parts are the schema, table, and alias.
    def split_symbol(sym)
      s = sym.to_s
      if m = COLUMN_REF_RE1.match(s)
        m[1..3]
      elsif m = COLUMN_REF_RE2.match(s)
        [nil, m[1], m[2]]
      elsif m = COLUMN_REF_RE3.match(s)
        [m[1], m[2], nil]
      else
        [nil, s, nil]
      end
    end

    # SQL to use if this dataset uses static SQL.  Since static SQL
    # can be a PlaceholderLiteralString in addition to a String,
    # we literalize nonstrings.
    def static_sql(sql)
      sql.is_a?(String) ? sql : literal(sql)
    end

    # SQL fragment for a subselect using the given database's SQL.
    def subselect_sql(ds)
      ds.sql
    end

    # SQL fragment specifying a table name.
    def table_ref(t)
      t.is_a?(String) ? quote_identifier(t) : literal(t)
    end
    
    # The order of methods to call to build the UPDATE SQL statement
    def update_clause_methods
      UPDATE_CLAUSE_METHODS
    end

    # SQL fragment specifying the tables from with to delete
    def update_table_sql(sql)
      sql << " #{source_list(@opts[:from])}"
    end

    # The SQL fragment specifying the columns and values to SET.
    def update_set_sql(sql)
      values = opts[:values]
      set = if values.is_a?(Hash)
        values = opts[:defaults].merge(values) if opts[:defaults]
        values = values.merge(opts[:overrides]) if opts[:overrides]
        # get values from hash
        values.map do |k, v|
          "#{[String, Symbol].any?{|c| k.is_a?(c)} ? quote_identifier(k) : literal(k)} = #{literal(v)}"
        end.join(COMMA_SEPARATOR)
      else
        # copy values verbatim
        values
      end
      sql << " SET #{set}"
    end
  end
end
