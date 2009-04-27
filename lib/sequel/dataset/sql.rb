module Sequel
  class Dataset
    AND_SEPARATOR = " AND ".freeze
    BOOL_FALSE = "'f'".freeze
    BOOL_TRUE = "'t'".freeze
    COLUMN_REF_RE1 = /\A([\w ]+)__([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE2 = /\A([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE3 = /\A([\w ]+)__([\w ]+)\z/.freeze
    COUNT_FROM_SELF_OPTS = [:distinct, :group, :sql, :limit, :compounds]
    IS_LITERALS = {nil=>'NULL'.freeze, true=>'TRUE'.freeze, false=>'FALSE'.freeze}.freeze
    IS_OPERATORS = ::Sequel::SQL::ComplexExpression::IS_OPERATORS
    N_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::N_ARITY_OPERATORS
    NULL = "NULL".freeze
    QUESTION_MARK = '?'.freeze
    STOCK_COUNT_OPTS = {:select => [SQL::AliasedExpression.new(LiteralString.new("COUNT(*)").freeze, :count)], :order => nil}.freeze
    SELECT_CLAUSE_ORDER = %w'distinct columns from join where group having compounds order limit'.freeze
    TWO_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::TWO_ARITY_OPERATORS
    WILDCARD = '*'.freeze

    # Adds an further filter to an existing filter using AND. If no filter 
    # exists an error is raised. This method is identical to #filter except
    # it expects an existing filter.
    #
    #   ds.filter(:a).and(:b) # SQL: WHERE a AND b
    def and(*cond, &block)
      raise(InvalidOperation, "No existing filter found.") unless @opts[:having] || @opts[:where]
      filter(*cond, &block)
    end

    # SQL fragment for the aliased expression
    def aliased_expression_sql(ae)
      as_sql(literal(ae.expression), ae.aliaz)
    end

    # SQL fragment for the SQL array.
    def array_sql(a)
      a.empty? ? '(NULL)' : "(#{expression_list(a)})"     
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
        v = IS_LITERALS[args.at(1)] || raise(Error, 'Invalid argument used for IS operator')
        "(#{literal(args.at(0))} #{op} #{v})"
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
        raise(Sequel::Error, "invalid operator #{op}")
      end
    end

    # Returns the number of records in the dataset.
    def count
      options_overlap(COUNT_FROM_SELF_OPTS) ? from_self.count : clone(STOCK_COUNT_OPTS).single_value.to_i
    end

    # Formats a DELETE statement using the given options and dataset options.
    # 
    #   dataset.filter{|o| o.price >= 100}.delete_sql #=>
    #     "DELETE FROM items WHERE (price >= 100)"
    def delete_sql
      opts = @opts

      return static_sql(opts[:sql]) if opts[:sql]

      if opts[:group]
        raise InvalidOperation, "Grouped datasets cannot be deleted from"
      elsif opts[:from].is_a?(Array) && opts[:from].size > 1
        raise InvalidOperation, "Joined datasets cannot be deleted from"
      end

      sql = "DELETE FROM #{source_list(opts[:from])}"

      if where = opts[:where]
        sql << " WHERE #{literal(where)}"
      end

      sql
    end

    # Returns a copy of the dataset with the SQL DISTINCT clause.
    # The DISTINCT clause is used to remove duplicate rows from the
    # output.  If arguments are provided, uses a DISTINCT ON clause,
    # in which case it will only be distinct on those columns, instead
    # of all returned columns.
    #
    #  dataset.distinct # SQL: SELECT DISTINCT * FROM items
    #  dataset.order(:id).distinct(:id) # SQL: SELECT DISTINCT ON (id) * FROM items ORDER BY id
    def distinct(*args)
      clone(:distinct => args)
    end

    # Adds an EXCEPT clause using a second dataset object. If all is true the
    # clause used is EXCEPT ALL, which may return duplicate rows.
    #
    #   DB[:items].except(DB[:other_items]).sql
    #   #=> "SELECT * FROM items EXCEPT SELECT * FROM other_items"
    def except(dataset, all = false)
      compound_clone(:except, dataset, all)
    end

    # Performs the inverse of Dataset#filter.
    #
    #   dataset.exclude(:category => 'software').sql #=>
    #     "SELECT * FROM items WHERE (category != 'software')"
    def exclude(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      cond = cond.first if cond.size == 1
      cond = filter_expr(cond, &block)
      cond = SQL::BooleanExpression.invert(cond)
      cond = SQL::BooleanExpression.new(:AND, @opts[clause], cond) if @opts[clause]
      clone(clause => cond)
    end

    # Returns an EXISTS clause for the dataset as a LiteralString.
    #
    #   DB.select(1).where(DB[:items].exists).sql
    #   #=> "SELECT 1 WHERE EXISTS (SELECT * FROM items)"
    def exists
      LiteralString.new("EXISTS (#{select_sql})")
    end

    # Returns a copy of the dataset with the given conditions imposed upon it.  
    # If the query already has a HAVING clause, then the conditions are imposed in the 
    # HAVING clause. If not, then they are imposed in the WHERE clause.
    # 
    # filter accepts the following argument types:
    #
    # * Hash - list of equality/inclusion expressions
    # * Array - depends:
    #   * If first member is a string, assumes the rest of the arguments
    #     are parameters and interpolates them into the string.
    #   * If all members are arrays of length two, treats the same way
    #     as a hash, except it allows for duplicate keys to be
    #     specified.
    # * String - taken literally
    # * Symbol - taken as a boolean column argument (e.g. WHERE active)
    # * Sequel::SQL::BooleanExpression - an existing condition expression,
    #   probably created using the Sequel expression filter DSL.
    #
    # filter also takes a block, which should return one of the above argument
    # types, and is treated the same way.  This block yields a virtual row object,
    # which is easy to use to create identifiers and functions.
    #
    # If both a block and regular argument
    # are provided, they get ANDed together.
    #
    # Examples:
    #
    #   dataset.filter(:id => 3).sql #=>
    #     "SELECT * FROM items WHERE (id = 3)"
    #   dataset.filter('price < ?', 100).sql #=>
    #     "SELECT * FROM items WHERE price < 100"
    #   dataset.filter([[:id, (1,2,3)], [:id, 0..10]]).sql #=>
    #     "SELECT * FROM items WHERE ((id IN (1, 2, 3)) AND ((id >= 0) AND (id <= 10)))"
    #   dataset.filter('price < 100').sql #=>
    #     "SELECT * FROM items WHERE price < 100"
    #   dataset.filter(:active).sql #=>
    #     "SELECT * FROM items WHERE :active
    #   dataset.filter{|o| o.price < 100}.sql #=>
    #     "SELECT * FROM items WHERE (price < 100)"
    # 
    # Multiple filter calls can be chained for scoping:
    #
    #   software = dataset.filter(:category => 'software')
    #   software.filter{|o| o.price < 100}.sql #=>
    #     "SELECT * FROM items WHERE ((category = 'software') AND (price < 100))"
    #
    # See doc/dataset_filters.rdoc for more examples and details.
    def filter(*cond, &block)
      _filter(@opts[:having] ? :having : :where, *cond, &block)
    end

    # The first source (primary table) for this dataset.  If the dataset doesn't
    # have a table, raises an error.  If the table is aliased, returns the aliased name.
    def first_source
      source = @opts[:from]
      if source.nil? || source.empty?
        raise Error, 'No source specified for query'
      end
      case s = source.first
      when Hash
        s.values.first
      when Symbol
        sch, table, aliaz = split_symbol(s)
        aliaz ? aliaz.to_sym : s
      else
        s
      end
    end

    # Returns a copy of the dataset with the source changed.
    #
    #   dataset.from # SQL: SELECT *
    #   dataset.from(:blah) # SQL: SELECT * FROM blah
    #   dataset.from(:blah, :foo) # SQL: SELECT * FROM blah, foo
    def from(*source)
      clone(:from=>source.empty? ? nil : source)
    end
    
    # Returns a dataset selecting from the current dataset.
    #
    #   ds = DB[:items].order(:name)
    #   ds.sql #=> "SELECT * FROM items ORDER BY name"
    #   ds.from_self.sql #=> "SELECT * FROM (SELECT * FROM items ORDER BY name)"
    def from_self
      fs = {}
      @opts.keys.each{|k| fs[k] = nil} 
      fs[:from] = [self]
      clone(fs)
    end

    # SQL fragment specifying an SQL function call
    def function_sql(f)
      args = f.args
      "#{f.f}#{args.empty? ? '()' : literal(args)}"
    end

    # Pattern match any of the columns to any of the terms.  The terms can be
    # strings (which use LIKE) or regular expressions (which are only supported
    # in some databases).  See Sequel::SQL::StringExpression.like.  Note that the
    # total number of pattern matches will be cols.length * terms.length,
    # which could cause performance issues.
    #
    #   dataset.grep(:a, '%test%') # SQL: SELECT * FROM items WHERE a LIKE '%test%'
    #   dataset.grep([:a, :b], %w'%test% foo') # SQL: SELECT * FROM items WHERE a LIKE '%test%' OR a LIKE 'foo' OR b LIKE '%test%' OR b LIKE 'foo' 
    def grep(cols, terms)
      filter(SQL::BooleanExpression.new(:OR, *Array(cols).collect{|c| SQL::StringExpression.like(c, *terms)}))
    end

    # Returns a copy of the dataset with the results grouped by the value of 
    # the given columns.
    #
    #   dataset.group(:id) # SELECT * FROM items GROUP BY id
    #   dataset.group(:id, :name) # SELECT * FROM items GROUP BY id, name
    def group(*columns)
      clone(:group => columns)
    end
    alias group_by group

    # Returns a copy of the dataset with the HAVING conditions changed. Raises 
    # an error if the dataset has not been grouped. See #filter for argument types.
    #
    #   dataset.group(:sum).having(:sum=>10) # SQL: SELECT * FROM items GROUP BY sum HAVING sum = 10 
    def having(*cond, &block)
      raise(InvalidOperation, "Can only specify a HAVING clause on a grouped dataset") unless @opts[:group]
      _filter(:having, *cond, &block)
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

    # Formats an INSERT statement using the given values. If a hash is given,
    # the resulting statement includes column names. If no values are given, 
    # the resulting statement includes a DEFAULT VALUES clause.
    #
    #   dataset.insert_sql #=> 'INSERT INTO items DEFAULT VALUES'
    #   dataset.insert_sql(1,2,3) #=> 'INSERT INTO items VALUES (1, 2, 3)'
    #   dataset.insert_sql(:a => 1, :b => 2) #=>
    #     'INSERT INTO items (a, b) VALUES (1, 2)'
    def insert_sql(*values)
      return static_sql(@opts[:sql]) if @opts[:sql]

      from = source_list(@opts[:from])
      case values.size
      when 0
        values = {}
      when 1
        vals = values.at(0)
        if [Hash, Dataset, Array].any?{|c| vals.is_a?(c)}
          values = vals
        elsif vals.respond_to?(:values)
          values = vals.values
        end
      end

      case values
      when Array
        if values.empty?
          insert_default_values_sql
        else
          "INSERT INTO #{from} VALUES #{literal(values)}"
        end
      when Hash
        values = @opts[:defaults].merge(values) if @opts[:defaults]
        values = values.merge(@opts[:overrides]) if @opts[:overrides]
        if values.empty?
          insert_default_values_sql
        else
          fl, vl = [], []
          values.each do |k, v|
            fl << literal(String === k ? k.to_sym : k)
            vl << literal(v)
          end
          "INSERT INTO #{from} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
        end
      when Dataset
        "INSERT INTO #{from} #{literal(values)}"
      end
    end
    
    # Adds an INTERSECT clause using a second dataset object. If all is true 
    # the clause used is INTERSECT ALL, which may return duplicate rows.
    #
    #   DB[:items].intersect(DB[:other_items]).sql
    #   #=> "SELECT * FROM items INTERSECT SELECT * FROM other_items"
    def intersect(dataset, all = false)
      compound_clone(:intersect, dataset, all)
    end

    # Inverts the current filter
    #
    #   dataset.filter(:category => 'software').invert.sql #=>
    #     "SELECT * FROM items WHERE (category != 'software')"
    def invert
      having, where = @opts[:having], @opts[:where]
      raise(Error, "No current filter") unless having || where
      o = {}
      o[:having] = SQL::BooleanExpression.invert(having) if having
      o[:where] = SQL::BooleanExpression.invert(where) if where
      clone(o)
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
    #   * :implicit_qualifer - The name to use for qualifying implicit conditions.  By default,
    #     the last joined or primary table is used.
    # * block - The block argument should only be given if a JOIN with an ON clause is used,
    #   in which case it yields the table alias/name for the table currently being joined,
    #   the table alias/name for the last joined (or first table), and an array of previous
    #   SQL::JoinClause.
    def join_table(type, table, expr=nil, options={}, &block)
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
          table_alias = "t#{table_alias_num}"
        end
        table_name = table_alias
      else
        table = table.table_name if table.respond_to?(:table_name)
        table_name = table_alias || table
      end

      join = if expr.nil? and !block_given?
        SQL::JoinClause.new(type, table, table_alias)
      elsif Array === expr and !expr.empty? and expr.all?{|x| Symbol === x}
        raise(Sequel::Error, "can't use a block if providing an array of symbols as expr") if block_given?
        SQL::JoinUsingClause.new(expr, type, table, table_alias)
      else
        last_alias ||= @opts[:last_joined_table] || (first_source.is_a?(Dataset) ? 't1' : first_source)
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

    # If given an integer, the dataset will contain only the first l results.
    # If given a range, it will contain only those at offsets within that
    # range. If a second argument is given, it is used as an offset.
    #
    #   dataset.limit(10) # SQL: SELECT * FROM items LIMIT 10
    #   dataset.limit(10, 20) # SQL: SELECT * FROM items LIMIT 10 OFFSET 20
    def limit(l, o = nil)
      return from_self.limit(l, o) if @opts[:sql]

      if Range === l
        o = l.first
        l = l.last - l.first + (l.exclude_end? ? 0 : 1)
      end
      l = l.to_i
      raise(Error, 'Limits must be greater than or equal to 1') unless l >= 1
      opts = {:limit => l}
      if o
        o = o.to_i
        raise(Error, 'Offsets must be greater than or equal to 0') unless o >= 0
        opts[:offset] = o
      end
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
      s = "INSERT INTO #{source_list(@opts[:from])} (#{identifier_list(columns)}) VALUES "
      values.map{|r| s + literal(r)}
    end
    
    # Adds an alternate filter to an existing filter using OR. If no filter 
    # exists an error is raised.
    #
    #   dataset.filter(:a).or(:b) # SQL: SELECT * FROM items WHERE a OR b
    def or(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      raise(InvalidOperation, "No existing filter found.") unless @opts[clause]
      cond = cond.first if cond.size == 1
      clone(clause => SQL::BooleanExpression.new(:OR, @opts[clause], filter_expr(cond, &block)))
    end

    # Returns a copy of the dataset with the order changed. If a nil is given
    # the returned dataset has no order. This can accept multiple arguments
    # of varying kinds, and even SQL functions.  If a block is given, it is treated
    # as a virtual row block, similar to filter.
    #
    #   ds.order(:name).sql #=> 'SELECT * FROM items ORDER BY name'
    #   ds.order(:a, :b).sql #=> 'SELECT * FROM items ORDER BY a, b'
    #   ds.order('a + b'.lit).sql #=> 'SELECT * FROM items ORDER BY a + b'
    #   ds.order(:a + :b).sql #=> 'SELECT * FROM items ORDER BY (a + b)'
    #   ds.order(:name.desc).sql #=> 'SELECT * FROM items ORDER BY name DESC'
    #   ds.order(:name.asc).sql #=> 'SELECT * FROM items ORDER BY name ASC'
    #   ds.order{|o| o.sum(:name)}.sql #=> 'SELECT * FROM items ORDER BY sum(name)'
    #   ds.order(nil).sql #=> 'SELECT * FROM items'
    def order(*columns, &block)
      columns += Array(virtual_row_block_call(block)) if block
      clone(:order => (columns.compact.empty?) ? nil : columns)
    end
    alias_method :order_by, :order
    
    # Returns a copy of the dataset with the order columns added
    # to the existing order.
    #
    #   ds.order(:a).order(:b).sql #=> 'SELECT * FROM items ORDER BY b'
    #   ds.order(:a).order_more(:b).sql #=> 'SELECT * FROM items ORDER BY a, b'
    def order_more(*columns, &block)
      order(*Array(@opts[:order]).concat(columns), &block)
    end
    
    # SQL fragment for the ordered expression, used in the ORDER BY
    # clause.
    def ordered_expression_sql(oe)
      "#{literal(oe.expression)} #{oe.descending ? 'DESC' : 'ASC'}"
    end

    # SQL fragment for a literal string with placeholders
    def placeholder_literal_string_sql(pls)
      args = pls.args.dup
      s = pls.str.gsub(QUESTION_MARK){literal(args.shift)}
      s = "(#{s})" if pls.parens
      s
    end

    # SQL fragment for the qualifed identifier, specifying
    # a table and a column (or schema and table).
    def qualified_identifier_sql(qcr)
      [qcr.table, qcr.column].map{|x| [SQL::QualifiedIdentifier, SQL::Identifier, Symbol].any?{|c| x.is_a?(c)} ? literal(x) : quote_identifier(x)}.join('.')
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

    # Returns a copy of the dataset with the order reversed. If no order is
    # given, the existing order is inverted.
    def reverse_order(*order)
      order(*invert_order(order.empty? ? @opts[:order] : order))
    end
    alias reverse reverse_order

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

    # Returns a copy of the dataset with the columns selected changed
    # to the given columns. This also takes a virtual row block,
    # similar to filter.
    #
    #   dataset.select(:a) # SELECT a FROM items
    #   dataset.select(:a, :b) # SELECT a, b FROM items
    #   dataset.select{|o| o.a, o.sum(:b)} # SELECT a, sum(b) FROM items
    def select(*columns, &block)
      columns += Array(virtual_row_block_call(block)) if block
      clone(:select => columns)
    end
    
    # Returns a copy of the dataset selecting the wildcard.
    #
    #   dataset.select(:a).select_all # SELECT * FROM items
    def select_all
      clone(:select => nil)
    end

    # Returns a copy of the dataset with the given columns added
    # to the existing selected columns.
    #
    #   dataset.select(:a).select(:b) # SELECT b FROM items
    #   dataset.select(:a).select_more(:b) # SELECT a, b FROM items
    def select_more(*columns, &block)
      select(*Array(@opts[:select]).concat(columns), &block)
    end
    
    # Formats a SELECT statement
    #
    #   dataset.select_sql # => "SELECT * FROM items"
    def select_sql
      opts = @opts
      return static_sql(opts[:sql]) if opts[:sql]
      sql = 'SELECT'
      select_clause_order.each{|x| send(:"select_#{x}_sql", sql, opts)}
      sql
    end

    # Same as select_sql, not aliased directly to make subclassing simpler.
    def sql
      select_sql
    end

    # SQL fragment for specifying subscripts (SQL arrays)
    def subscript_sql(s)
      "#{literal(s.f)}[#{s.sub.join(COMMA_SEPARATOR)}]"
    end

    # Returns a copy of the dataset with no filters (HAVING or WHERE clause) applied.
    # 
    #   dataset.group(:a).having(:a=>1).where(:b).unfiltered # SELECT * FROM items
    def unfiltered
      clone(:where => nil, :having => nil)
    end

    # Adds a UNION clause using a second dataset object. If all is true the
    # clause used is UNION ALL, which may return duplicate rows.
    #
    #   DB[:items].union(DB[:other_items]).sql
    #   #=> "SELECT * FROM items UNION SELECT * FROM other_items"
    def union(dataset, all = false)
      compound_clone(:union, dataset, all)
    end

    # Returns a copy of the dataset with no order.
    # 
    #   dataset.order(:a).unordered # SELECT * FROM items
    def unordered
      order(nil)
    end

    # Formats an UPDATE statement using the given values.
    #
    #   dataset.update_sql(:price => 100, :category => 'software') #=>
    #     "UPDATE items SET price = 100, category = 'software'"
    #
    # Raises an error if the dataset is grouped or includes more
    # than one table.
    def update_sql(values = {})
      opts = @opts

      return static_sql(opts[:sql]) if opts[:sql]

      if opts[:group]
        raise InvalidOperation, "A grouped dataset cannot be updated"
      elsif (opts[:from].size > 1) or opts[:join]
        raise InvalidOperation, "A joined dataset cannot be updated"
      end
      
      sql = "UPDATE #{source_list(@opts[:from])} SET "
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
      sql << set
      if where = opts[:where]
        sql << " WHERE #{literal(where)}"
      end

      sql
    end

    # Add a condition to the WHERE clause.  See #filter for argument types.
    #
    #   dataset.group(:a).having(:a).filter(:b) # SELECT * FROM items GROUP BY a HAVING a AND b
    #   dataset.group(:a).having(:a).where(:b) # SELECT * FROM items WHERE b GROUP BY a HAVING a
    def where(*cond, &block)
      _filter(:where, *cond, &block)
    end

    # Returns a copy of the dataset with the static SQL used.  This is useful if you want
    # to keep the same row_proc/graph, but change the SQL used to custom SQL.
    #
    #   dataset.with_sql('SELECT * FROM foo') # SELECT * FROM foo
    def with_sql(sql, *args)
      sql = SQL::PlaceholderLiteralString.new(sql, args) unless args.empty?
      clone(:sql=>sql)
    end

    [:inner, :full_outer, :right_outer, :left_outer].each do |jtype|
      class_eval("def #{jtype}_join(*args, &block); join_table(:#{jtype}, *args, &block) end")
    end
    alias join inner_join

    protected

    # Returns a table reference for use in the FROM clause.  Returns an SQL subquery
    # frgament with an optional table alias.
    def to_table_reference(table_alias=nil)
      s = "(#{sql})"
      table_alias ? as_sql(s, table_alias) : s
    end

    private

    # Internal filter method so it works on either the having or where clauses.
    def _filter(clause, *cond, &block)
      cond = cond.first if cond.size == 1
      cond = filter_expr(cond, &block)
      cond = SQL::BooleanExpression.new(:AND, @opts[clause], cond) if @opts[clause]
      clone(clause => cond)
    end

    # SQL fragment for specifying an alias.  expression should already be literalized.
    def as_sql(expression, aliaz)
      "#{expression} AS #{quote_identifier(aliaz)}"
    end

    # Converts an array of column names into a comma seperated string of 
    # column names. If the array is empty, a wildcard (*) is returned.
    def column_list(columns)
      if columns.nil? || columns.empty?
        WILDCARD
      else
        m = columns.map do |i|
          i.is_a?(Hash) ? i.map{|k, v| as_sql(literal(k), v)} : literal(i)
        end
        m.join(COMMA_SEPARATOR)
      end
    end
    
    # Add the dataset to the list of compounds
    def compound_clone(type, dataset, all)
      clone(:compounds=>Array(@opts[:compounds]).map{|x| x.dup} + [[type, dataset, all]])
    end
    
    # Converts an array of expressions into a comma separated string of
    # expressions.
    def expression_list(columns)
      columns.map{|i| literal(i)}.join(COMMA_SEPARATOR)
    end
    
    # SQL fragment based on the expr type.  See #filter.
    def filter_expr(expr = nil, &block)
      expr = nil if expr == []
      if expr && block
        return SQL::BooleanExpression.new(:AND, filter_expr(expr), filter_expr(block))
      elsif block
        expr = block
      end
      case expr
      when Hash
        SQL::BooleanExpression.from_value_pairs(expr)
      when Array
        if String === expr[0]
          SQL::PlaceholderLiteralString.new(expr.shift, expr, true)
        elsif Sequel.condition_specifier?(expr)
          SQL::BooleanExpression.from_value_pairs(expr)
        else
          SQL::BooleanExpression.new(:AND, *expr.map{|x| filter_expr(x)})
        end
      when Proc
        filter_expr(virtual_row_block_call(expr))
      when SQL::NumericExpression, SQL::StringExpression
        raise(Error, "Invalid SQL Expression type: #{expr.inspect}") 
      when Symbol, SQL::Expression
        expr
      when TrueClass, FalseClass
        SQL::BooleanExpression.new(:NOOP, expr)
      when String
        LiteralString.new("(#{expr})")
      else
        raise(Error, 'Invalid filter argument')
      end
    end

    # SQL fragment specifying a list of identifiers
    def identifier_list(columns)
      columns.map{|i| quote_identifier(i)}.join(COMMA_SEPARATOR)
    end

    # SQL statement for formatting an insert statement with default values
    def insert_default_values_sql
      "INSERT INTO #{source_list(@opts[:from])} DEFAULT VALUES"
    end

    # Inverts the given order by breaking it into a list of column references
    # and inverting them.
    #
    #   dataset.invert_order([:id.desc]]) #=> [:id]
    #   dataset.invert_order(:category, :price.desc]) #=>
    #     [:category.desc, :price]
    def invert_order(order)
      return nil unless order
      new_order = []
      order.map do |f|
        case f
        when SQL::OrderedExpression
          SQL::OrderedExpression.new(f.expression, !f.descending)
        else
          SQL::OrderedExpression.new(f)
        end
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
      "'#{v}'"
    end

    # SQL fragment for DateTime, using the ISO8601 format.
    def literal_datetime(v)
      "'#{v}'"
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

    # SQL fragmento for a type of object not handled by Dataset#literal.  Raises an error.  If a database specific type is allowed, this should be overriden in a subclass.
    def literal_other(v)
      raise Error, "can't express #{v.inspect} as a SQL literal"
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

    # SQL fragment for Time, uses the ISO8601 format.
    def literal_time(v)
      "'#{v.iso8601}'"
    end

    # SQL fragment for true.
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

    # The order of methods to call to build the SELECT SQL statement
    def select_clause_order
      SELECT_CLAUSE_ORDER
    end

    # Modify the sql to add the columns selected
    def select_columns_sql(sql, opts)
      sql << " #{column_list(opts[:select])}"
    end

    # Modify the sql to add the DISTINCT modifier
    def select_distinct_sql(sql, opts)
      if distinct = opts[:distinct]
        sql << " DISTINCT#{" ON (#{expression_list(distinct)})" unless distinct.empty?}"
      end
    end

    # Modify the sql to add a dataset to the via an EXCEPT, INTERSECT, or UNION clause.
    # This uses a subselect for the compound datasets used, because using parantheses doesn't
    # work on all databases.  I consider this an ugly hack, but can't I think of a better default.
    def select_compounds_sql(sql, opts)
      return unless opts[:compounds]
      opts[:compounds].each do |type, dataset, all|
        compound_sql = subselect_sql(dataset)
        compound_sql = "SELECT * FROM (#{compound_sql})" if dataset.opts[:compounds]
        sql.replace("#{sql} #{type.to_s.upcase}#{' ALL' if all} #{compound_sql}")
      end
    end

    # Modify the sql to add the list of tables to select FROM
    def select_from_sql(sql, opts)
      sql << " FROM #{source_list(opts[:from])}" if opts[:from]
    end

    # Modify the sql to add the expressions to GROUP BY
    def select_group_sql(sql, opts)
      sql << " GROUP BY #{expression_list(opts[:group])}" if opts[:group]
    end

    # Modify the sql to add the filter criteria in the HAVING clause
    def select_having_sql(sql, opts)
      sql << " HAVING #{literal(opts[:having])}" if opts[:having]
    end

    # Modify the sql to add the list of tables to JOIN to
    def select_join_sql(sql, opts)
      opts[:join].each{|j| sql << literal(j)} if opts[:join]
    end

    # Modify the sql to limit the number of rows returned and offset
    def select_limit_sql(sql, opts)
      sql << " LIMIT #{opts[:limit]}" if opts[:limit]
      sql << " OFFSET #{opts[:offset]}" if opts[:offset]
    end

    # Modify the sql to add the expressions to ORDER BY
    def select_order_sql(sql, opts)
      sql << " ORDER BY #{expression_list(opts[:order])}" if opts[:order]
    end

    # Modify the sql to add the filter criteria in the WHERE clause
    def select_where_sql(sql, opts)
      sql << " WHERE #{literal(opts[:where])}" if opts[:where]
    end

    # Converts an array of source names into into a comma separated list.
    def source_list(source)
      if source.nil? || source.empty?
        raise Error, 'No source specified for query'
      end
      auto_alias_count = @opts[:num_dataset_sources] || 0
      m = source.map do |s|
        case s
        when Dataset
          auto_alias_count += 1
          s.to_table_reference("t#{auto_alias_count}")
        else
          table_ref(s)
        end
      end
      m.join(COMMA_SEPARATOR)
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
      case t
      when Symbol
        literal_symbol(t)
      when Dataset
        t.to_table_reference
      when Hash
        t.map{|k, v| as_sql(table_ref(k), v)}.join(COMMA_SEPARATOR)
      when String
        quote_identifier(t)
      else
        literal(t)
      end
    end
  end
end
