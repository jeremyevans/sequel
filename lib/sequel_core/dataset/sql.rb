module Sequel
  class Dataset
    AND_SEPARATOR = " AND ".freeze
    BOOL_FALSE = "'f'".freeze
    BOOL_TRUE = "'t'".freeze
    COLUMN_REF_RE1 = /\A([\w ]+)__([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE2 = /\A([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE3 = /\A([\w ]+)__([\w ]+)\z/.freeze
    COUNT_FROM_SELF_OPTS = [:distinct, :group, :sql, :limit]
    DATE_FORMAT = "DATE '%Y-%m-%d'".freeze
    N_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::N_ARITY_OPERATORS
    NULL = "NULL".freeze
    QUESTION_MARK = '?'.freeze
    STOCK_COUNT_OPTS = {:select => ["COUNT(*)".lit], :order => nil}.freeze
    TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S'".freeze
    TWO_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::TWO_ARITY_OPERATORS
    WILDCARD = '*'.freeze

    # Adds an further filter to an existing filter using AND. If no filter 
    # exists an error is raised. This method is identical to #filter except
    # it expects an existing filter.
    def and(*cond, &block)
      raise(Error::NoExistingFilter, "No existing filter found.") unless @opts[:having] || @opts[:where]
      filter(*cond, &block)
    end

    # SQL fragment for the aliased expression
    def aliased_expression_sql(ae)
      as_sql(literal(ae.expression), ae.aliaz)
    end

    # SQL fragment for specifying given CaseExpression.
    def case_expression_sql(ce)
      "(CASE #{ce.conditions.collect{|c,r| "WHEN #{literal(c)} THEN #{literal(r)} "}.join}ELSE #{literal(ce.default)} END)"
    end

    # SQL fragment for specifying all columns in a given table.
    def column_all_sql(ca)
      "#{quote_identifier(ca.table)}.*"
    end

    # SQL fragment for complex expressions
    def complex_expression_sql(op, args)
      case op
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
      options_overlap(COUNT_FROM_SELF_OPTS) ? from_self.count : single_value(STOCK_COUNT_OPTS).to_i
    end
    alias_method :size, :count

    # Formats a DELETE statement using the given options and dataset options.
    # 
    #   dataset.filter(:price >= 100).delete_sql #=>
    #     "DELETE FROM items WHERE (price >= 100)"
    def delete_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      if sql = opts[:sql]
        return sql
      end

      if opts[:group]
        raise Error::InvalidOperation, "Grouped datasets cannot be deleted from"
      elsif opts[:from].is_a?(Array) && opts[:from].size > 1
        raise Error::InvalidOperation, "Joined datasets cannot be deleted from"
      end

      sql = "DELETE FROM #{source_list(opts[:from])}"

      if where = opts[:where]
        sql << " WHERE #{literal(where)}"
      end

      sql
    end

    # Adds an EXCEPT clause using a second dataset object. If all is true the
    # clause used is EXCEPT ALL, which may return duplicate rows.
    #
    #   DB[:items].except(DB[:other_items]).sql
    #   #=> "SELECT * FROM items EXCEPT SELECT * FROM other_items"
    def except(dataset, all = false)
      clone(:except => dataset, :except_all => all)
    end

    # Performs the inverse of Dataset#filter.
    #
    #   dataset.exclude(:category => 'software').sql #=>
    #     "SELECT * FROM items WHERE (category != 'software')"
    def exclude(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      cond = cond.first if cond.size == 1
      cond = cond.sql_or if (Hash === cond) || ((Array === cond) && (cond.all_two_pairs?))
      cond = filter_expr(cond, &block)
      cond = SQL::BooleanExpression.invert(cond)
      cond = SQL::BooleanExpression.new(:AND, @opts[clause], cond) if @opts[clause]
      clone(clause => cond)
    end

    # Returns an EXISTS clause for the dataset.
    #
    #   DB.select(1).where(DB[:items].exists).sql
    #   #=> "SELECT 1 WHERE EXISTS (SELECT * FROM items)"
    def exists(opts = nil)
      "EXISTS (#{select_sql(opts)})"
    end

    # Returns a copy of the dataset with the given conditions imposed upon it.  
    # If the query has been grouped, then the conditions are imposed in the 
    # HAVING clause. If not, then they are imposed in the WHERE clause. Filter
    # 
    # filter accepts the following argument types:
    #
    # * Hash - list of equality expressions
    # * Array - depends:
    #   * If first member is a string, assumes the rest of the arguments
    #     are parameters and interpolates them into the string.
    #   * If all members are arrays of length two, treats the same way
    #     as a hash, except it allows for duplicate keys to be
    #     specified.
    # * String - taken literally
    # * Symbol - taken as a boolean column argument (e.g. WHERE active)
    # * Sequel::SQL::BooleanExpression - an existing condition expression,
    #   probably created using the Sequel blockless filter DSL.
    #
    # filter also takes a block, which should return one of the above argument
    # types, and is treated the same way. If both a block and regular argument
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
    #   dataset.filter(:price < 100).sql #=>
    #     "SELECT * FROM items WHERE (price < 100)"
    # 
    # Multiple filter calls can be chained for scoping:
    #
    #   software = dataset.filter(:category => 'software')
    #   software.filter(price < 100).sql #=>
    #     "SELECT * FROM items WHERE ((category = 'software') AND (price < 100))"
    #
    # See doc/dataset_filters.rdoc for more examples and details.
    def filter(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      cond = cond.first if cond.size == 1
      cond = transform_save(cond) if @transform if cond.is_a?(Hash)
      cond = filter_expr(cond, &block)
      cond = SQL::BooleanExpression.new(:AND, @opts[clause], cond) if @opts[clause] && !@opts[clause].blank?
      clone(clause => cond)
    end
    alias_method :where, :filter

    # The first source (primary table) for this dataset.  If the dataset doesn't
    # have a table, raises an error.  If the table is aliased, returns the actual
    # table name, not the alias.
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
    def from(*source)
      clone(:from => source)
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
    def grep(cols, terms)
      filter(SQL::BooleanExpression.new(:OR, *Array(cols).collect{|c| SQL::StringExpression.like(c, *terms)}))
    end

    # Returns a copy of the dataset with the results grouped by the value of 
    # the given columns
    def group(*columns)
      clone(:group => columns)
    end
    alias_method :group_by, :group

    # Returns a copy of the dataset with the having conditions changed. Raises 
    # an error if the dataset has not been grouped. See also #filter.
    def having(*cond, &block)
      raise(Error::InvalidOperation, "Can only specify a HAVING clause on a grouped dataset") unless @opts[:group]
      clone(:having=>{}).filter(*cond, &block)
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
    #   dataset.insert_sql() #=> 'INSERT INTO items DEFAULT VALUES'
    #   dataset.insert_sql(1,2,3) #=> 'INSERT INTO items VALUES (1, 2, 3)'
    #   dataset.insert_sql(:a => 1, :b => 2) #=>
    #     'INSERT INTO items (a, b) VALUES (1, 2)'
    def insert_sql(*values)
      if sql = @opts[:sql]
        return sql
      end

      from = source_list(@opts[:from])
      case values.size
      when 0
        values = {}
      when 1
        vals = values.at(0)
        if vals.is_one_of?(Hash, Dataset, Array)
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
        values = transform_save(values) if @transform
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
      clone(:intersect => dataset, :intersect_all => all)
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

    # SQL fragment specifying an Irregular (cast/extract) SQL function call
    def irregular_function_sql(f)
      "#{f.f}(#{literal(f.arg1)} #{f.joiner} #{literal(f.arg2)})"
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
    # * type - The type of join to do (:inner, :left_outer, :right_outer, :full)
    # * table - Depends on type:
    #   * Dataset - a subselect is performed with an alias of tN for some value of N
    #   * Model (or anything responding to :table_name) - table.table_name
    #   * String, Symbol: table
    # * expr - specifies conditions, depends on type:
    #   * Hash, Array with all two pairs - Assumes key (1st arg) is column of joined table (unless already
    #     qualified), and value (2nd arg) is column of the last joined or primary table.
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
    # * table_alias - the name of the table's alias when joining, necessary for joining
    #   to the same table more than once.  No alias is used by default.
    # * block - The block argument should only be given if a JOIN with an ON clause is used,
    #   in which case it yields the table alias/name for the table currently being joined,
    #   the table alias/name for the last joined (or first table), and an array of previous
    #   SQL::JoinClause.
    def join_table(type, table, expr=nil, table_alias=nil, &block)
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
        last_alias = @opts[:last_joined_table] || (first_source.is_a?(Dataset) ? 't1' : first_source)
        if Hash === expr or (Array === expr and expr.all_two_pairs?)
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
    def limit(l, o = nil)
      return from_self.limit(l, o) if @opts[:sql]

      if Range === l
        o = l.first
        l = l.interval + 1
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
      when LiteralString
        v
      when String
        "'#{v.gsub(/\\/, "\\\\\\\\").gsub(/'/, "''")}'"
      when Integer, Float
        v.to_s
      when BigDecimal
        v.to_s("F")
      when NilClass
        NULL
      when TrueClass
        BOOL_TRUE
      when FalseClass
        BOOL_FALSE
      when Symbol
        symbol_to_column_ref(v)
      when ::Sequel::SQL::Expression
        v.to_s(self)
      when Array
        v.all_two_pairs? ? literal(v.sql_expr) : (v.empty? ? '(NULL)' : "(#{expression_list(v)})")
      when Hash
        literal(v.sql_expr)
      when Time, DateTime
        v.strftime(TIMESTAMP_FORMAT)
      when Date
        v.strftime(DATE_FORMAT)
      when Dataset
        "(#{v.sql})"
      else
        raise Error, "can't express #{v.inspect} as a SQL literal"
      end
    end

    # Returns an array of insert statements for inserting multiple records.
    # This method is used by #multi_insert to format insert statements and
    # expects a keys array and and an array of value arrays.
    #
    # This method should be overridden by descendants if the support
    # inserting multiple records in a single SQL statement.
    def multi_insert_sql(columns, values)
      table = quote_identifier(@opts[:from].first)
      columns = identifier_list(columns)
      values.map do |r|
        "INSERT INTO #{table} (#{columns}) VALUES #{literal(r)}"
      end
    end
    
    # Adds an alternate filter to an existing filter using OR. If no filter 
    # exists an error is raised.
    def or(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      cond = cond.first if cond.size == 1
      if @opts[clause]
        clone(clause => SQL::BooleanExpression.new(:OR, @opts[clause], filter_expr(cond, &block)))
      else
        raise Error::NoExistingFilter, "No existing filter found."
      end
    end

    # Returns a copy of the dataset with the order changed. If a nil is given
    # the returned dataset has no order. This can accept multiple arguments
    # of varying kinds, and even SQL functions.
    #
    #   ds.order(:name).sql #=> 'SELECT * FROM items ORDER BY name'
    #   ds.order(:a, :b).sql #=> 'SELECT * FROM items ORDER BY a, b'
    #   ds.order('a + b'.lit).sql #=> 'SELECT * FROM items ORDER BY a + b'
    #   ds.order(:a + :b).sql #=> 'SELECT * FROM items ORDER BY (a + b)'
    #   ds.order(:name.desc).sql #=> 'SELECT * FROM items ORDER BY name DESC'
    #   ds.order(:name.asc).sql #=> 'SELECT * FROM items ORDER BY name ASC'
    #   ds.order(:arr|1).sql #=> 'SELECT * FROM items ORDER BY arr[1]'
    #   ds.order(nil).sql #=> 'SELECT * FROM items'
    def order(*order)
      clone(:order => (order.compact.empty?) ? nil : order)
    end
    alias_method :order_by, :order
    
    # Returns a copy of the dataset with the order columns added
    # to the existing order.
    def order_more(*order)
      order(*((@opts[:order] || []) + order))
    end
    
    # SQL fragment for the ordered expression, used in the ORDER BY
    # clause.
    def ordered_expression_sql(oe)
      "#{literal(oe.expression)} #{oe.descending ? 'DESC' : 'ASC'}"
    end

    # SQL fragment for the qualifed identifier, specifying
    # a table and a column (or schema and table).
    def qualified_identifier_sql(qcr)
      [qcr.table, qcr.column].map{|x| x.is_one_of?(SQL::QualifiedIdentifier, SQL::Identifier) ? literal(x) : quote_identifier(x)}.join('.')
    end

    # Adds quoting to identifiers (columns and tables). If identifiers are not
    # being quoted, returns name as a string.  If identifiers are being quoted
    # quote the name with quoted_identifier.
    def quote_identifier(name)
      quote_identifiers? ? quoted_identifier(name) : name.to_s
    end
    alias_method :quote_column_ref, :quote_identifier

    # This method quotes the given name with the SQL standard double quote. 
    # It uppercases the name given to conform with the SQL standard. This
    # should be overridden by subclasses to provide quoting not matching the
    # SQL standard, such as backtick (used by MySQL and SQLite), or where
    # lowercase is the default for unquoted identifiers (PostgreSQL).
    #
    # If you are using a database such as Oracle that defaults to uppercase
    # but you are using lower case identifiers, you should override this
    # method to not upcase the name for those identifiers.
    def quoted_identifier(name)
      "\"#{name.to_s.upcase}\""
    end

    # Returns a copy of the dataset with the order reversed. If no order is
    # given, the existing order is inverted.
    def reverse_order(*order)
      order(*invert_order(order.empty? ? @opts[:order] : order))
    end
    alias_method :reverse, :reverse_order

    # Returns a copy of the dataset with the columns selected changed
    # to the given columns.
    def select(*columns)
      clone(:select => columns)
    end
    
    # Returns a copy of the dataset selecting the wildcard.
    def select_all
      clone(:select => nil)
    end

    # Returns a copy of the dataset with the given columns added
    # to the existing selected columns.
    def select_more(*columns)
      select(*((@opts[:select] || []) + columns))
    end
    
    # Formats a SELECT statement using the given options and the dataset
    # options.
    def select_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts
      
      if sql = opts[:sql]
        return sql
      end

      columns = opts[:select]
      select_columns = columns ? column_list(columns) : WILDCARD

      if distinct = opts[:distinct]
        distinct_clause = distinct.empty? ? "DISTINCT" : "DISTINCT ON (#{expression_list(distinct)})"
        sql = "SELECT #{distinct_clause} #{select_columns}"
      else
        sql = "SELECT #{select_columns}"
      end
      
      if opts[:from]
        sql << " FROM #{source_list(opts[:from])}"
      end
      
      if join = opts[:join]
        join.each{|j| sql << literal(j)}
      end

      if where = opts[:where]
        sql << " WHERE #{literal(where)}"
      end

      if group = opts[:group]
        sql << " GROUP BY #{expression_list(group)}"
      end

      if having = opts[:having]
        sql << " HAVING #{literal(having)}"
      end

      if order = opts[:order]
        sql << " ORDER BY #{expression_list(order)}"
      end

      if limit = opts[:limit]
        sql << " LIMIT #{limit}"
        if offset = opts[:offset]
          sql << " OFFSET #{offset}"
        end
      end

      if union = opts[:union]
        sql << (opts[:union_all] ? \
          " UNION ALL #{union.sql}" : " UNION #{union.sql}")
      elsif intersect = opts[:intersect]
        sql << (opts[:intersect_all] ? \
          " INTERSECT ALL #{intersect.sql}" : " INTERSECT #{intersect.sql}")
      elsif except = opts[:except]
        sql << (opts[:except_all] ? \
          " EXCEPT ALL #{except.sql}" : " EXCEPT #{except.sql}")
      end

      sql
    end
    alias_method :sql, :select_sql

    # SQL fragment for specifying subscripts (SQL arrays)
    def subscript_sql(s)
      "#{s.f}[#{s.sub.join(COMMA_SEPARATOR)}]"
    end

    # Converts a symbol into a column name. This method supports underscore
    # notation in order to express qualified (two underscores) and aliased
    # (three underscores) columns:
    #
    #   ds = DB[:items]
    #   :abc.to_column_ref(ds) #=> "abc"
    #   :abc___a.to_column_ref(ds) #=> "abc AS a"
    #   :items__abc.to_column_ref(ds) #=> "items.abc"
    #   :items__abc___a.to_column_ref(ds) #=> "items.abc AS a"
    #
    def symbol_to_column_ref(sym)
      c_table, column, c_alias = split_symbol(sym)
      qc = "#{"#{quote_identifier(c_table)}." if c_table}#{quote_identifier(column)}"
      c_alias ? as_sql(qc, c_alias) : qc
    end

    # Returns a copy of the dataset with no filters (HAVING or WHERE clause) applied.
    def unfiltered
      clone(:where => nil, :having => nil)
    end

    # Adds a UNION clause using a second dataset object. If all is true the
    # clause used is UNION ALL, which may return duplicate rows.
    #
    #   DB[:items].union(DB[:other_items]).sql
    #   #=> "SELECT * FROM items UNION SELECT * FROM other_items"
    def union(dataset, all = false)
      clone(:union => dataset, :union_all => all)
    end

    # Returns a copy of the dataset with the distinct option.
    def uniq(*args)
      clone(:distinct => args)
    end
    alias_method :distinct, :uniq

    # Returns a copy of the dataset with no order.
    def unordered
      order(nil)
    end

    # Formats an UPDATE statement using the given values.
    #
    #   dataset.update_sql(:price => 100, :category => 'software') #=>
    #     "UPDATE items SET price = 100, category = 'software'"
    #
    # Accepts a block, but such usage is discouraged.
    #
    # Raises an error if the dataset is grouped or includes more
    # than one table.
    def update_sql(values = {}, opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      if sql = opts[:sql]
        return sql
      end

      if opts[:group]
        raise Error::InvalidOperation, "A grouped dataset cannot be updated"
      elsif (opts[:from].size > 1) or opts[:join]
        raise Error::InvalidOperation, "A joined dataset cannot be updated"
      end
      
      sql = "UPDATE #{source_list(@opts[:from])} SET "
      set = if values.is_a?(Hash)
        values = opts[:defaults].merge(values) if opts[:defaults]
        values = values.merge(opts[:overrides]) if opts[:overrides]
        # get values from hash
        values = transform_save(values) if @transform
        values.map do |k, v|
          "#{k.is_one_of?(String, Symbol) ? quote_identifier(k) : literal(k)} = #{literal(v)}"
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

    [:inner, :full_outer, :right_outer, :left_outer].each do |jtype|
      class_eval("def #{jtype}_join(*args, &block); join_table(:#{jtype}, *args, &block) end")
    end
    alias_method :join, :inner_join

    protected

    # Returns a table reference for use in the FROM clause.  Returns an SQL subquery
    # frgament with an optional table alias.
    def to_table_reference(table_alias=nil)
      "(#{sql})#{" #{quote_identifier(table_alias)}" if table_alias}"
    end

    private

    # SQL fragment for specifying an alias.  expression should already be literalized.
    def as_sql(expression, aliaz)
      "#{expression} AS #{quote_identifier(aliaz)}"
    end

    # Converts an array of column names into a comma seperated string of 
    # column names. If the array is empty, a wildcard (*) is returned.
    def column_list(columns)
      if columns.empty?
        WILDCARD
      else
        m = columns.map do |i|
          i.is_a?(Hash) ? i.map{|k, v| as_sql(literal(k), v)} : literal(i)
        end
        m.join(COMMA_SEPARATOR)
      end
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
          filter_expr(expr.shift.gsub(QUESTION_MARK){literal(expr.shift)}.lit)
        else
          SQL::BooleanExpression.from_value_pairs(expr)
        end
      when Proc
        filter_expr(expr.call(SQL::VirtualRow.new))
      when SQL::NumericExpression, SQL::StringExpression
        raise(Error, "Invalid SQL Expression type: #{expr.inspect}") 
      when Symbol, SQL::Expression
        expr
      when TrueClass, FalseClass
        SQL::BooleanExpression.new(:NOOP, expr)
      when String
        "(#{expr})".lit
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

    # Returns a qualified column name (including a table name) if the column
    # name isn't already qualified.
    def qualified_column_name(column, table)
      if Symbol === column 
        c_table, column, c_alias = split_symbol(column)
        schema, table, t_alias = split_symbol(table) if Symbol === table
        c_table ||= t_alias || table
        ::Sequel::SQL::QualifiedIdentifier.new(c_table, column)
      else
        column
      end
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

    # SQL fragement specifying a table name.
    def table_ref(t)
      case t
      when Dataset
        t.to_table_reference
      when Hash
        t.map{|k, v| as_sql(table_ref(k), v)}.join(COMMA_SEPARATOR)
      when Symbol
        symbol_to_column_ref(t)
      when String
        quote_identifier(t)
      else
        literal(t)
      end
    end
  end
end
