module Sequel
  class Dataset
    # ---------------------
    # :section: Methods that return modified datasets
    # These methods all return modified copies of the receiver.
    # ---------------------

    # The dataset options that require the removal of cached columns
    # if changed.
    COLUMN_CHANGE_OPTS = [:select, :sql, :from, :join].freeze

    # Which options don't affect the SQL generation.  Used by simple_select_all?
    # to determine if this is a simple SELECT * FROM table.
    NON_SQL_OPTIONS = [:server, :defaults, :overrides, :graph, :eager_graph, :graph_aliases]
    
    # These symbols have _join methods created (e.g. inner_join) that
    # call join_table with the symbol, passing along the arguments and
    # block from the method call.
    CONDITIONED_JOIN_TYPES = [:inner, :full_outer, :right_outer, :left_outer, :full, :right, :left]

    # These symbols have _join methods created (e.g. natural_join) that
    # call join_table with the symbol.  They only accept a single table
    # argument which is passed to join_table, and they raise an error
    # if called with a block.
    UNCONDITIONED_JOIN_TYPES = [:natural, :natural_left, :natural_right, :natural_full, :cross]
    
    # All methods that return modified datasets with a joined table added.
    JOIN_METHODS = (CONDITIONED_JOIN_TYPES + UNCONDITIONED_JOIN_TYPES).map{|x| "#{x}_join".to_sym} + [:join, :join_table]
    
    # Methods that return modified datasets
    QUERY_METHODS = %w'add_graph_aliases and distinct except exclude
    filter for_update from from_self graph grep group group_and_count group_by having intersect invert
    limit lock_style naked or order order_append order_by order_more order_prepend paginate qualify query
    reverse reverse_order select select_all select_append select_more server
    set_defaults set_graph_aliases set_overrides unfiltered ungraphed ungrouped union
    unlimited unordered where with with_recursive with_sql'.collect{|x| x.to_sym} + JOIN_METHODS

    # Adds an further filter to an existing filter using AND. If no filter 
    # exists an error is raised. This method is identical to #filter except
    # it expects an existing filter.
    #
    #   ds.filter(:a).and(:b) # SQL: WHERE a AND b
    def and(*cond, &block)
      raise(InvalidOperation, "No existing filter found.") unless @opts[:having] || @opts[:where]
      filter(*cond, &block)
    end
    
    # Returns a new clone of the dataset with with the given options merged.
    # If the options changed include options in COLUMN_CHANGE_OPTS, the cached
    # columns are deleted.
    def clone(opts = {})
      c = super()
      c.opts = @opts.merge(opts)
      c.instance_variable_set(:@columns, nil) if opts.keys.any?{|o| COLUMN_CHANGE_OPTS.include?(o)}
      c
    end

    # Returns a copy of the dataset with the SQL DISTINCT clause.
    # The DISTINCT clause is used to remove duplicate rows from the
    # output.  If arguments are provided, uses a DISTINCT ON clause,
    # in which case it will only be distinct on those columns, instead
    # of all returned columns.  Raises an error if arguments
    # are given and DISTINCT ON is not supported.
    #
    #  dataset.distinct # SQL: SELECT DISTINCT * FROM items
    #  dataset.order(:id).distinct(:id) # SQL: SELECT DISTINCT ON (id) * FROM items ORDER BY id
    def distinct(*args)
      raise(InvalidOperation, "DISTINCT ON not supported") if !args.empty? && !supports_distinct_on?
      clone(:distinct => args)
    end

    # Adds an EXCEPT clause using a second dataset object.
    # An EXCEPT compound dataset returns all rows in the current dataset
    # that are not in the given dataset.
    # Raises an InvalidOperation if the operation is not supported.
    # Options:
    # * :all - Set to true to use EXCEPT ALL instead of EXCEPT, so duplicate rows can occur
    # * :from_self - Set to false to not wrap the returned dataset in a from_self, use with care.
    #
    #   DB[:items].except(DB[:other_items]).sql
    #   #=> "SELECT * FROM items EXCEPT SELECT * FROM other_items"
    def except(dataset, opts={})
      opts = {:all=>opts} unless opts.is_a?(Hash)
      raise(InvalidOperation, "EXCEPT not supported") unless supports_intersect_except?
      raise(InvalidOperation, "EXCEPT ALL not supported") if opts[:all] && !supports_intersect_except_all?
      compound_clone(:except, dataset, opts)
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
    # which is easy to use to create identifiers and functions.  For more details
    # on the virtual row support, see the {"Virtual Rows" guide}[link:files/doc/virtual_rows_rdoc.html]
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
    # See the the {"Dataset Filtering" guide}[link:files/doc/dataset_filtering_rdoc.html] for more examples and details.
    def filter(*cond, &block)
      _filter(@opts[:having] ? :having : :where, *cond, &block)
    end
    
    # Returns a cloned dataset with a :update lock style.
    def for_update
      lock_style(:update)
    end

    # Returns a copy of the dataset with the source changed.
    #
    #   dataset.from # SQL: SELECT *
    #   dataset.from(:blah) # SQL: SELECT * FROM blah
    #   dataset.from(:blah, :foo) # SQL: SELECT * FROM blah, foo
    def from(*source)
      table_alias_num = 0
      sources = []
      source.each do |s|
        case s
        when Hash
          s.each{|k,v| sources << SQL::AliasedExpression.new(k,v)}
        when Dataset
          sources << SQL::AliasedExpression.new(s, dataset_alias(table_alias_num+=1))
        when Symbol
          sch, table, aliaz = split_symbol(s)
          if aliaz
            s = sch ? SQL::QualifiedIdentifier.new(sch.to_sym, table.to_sym) : SQL::Identifier.new(table.to_sym)
            sources << SQL::AliasedExpression.new(s, aliaz.to_sym)
          else
            sources << s
          end
        else
          sources << s
        end
      end
      o = {:from=>sources.empty? ? nil : sources}
      o[:num_dataset_sources] = table_alias_num if table_alias_num > 0
      clone(o)
    end

    # Returns a dataset selecting from the current dataset.
    # Supplying the :alias option controls the name of the result.
    #
    #   ds = DB[:items].order(:name).select(:id, :name)
    #   ds.sql                         #=> "SELECT id,name FROM items ORDER BY name"
    #   ds.from_self.sql               #=> "SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS t1"
    #   ds.from_self(:alias=>:foo).sql #=> "SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS foo"
    def from_self(opts={})
      fs = {}
      @opts.keys.each{|k| fs[k] = nil unless NON_SQL_OPTIONS.include?(k)}
      clone(fs).from(opts[:alias] ? as(opts[:alias]) : self)
    end

    # Match any of the columns to any of the patterns. The terms can be
    # strings (which use LIKE) or regular expressions (which are only
    # supported on MySQL and PostgreSQL).  Note that the total number of
    # pattern matches will be Array(columns).length * Array(terms).length,
    # which could cause performance issues.
    #
    # Options (all are boolean):
    #
    # :all_columns :: All columns must be matched to any of the given patterns.
    # :all_patterns :: All patterns must match at least one of the columns.
    # :case_insensitive :: Use a case insensitive pattern match (the default is
    #                      case sensitive if the database supports it).
    #
    # If both :all_columns and :all_patterns are true, all columns must match all patterns.
    #
    # Examples:
    #
    #   dataset.grep(:a, '%test%')
    #   # SELECT * FROM items WHERE (a LIKE '%test%')
    #
    #   dataset.grep([:a, :b], %w'%test% foo')
    #   # SELECT * FROM items WHERE ((a LIKE '%test%') OR (a LIKE 'foo') OR (b LIKE '%test%') OR (b LIKE 'foo'))
    #
    #   dataset.grep([:a, :b], %w'%foo% %bar%', :all_patterns=>true)
    #   # SELECT * FROM a WHERE (((a LIKE '%foo%') OR (b LIKE '%foo%')) AND ((a LIKE '%bar%') OR (b LIKE '%bar%')))
    #
    #   dataset.grep([:a, :b], %w'%foo% %bar%', :all_columns=>true)
    #   # SELECT * FROM a WHERE (((a LIKE '%foo%') OR (a LIKE '%bar%')) AND ((b LIKE '%foo%') OR (b LIKE '%bar%')))
    #
    #   dataset.grep([:a, :b], %w'%foo% %bar%', :all_patterns=>true, :all_columns=>true)
    #   # SELECT * FROM a WHERE ((a LIKE '%foo%') AND (b LIKE '%foo%') AND (a LIKE '%bar%') AND (b LIKE '%bar%'))
    def grep(columns, patterns, opts={})
      if opts[:all_patterns]
        conds = Array(patterns).map do |pat|
          SQL::BooleanExpression.new(opts[:all_columns] ? :AND : :OR, *Array(columns).map{|c| SQL::StringExpression.like(c, pat, opts)})
        end
        filter(SQL::BooleanExpression.new(opts[:all_patterns] ? :AND : :OR, *conds))
      else
        conds = Array(columns).map do |c|
          SQL::BooleanExpression.new(:OR, *Array(patterns).map{|pat| SQL::StringExpression.like(c, pat, opts)})
        end
        filter(SQL::BooleanExpression.new(opts[:all_columns] ? :AND : :OR, *conds))
      end
    end

    # Returns a copy of the dataset with the results grouped by the value of 
    # the given columns.
    #
    #   dataset.group(:id) # SELECT * FROM items GROUP BY id
    #   dataset.group(:id, :name) # SELECT * FROM items GROUP BY id, name
    def group(*columns)
      clone(:group => (columns.compact.empty? ? nil : columns))
    end

    # Alias of group
    def group_by(*columns)
      group(*columns)
    end
    
    # Returns a dataset grouped by the given column with count by group.
    # Column aliases may be supplied, and will be included in the select clause.
    #
    # Examples:
    #
    #   ds.group_and_count(:name).all => [{:name=>'a', :count=>1}, ...]
    #   ds.group_and_count(:first_name, :last_name).all => [{:first_name=>'a', :last_name=>'b', :count=>1}, ...]
    #   ds.group_and_count(:first_name___name).all => [{:name=>'a', :count=>1}, ...]
    def group_and_count(*columns)
      group(*columns.map{|c| unaliased_identifier(c)}).select(*(columns + [COUNT_OF_ALL_AS_COUNT]))
    end

    # Returns a copy of the dataset with the HAVING conditions changed. See #filter for argument types.
    #
    #   dataset.group(:sum).having(:sum=>10) # SQL: SELECT * FROM items GROUP BY sum HAVING sum = 10 
    def having(*cond, &block)
      _filter(:having, *cond, &block)
    end
    
    # Adds an INTERSECT clause using a second dataset object.
    # An INTERSECT compound dataset returns all rows in both the current dataset
    # and the given dataset.
    # Raises an InvalidOperation if the operation is not supported.
    # Options:
    # * :all - Set to true to use INTERSECT ALL instead of INTERSECT, so duplicate rows can occur
    # * :from_self - Set to false to not wrap the returned dataset in a from_self, use with care.
    #
    #   DB[:items].intersect(DB[:other_items]).sql
    #   #=> "SELECT * FROM items INTERSECT SELECT * FROM other_items"
    def intersect(dataset, opts={})
      opts = {:all=>opts} unless opts.is_a?(Hash)
      raise(InvalidOperation, "INTERSECT not supported") unless supports_intersect_except?
      raise(InvalidOperation, "INTERSECT ALL not supported") if opts[:all] && !supports_intersect_except_all?
      compound_clone(:intersect, dataset, opts)
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

    # Alias of inner_join
    def join(*args, &block)
      inner_join(*args, &block)
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

      case options
      when Hash
        table_alias = options[:table_alias]
        last_alias = options[:implicit_qualifier]
      when Symbol, String, SQL::Identifier
        table_alias = options
        last_alias = nil 
      else
        raise Error, "invalid options format for join_table: #{options.inspect}"
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
    
    CONDITIONED_JOIN_TYPES.each do |jtype|
      class_eval("def #{jtype}_join(*args, &block); join_table(:#{jtype}, *args, &block) end", __FILE__, __LINE__)
    end
    UNCONDITIONED_JOIN_TYPES.each do |jtype|
      class_eval("def #{jtype}_join(table); raise(Sequel::Error, '#{jtype}_join does not accept join table blocks') if block_given?; join_table(:#{jtype}, table) end", __FILE__, __LINE__)
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
      l = l.to_i if l.is_a?(String) && !l.is_a?(LiteralString)
      if l.is_a?(Integer)
        raise(Error, 'Limits must be greater than or equal to 1') unless l >= 1
      end
      opts = {:limit => l}
      if o
        o = o.to_i if o.is_a?(String) && !o.is_a?(LiteralString)
        if o.is_a?(Integer)
          raise(Error, 'Offsets must be greater than or equal to 0') unless o >= 0
        end
        opts[:offset] = o
      end
      clone(opts)
    end
    
    # Returns a cloned dataset with the given lock style.  If style is a
    # string, it will be used directly.  Otherwise, a symbol may be used
    # for database independent locking.  Currently :update is respected
    # by most databases, and :share is supported by some.
    def lock_style(style)
      clone(:lock => style)
    end
    
    # Returns a naked dataset clone - i.e. a dataset that returns records as
    # hashes instead of calling the row proc.
    def naked
      ds = clone
      ds.row_proc = nil
      ds
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
      columns += Array(Sequel.virtual_row(&block)) if block
      clone(:order => (columns.compact.empty?) ? nil : columns)
    end
    
    # Alias of order_more, for naming consistency with order_prepend.
    def order_append(*columns, &block)
      order_more(*columns, &block)
    end

    # Alias of order
    def order_by(*columns, &block)
      order(*columns, &block)
    end

    # Returns a copy of the dataset with the order columns added
    # to the end of the existing order.
    #
    #   ds.order(:a).order(:b).sql #=> 'SELECT * FROM items ORDER BY b'
    #   ds.order(:a).order_more(:b).sql #=> 'SELECT * FROM items ORDER BY a, b'
    def order_more(*columns, &block)
      columns = @opts[:order] + columns if @opts[:order]
      order(*columns, &block)
    end
    
    # Returns a copy of the dataset with the order columns added
    # to the beginning of the existing order.
    #
    #   ds.order(:a).order(:b).sql #=> 'SELECT * FROM items ORDER BY b'
    #   ds.order(:a).order_prepend(:b).sql #=> 'SELECT * FROM items ORDER BY b, a'
    def order_prepend(*columns, &block)
      ds = order(*columns, &block)
      @opts[:order] ? ds.order_more(*@opts[:order]) : ds
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
    
    # Returns a copy of the dataset with the order reversed. If no order is
    # given, the existing order is inverted.
    def reverse(*order)
      order(*invert_order(order.empty? ? @opts[:order] : order))
    end

    # Alias of reverse
    def reverse_order(*order)
      reverse(*order)
    end

    # Returns a copy of the dataset with the columns selected changed
    # to the given columns. This also takes a virtual row block,
    # similar to filter.
    #
    #   dataset.select(:a) # SELECT a FROM items
    #   dataset.select(:a, :b) # SELECT a, b FROM items
    #   dataset.select{|o| [o.a, o.sum(:b)]} # SELECT a, sum(b) FROM items
    def select(*columns, &block)
      columns += Array(Sequel.virtual_row(&block)) if block
      m = []
      columns.map do |i|
        i.is_a?(Hash) ? m.concat(i.map{|k, v| SQL::AliasedExpression.new(k,v)}) : m << i
      end
      clone(:select => m)
    end
    
    # Returns a copy of the dataset selecting the wildcard.
    #
    #   dataset.select(:a).select_all # SELECT * FROM items
    def select_all
      clone(:select => nil)
    end
    
    # Returns a copy of the dataset with the given columns added
    # to the existing selected columns.  If no columns are currently selected
    # it will select the columns given in addition to *.
    #
    #   dataset.select(:a).select(:b) # SELECT b FROM items
    #   dataset.select(:a).select_append(:b) # SELECT a, b FROM items
    #   dataset.select_append(:b) # SELECT *, b FROM items
    def select_append(*columns, &block)
      cur_sel = @opts[:select]
      cur_sel = [WILDCARD] if !cur_sel || cur_sel.empty?
      select(*(cur_sel + columns), &block)
    end

    # Returns a copy of the dataset with the given columns added
    # to the existing selected columns. If no columns are currently selected
    # it will just select the columns given. 
    #
    #   dataset.select(:a).select(:b) # SELECT b FROM items
    #   dataset.select(:a).select_more(:b) # SELECT a, b FROM items
    #   dataset.select_more(:b) # SELECT b FROM items
    def select_more(*columns, &block)
      columns = @opts[:select] + columns if @opts[:select]
      select(*columns, &block)
    end
    
    # Set the server for this dataset to use.  Used to pick a specific database
    # shard to run a query against, or to override the default (which is SELECT uses
    # :read_only database and all other queries use the :default database).
    def server(servr)
      clone(:server=>servr)
    end

    # Set the default values for insert and update statements.  The values hash passed
    # to insert or update are merged into this hash.
    def set_defaults(hash)
      clone(:defaults=>(@opts[:defaults]||{}).merge(hash))
    end

    # Set values that override hash arguments given to insert and update statements.
    # This hash is merged into the hash provided to insert or update.
    def set_overrides(hash)
      clone(:overrides=>hash.merge(@opts[:overrides]||{}))
    end
    
    # Returns a copy of the dataset with no filters (HAVING or WHERE clause) applied.
    # 
    #   dataset.group(:a).having(:a=>1).where(:b).unfiltered # SELECT * FROM items GROUP BY a
    def unfiltered
      clone(:where => nil, :having => nil)
    end

    # Returns a copy of the dataset with no grouping (GROUP or HAVING clause) applied.
    # 
    #   dataset.group(:a).having(:a=>1).where(:b).ungrouped # SELECT * FROM items WHERE b
    def ungrouped
      clone(:group => nil, :having => nil)
    end

    # Adds a UNION clause using a second dataset object.
    # A UNION compound dataset returns all rows in either the current dataset
    # or the given dataset.
    # Options:
    # * :all - Set to true to use UNION ALL instead of UNION, so duplicate rows can occur
    # * :from_self - Set to false to not wrap the returned dataset in a from_self, use with care.
    #
    #   DB[:items].union(DB[:other_items]).sql
    #   #=> "SELECT * FROM items UNION SELECT * FROM other_items"
    def union(dataset, opts={})
      opts = {:all=>opts} unless opts.is_a?(Hash)
      compound_clone(:union, dataset, opts)
    end
    
    # Returns a copy of the dataset with no limit or offset.
    # 
    #   dataset.limit(10, 20).unlimited # SELECT * FROM items
    def unlimited
      clone(:limit=>nil, :offset=>nil)
    end

    # Returns a copy of the dataset with no order.
    # 
    #   dataset.order(:a).unordered # SELECT * FROM items
    def unordered
      order(nil)
    end
    
    # Add a condition to the WHERE clause.  See #filter for argument types.
    #
    #   dataset.group(:a).having(:a).filter(:b) # SELECT * FROM items GROUP BY a HAVING a AND b
    #   dataset.group(:a).having(:a).where(:b) # SELECT * FROM items WHERE b GROUP BY a HAVING a
    def where(*cond, &block)
      _filter(:where, *cond, &block)
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
    
    protected

    # Return true if the dataset has a non-nil value for any key in opts.
    def options_overlap(opts)
      !(@opts.collect{|k,v| k unless v.nil?}.compact & opts).empty?
    end

    # Whether this dataset is a simple SELECT * FROM table.
    def simple_select_all?
      o = @opts.reject{|k,v| v.nil? || NON_SQL_OPTIONS.include?(k)}
      o.length == 1 && (f = o[:from]) && f.length == 1 && f.first.is_a?(Symbol)
    end

    private

    # Internal filter method so it works on either the having or where clauses.
    def _filter(clause, *cond, &block)
      cond = cond.first if cond.size == 1
      if cond.respond_to?(:empty?) && cond.empty? && !block
        clone
      else
        cond = filter_expr(cond, &block)
        cond = SQL::BooleanExpression.new(:AND, @opts[clause], cond) if @opts[clause]
        clone(clause => cond)
      end
    end
    
    # Add the dataset to the list of compounds
    def compound_clone(type, dataset, opts)
      ds = compound_from_self.clone(:compounds=>Array(@opts[:compounds]).map{|x| x.dup} + [[type, dataset.compound_from_self, opts[:all]]])
      opts[:from_self] == false ? ds : ds.from_self(opts)
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
        if (sexpr = expr.at(0)).is_a?(String)
          SQL::PlaceholderLiteralString.new(sexpr, expr[1..-1], true)
        elsif Sequel.condition_specifier?(expr)
          SQL::BooleanExpression.from_value_pairs(expr)
        else
          SQL::BooleanExpression.new(:AND, *expr.map{|x| filter_expr(x)})
        end
      when Proc
        filter_expr(Sequel.virtual_row(&expr))
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
          f.invert
        else
          SQL::OrderedExpression.new(f)
        end
      end
    end
  end
end
