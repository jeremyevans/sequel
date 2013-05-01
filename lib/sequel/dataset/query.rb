module Sequel
  class Dataset
    # ---------------------
    # :section: 1 - Methods that return modified datasets
    # These methods all return modified copies of the receiver.
    # ---------------------

    # Hash of extension name symbols to callable objects to load the extension
    # into the Dataset object (usually by extending it with a module defined
    # in the extension).
    EXTENSIONS = {}

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
    QUERY_METHODS = (<<-METHS).split.map{|x| x.to_sym} + JOIN_METHODS
      add_graph_aliases and distinct except exclude exclude_having exclude_where
      filter for_update from from_self graph grep group group_and_count group_by having intersect invert
      limit lock_style naked or order order_append order_by order_more order_prepend paginate qualify query
      reverse reverse_order select select_all select_append select_group select_more server
      set_defaults set_graph_aliases set_overrides unfiltered ungraphed ungrouped union
      unlimited unordered where with with_recursive with_sql
    METHS

    # Register an extension callback for Dataset objects.  ext should be the
    # extension name symbol, and mod should either be a Module that the
    # dataset is extended with, or a callable object called with the database
    # object.  If mod is not provided, a block can be provided and is treated
    # as the mod object.
    #
    # If mod is a module, this also registers a Database extension that will
    # extend all of the database's datasets.
    def self.register_extension(ext, mod=nil, &block)
      if mod
        raise(Error, "cannot provide both mod and block to Dataset.register_extension") if block
        if mod.is_a?(Module)
          block = proc{|ds| ds.extend(mod)}
          Sequel::Database.register_extension(ext){|db| db.extend_datasets(mod)}
        else
          block = mod
        end
      end
      Sequel.synchronize{EXTENSIONS[ext] = block}
    end

    # Adds an further filter to an existing filter using AND. If no filter 
    # exists an error is raised. This method is identical to #filter except
    # it expects an existing filter.
    #
    #   DB[:table].filter(:a).and(:b) # SELECT * FROM table WHERE a AND b
    def and(*cond, &block)
      unless @opts[:having] || @opts[:where]
        Sequel::Deprecation.deprecate('Dataset#and will no longer raise for an unfilered dataset starting in Sequel 4.')
        raise(InvalidOperation, "No existing filter found.")
      end
      if @opts[:having]
        Sequel::Deprecation.deprecate('Dataset#and will no longer modify the HAVING clause starting in Sequel 4.  Switch to using Dataset#having or use the filter_having extension.')
        having(*cond, &block)
      else
        where(*cond, &block)
      end
    end
    
    # Returns a new clone of the dataset with with the given options merged.
    # If the options changed include options in COLUMN_CHANGE_OPTS, the cached
    # columns are deleted.  This method should generally not be called
    # directly by user code.
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
    #  DB[:items].distinct # SQL: SELECT DISTINCT * FROM items
    #  DB[:items].order(:id).distinct(:id) # SQL: SELECT DISTINCT ON (id) * FROM items ORDER BY id
    def distinct(*args)
      raise(InvalidOperation, "DISTINCT ON not supported") if !args.empty? && !supports_distinct_on?
      clone(:distinct => args)
    end

    # Adds an EXCEPT clause using a second dataset object.
    # An EXCEPT compound dataset returns all rows in the current dataset
    # that are not in the given dataset.
    # Raises an +InvalidOperation+ if the operation is not supported.
    # Options:
    # :alias :: Use the given value as the from_self alias
    # :all :: Set to true to use EXCEPT ALL instead of EXCEPT, so duplicate rows can occur
    # :from_self :: Set to false to not wrap the returned dataset in a from_self, use with care.
    #
    #   DB[:items].except(DB[:other_items])
    #   # SELECT * FROM (SELECT * FROM items EXCEPT SELECT * FROM other_items) AS t1
    #
    #   DB[:items].except(DB[:other_items], :all=>true, :from_self=>false)
    #   # SELECT * FROM items EXCEPT ALL SELECT * FROM other_items
    #
    #   DB[:items].except(DB[:other_items], :alias=>:i)
    #   # SELECT * FROM (SELECT * FROM items EXCEPT SELECT * FROM other_items) AS i
    def except(dataset, opts={})
      opts = {:all=>opts} unless opts.is_a?(Hash)
      raise(InvalidOperation, "EXCEPT not supported") unless supports_intersect_except?
      raise(InvalidOperation, "EXCEPT ALL not supported") if opts[:all] && !supports_intersect_except_all?
      compound_clone(:except, dataset, opts)
    end

    # Performs the inverse of Dataset#filter.  Note that if you have multiple filter
    # conditions, this is not the same as a negation of all conditions.
    #
    #   DB[:items].exclude(:category => 'software')
    #   # SELECT * FROM items WHERE (category != 'software')
    #   
    #   DB[:items].exclude(:category => 'software', :id=>3)
    #   # SELECT * FROM items WHERE ((category != 'software') OR (id != 3))
    def exclude(*cond, &block)
      Sequel::Deprecation.deprecate('Dataset#exclude will no longer modify the HAVING clause starting in Sequel 4.  Switch to using Dataset#exclude_having or use the filter_having extension.') if @opts[:having]
      _filter_or_exclude(true, @opts[:having] ? :having : :where, *cond, &block)
    end

    # Inverts the given conditions and adds them to the HAVING clause.
    #
    #   DB[:items].select_group(:name).exclude_having{count(name) < 2}
    #   # SELECT name FROM items GROUP BY name HAVING (count(name) >= 2)
    def exclude_having(*cond, &block)
      _filter_or_exclude(true, :having, *cond, &block)
    end

    # Inverts the given conditions and adds them to the WHERE clause.
    #
    #   DB[:items].select_group(:name).exclude_where(:category => 'software')
    #   # SELECT * FROM items WHERE (category != 'software')
    #
    #   DB[:items].select_group(:name).
    #     exclude_having{count(name) < 2}.
    #     exclude_where(:category => 'software')
    #   # SELECT name FROM items WHERE (category != 'software')
    #   # GROUP BY name HAVING (count(name) >= 2)
    def exclude_where(*cond, &block)
      _filter_or_exclude(true, :where, *cond, &block)
    end

    # Return a clone of the dataset loaded with the extensions, see #extension!.
    def extension(*exts)
      clone.extension!(*exts)
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
    #   * Otherwise, treats each argument as a separate condition.
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
    # If both a block and regular argument are provided, they get ANDed together.
    #
    # Examples:
    #
    #   DB[:items].filter(:id => 3)
    #   # SELECT * FROM items WHERE (id = 3)
    #
    #   DB[:items].filter('price < ?', 100)
    #   # SELECT * FROM items WHERE price < 100
    #
    #   DB[:items].filter([[:id, [1,2,3]], [:id, 0..10]])
    #   # SELECT * FROM items WHERE ((id IN (1, 2, 3)) AND ((id >= 0) AND (id <= 10)))
    #
    #   DB[:items].filter('price < 100')
    #   # SELECT * FROM items WHERE price < 100
    #
    #   DB[:items].filter(:active)
    #   # SELECT * FROM items WHERE :active
    #
    #   DB[:items].filter{price < 100}
    #   # SELECT * FROM items WHERE (price < 100)
    # 
    # Multiple filter calls can be chained for scoping:
    #
    #   software = dataset.filter(:category => 'software').filter{price < 100}
    #   # SELECT * FROM items WHERE ((category = 'software') AND (price < 100))
    #
    # See the the {"Dataset Filtering" guide}[link:files/doc/dataset_filtering_rdoc.html] for more examples and details.
    def filter(*cond, &block)
      Sequel::Deprecation.deprecate('Dataset#filter will no longer modify the HAVING clause starting in Sequel 4.  Switch to using Dataset#having or use the filter_having extension.') if @opts[:having]
      _filter(@opts[:having] ? :having : :where, *cond, &block)
    end
    
    # Returns a cloned dataset with a :update lock style.
    #
    #   DB[:table].for_update # SELECT * FROM table FOR UPDATE
    def for_update
      lock_style(:update)
    end

    # Returns a copy of the dataset with the source changed. If no
    # source is given, removes all tables.  If multiple sources
    # are given, it is the same as using a CROSS JOIN (cartesian product) between all tables.
    #
    #   DB[:items].from # SQL: SELECT *
    #   DB[:items].from(:blah) # SQL: SELECT * FROM blah
    #   DB[:items].from(:blah, :foo) # SQL: SELECT * FROM blah, foo
    def from(*source)
      table_alias_num = 0
      sources = []
      ctes = nil
      source.each do |s|
        case s
        when Hash
          Sequel::Deprecation.deprecate('Dataset#from will no longer treat an input hash as an alias specifier.  Switch to aliasing using Sequel.as or use the hash_aliases extension.')
          s.each{|k,v| sources << SQL::AliasedExpression.new(k,v)}
        when Dataset
          if hoist_cte?(s)
            ctes ||= []
            ctes += s.opts[:with]
            s = s.clone(:with=>nil)
          end
          sources << SQL::AliasedExpression.new(s, dataset_alias(table_alias_num+=1))
        when Symbol
          sch, table, aliaz = split_symbol(s)
          if aliaz
            s = sch ? SQL::QualifiedIdentifier.new(sch, table) : SQL::Identifier.new(table)
            sources << SQL::AliasedExpression.new(s, aliaz.to_sym)
          else
            sources << s
          end
        else
          sources << s
        end
      end
      o = {:from=>sources.empty? ? nil : sources}
      o[:with] = (opts[:with] || []) + ctes if ctes
      o[:num_dataset_sources] = table_alias_num if table_alias_num > 0
      clone(o)
    end

    # Returns a dataset selecting from the current dataset.
    # Supplying the :alias option controls the alias of the result.
    #
    #   ds = DB[:items].order(:name).select(:id, :name)
    #   # SELECT id,name FROM items ORDER BY name
    #
    #   ds.from_self
    #   # SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS t1
    #
    #   ds.from_self(:alias=>:foo)
    #   # SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS foo
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
    # the given columns.  If a block is given, it is treated
    # as a virtual row block, similar to +filter+.
    #
    #   DB[:items].group(:id) # SELECT * FROM items GROUP BY id
    #   DB[:items].group(:id, :name) # SELECT * FROM items GROUP BY id, name
    #   DB[:items].group{[a, sum(b)]} # SELECT * FROM items GROUP BY a, sum(b)
    def group(*columns, &block)
      virtual_row_columns(columns, block)
      clone(:group => (columns.compact.empty? ? nil : columns))
    end

    # Alias of group
    def group_by(*columns, &block)
      group(*columns, &block)
    end
    
    # Returns a dataset grouped by the given column with count by group.
    # Column aliases may be supplied, and will be included in the select clause.
    # If a block is given, it is treated as a virtual row block, similar to +filter+.
    #
    # Examples:
    #
    #   DB[:items].group_and_count(:name).all
    #   # SELECT name, count(*) AS count FROM items GROUP BY name 
    #   # => [{:name=>'a', :count=>1}, ...]
    #
    #   DB[:items].group_and_count(:first_name, :last_name).all
    #   # SELECT first_name, last_name, count(*) AS count FROM items GROUP BY first_name, last_name
    #   # => [{:first_name=>'a', :last_name=>'b', :count=>1}, ...]
    #
    #   DB[:items].group_and_count(:first_name___name).all
    #   # SELECT first_name AS name, count(*) AS count FROM items GROUP BY first_name
    #   # => [{:name=>'a', :count=>1}, ...]
    #
    #   DB[:items].group_and_count{substr(first_name, 1, 1).as(initial)}.all
    #   # SELECT substr(first_name, 1, 1) AS initial, count(*) AS count FROM items GROUP BY substr(first_name, 1, 1)
    #   # => [{:initial=>'a', :count=>1}, ...]
    def group_and_count(*columns, &block)
      select_group(*columns, &block).select_more(COUNT_OF_ALL_AS_COUNT)
    end

    # Adds the appropriate CUBE syntax to GROUP BY.
    def group_cube
      raise Error, "GROUP BY CUBE not supported on #{db.database_type}" unless supports_group_cube?
      clone(:group_options=>:cube)
    end

    # Adds the appropriate ROLLUP syntax to GROUP BY.
    def group_rollup
      raise Error, "GROUP BY ROLLUP not supported on #{db.database_type}" unless supports_group_rollup?
      clone(:group_options=>:rollup)
    end

    # Returns a copy of the dataset with the HAVING conditions changed. See #filter for argument types.
    #
    #   DB[:items].group(:sum).having(:sum=>10)
    #   # SELECT * FROM items GROUP BY sum HAVING (sum = 10)
    def having(*cond, &block)
      _filter(:having, *cond, &block)
    end
    
    # Adds an INTERSECT clause using a second dataset object.
    # An INTERSECT compound dataset returns all rows in both the current dataset
    # and the given dataset.
    # Raises an +InvalidOperation+ if the operation is not supported.
    # Options:
    # :alias :: Use the given value as the from_self alias
    # :all :: Set to true to use INTERSECT ALL instead of INTERSECT, so duplicate rows can occur
    # :from_self :: Set to false to not wrap the returned dataset in a from_self, use with care.
    #
    #   DB[:items].intersect(DB[:other_items])
    #   # SELECT * FROM (SELECT * FROM items INTERSECT SELECT * FROM other_items) AS t1
    #
    #   DB[:items].intersect(DB[:other_items], :all=>true, :from_self=>false)
    #   # SELECT * FROM items INTERSECT ALL SELECT * FROM other_items
    #
    #   DB[:items].intersect(DB[:other_items], :alias=>:i)
    #   # SELECT * FROM (SELECT * FROM items INTERSECT SELECT * FROM other_items) AS i
    def intersect(dataset, opts={})
      opts = {:all=>opts} unless opts.is_a?(Hash)
      raise(InvalidOperation, "INTERSECT not supported") unless supports_intersect_except?
      raise(InvalidOperation, "INTERSECT ALL not supported") if opts[:all] && !supports_intersect_except_all?
      compound_clone(:intersect, dataset, opts)
    end

    # Inverts the current filter.
    #
    #   DB[:items].filter(:category => 'software').invert
    #   # SELECT * FROM items WHERE (category != 'software')
    #
    #   DB[:items].filter(:category => 'software', :id=>3).invert
    #   # SELECT * FROM items WHERE ((category != 'software') OR (id != 3))
    def invert
      having, where = @opts[:having], @opts[:where]
      unless having || where
        Sequel::Deprecation.deprecate('Dataset#invert will no longer raise for an unfilered dataset starting in Sequel 4.')
        raise(Error, "No current filter")
      end
      o = {}
      o[:having] = SQL::BooleanExpression.invert(having) if having
      o[:where] = SQL::BooleanExpression.invert(where) if where
      clone(o)
    end

    # Alias of +inner_join+
    def join(*args, &block)
      inner_join(*args, &block)
    end

    # Returns a joined dataset.  Not usually called directly, users should use the
    # appropriate join method (e.g. join, left_join, natural_join, cross_join) which fills
    # in the +type+ argument.
    #
    # Takes the following arguments:
    #
    # * type - The type of join to do (e.g. :inner)
    # * table - Depends on type:
    #   * Dataset - a subselect is performed with an alias of tN for some value of N
    #   * String, Symbol: table
    # * expr - specifies conditions, depends on type:
    #   * Hash, Array of two element arrays - Assumes key (1st arg) is column of joined table (unless already
    #     qualified), and value (2nd arg) is column of the last joined or primary table (or the
    #     :implicit_qualifier option).
    #     To specify multiple conditions on a single joined table column, you must use an array.
    #     Uses a JOIN with an ON clause.
    #   * Array - If all members of the array are symbols, considers them as columns and 
    #     uses a JOIN with a USING clause.  Most databases will remove duplicate columns from
    #     the result set if this is used.
    #   * nil - If a block is not given, doesn't use ON or USING, so the JOIN should be a NATURAL
    #     or CROSS join. If a block is given, uses an ON clause based on the block, see below.
    #   * Everything else - pretty much the same as a using the argument in a call to filter,
    #     so strings are considered literal, symbols specify boolean columns, and Sequel
    #     expressions can be used. Uses a JOIN with an ON clause.
    # * options - a hash of options, with any of the following keys:
    #   * :table_alias - the name of the table's alias when joining, necessary for joining
    #     to the same table more than once.  No alias is used by default.
    #   * :implicit_qualifier - The name to use for qualifying implicit conditions.  By default,
    #     the last joined or primary table is used.
    #   * :qualify - Can be set to false to not do any implicit qualification.  Can be set
    #     to :deep to use the Qualifier AST Transformer, which will attempt to qualify
    #     subexpressions of the expression tree.  Defaults to the value of
    #     default_join_table_qualification.
    # * block - The block argument should only be given if a JOIN with an ON clause is used,
    #   in which case it yields the table alias/name for the table currently being joined,
    #   the table alias/name for the last joined (or first table), and an array of previous
    #   SQL::JoinClause. Unlike +filter+, this block is not treated as a virtual row block.
    #
    # Examples:
    #
    #   DB[:a].join_table(:cross, :b)
    #   # SELECT * FROM a CROSS JOIN b
    #
    #   DB[:a].join_table(:inner, DB[:b], :c=>d)
    #   # SELECT * FROM a INNER JOIN (SELECT * FROM b) AS t1 ON (t1.c = a.d)
    #
    #   DB[:a].join_table(:left, :b___c, [:d])
    #   # SELECT * FROM a LEFT JOIN b AS c USING (d)
    #
    #   DB[:a].natural_join(:b).join_table(:inner, :c) do |ta, jta, js|
    #     (Sequel.qualify(ta, :d) > Sequel.qualify(jta, :e)) & {Sequel.qualify(ta, :f)=>DB.from(js.first.table).select(:g)}
    #   end
    #   # SELECT * FROM a NATURAL JOIN b INNER JOIN c
    #   #   ON ((c.d > b.e) AND (c.f IN (SELECT g FROM b)))
    def join_table(type, table, expr=nil, options={}, &block)
      if hoist_cte?(table)
        s, ds = hoist_cte(table)
        return s.join_table(type, ds, expr, options, &block)
      end

      using_join = expr.is_a?(Array) && !expr.empty? && expr.all?{|x| x.is_a?(Symbol)}
      if using_join && !supports_join_using?
        h = {}
        expr.each{|e| h[e] = e}
        return join_table(type, table, h, options)
      end

      case options
      when Hash
        table_alias = options[:table_alias]
        last_alias = options[:implicit_qualifier]
        qualify_type = options[:qualify]
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
        table, implicit_table_alias = split_alias(table)
        table_alias ||= implicit_table_alias
        table_name = table_alias || table
      end

      join = if expr.nil? and !block
        SQL::JoinClause.new(type, table, table_alias)
      elsif using_join
        raise(Sequel::Error, "can't use a block if providing an array of symbols as expr") if block
        SQL::JoinUsingClause.new(expr, type, table, table_alias)
      else
        last_alias ||= @opts[:last_joined_table] || first_source_alias
        if Sequel.condition_specifier?(expr)
          expr = expr.collect do |k, v|
            qualify_type = default_join_table_qualification if qualify_type.nil?
            case qualify_type
            when false
              nil # Do no qualification
            when :deep
              k = Sequel::Qualifier.new(self, table_name).transform(k)
              v = Sequel::Qualifier.new(self, last_alias).transform(v)
            else
              k = qualified_column_name(k, table_name) if k.is_a?(Symbol)
              v = qualified_column_name(v, last_alias) if v.is_a?(Symbol)
            end
            [k,v]
          end
          expr = SQL::BooleanExpression.from_value_pairs(expr)
        end
        if block
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
    # range. If a second argument is given, it is used as an offset. To use
    # an offset without a limit, pass nil as the first argument.
    #
    #   DB[:items].limit(10) # SELECT * FROM items LIMIT 10
    #   DB[:items].limit(10, 20) # SELECT * FROM items LIMIT 10 OFFSET 20
    #   DB[:items].limit(10...20) # SELECT * FROM items LIMIT 10 OFFSET 10
    #   DB[:items].limit(10..20) # SELECT * FROM items LIMIT 11 OFFSET 10
    #   DB[:items].limit(nil, 20) # SELECT * FROM items OFFSET 20
    def limit(l, o = (no_offset = true; nil))
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
      elsif !no_offset
        opts[:offset] = nil
      end
      clone(opts)
    end
    
    # Returns a cloned dataset with the given lock style.  If style is a
    # string, it will be used directly. You should never pass a string
    # to this method that is derived from user input, as that can lead to
    # SQL injection.
    #
    # A symbol may be used for database independent locking behavior, but
    # all supported symbols have separate methods (e.g. for_update).
    #
    #   DB[:items].lock_style('FOR SHARE NOWAIT') # SELECT * FROM items FOR SHARE NOWAIT
    def lock_style(style)
      clone(:lock => style)
    end
    
    # Returns a cloned dataset without a row_proc.
    #
    #   ds = DB[:items]
    #   ds.row_proc = proc{|r| r.invert}
    #   ds.all # => [{2=>:id}]
    #   ds.naked.all # => [{:id=>2}]
    def naked
      ds = clone
      ds.row_proc = nil
      ds
    end
    
    # Adds an alternate filter to an existing filter using OR. If no filter 
    # exists an +Error+ is raised.
    #
    #   DB[:items].filter(:a).or(:b) # SELECT * FROM items WHERE a OR b
    def or(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      unless @opts[clause]
        Sequel::Deprecation.deprecate('Dataset#or will no longer raise for an unfilered dataset starting in Sequel 4.')
        raise(InvalidOperation, "No existing filter found.")
      end
      Sequel::Deprecation.deprecate('Dataset#or will no longer modify the HAVING clause starting in Sequel 4.  You can use the filter_having extension to continue to use the current behavior.') if clause == :having
      cond = cond.first if cond.size == 1
      clone(clause => SQL::BooleanExpression.new(:OR, @opts[clause], filter_expr(cond, &block)))
    end

    # Returns a copy of the dataset with the order changed. If the dataset has an
    # existing order, it is ignored and overwritten with this order. If a nil is given
    # the returned dataset has no order. This can accept multiple arguments
    # of varying kinds, such as SQL functions.  If a block is given, it is treated
    # as a virtual row block, similar to +filter+.
    #
    #   DB[:items].order(:name) # SELECT * FROM items ORDER BY name
    #   DB[:items].order(:a, :b) # SELECT * FROM items ORDER BY a, b
    #   DB[:items].order(Sequel.lit('a + b')) # SELECT * FROM items ORDER BY a + b
    #   DB[:items].order(:a + :b) # SELECT * FROM items ORDER BY (a + b)
    #   DB[:items].order(Sequel.desc(:name)) # SELECT * FROM items ORDER BY name DESC
    #   DB[:items].order(Sequel.asc(:name, :nulls=>:last)) # SELECT * FROM items ORDER BY name ASC NULLS LAST
    #   DB[:items].order{sum(name).desc} # SELECT * FROM items ORDER BY sum(name) DESC
    #   DB[:items].order(nil) # SELECT * FROM items
    def order(*columns, &block)
      virtual_row_columns(columns, block)
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
    #   DB[:items].order(:a).order(:b) # SELECT * FROM items ORDER BY b
    #   DB[:items].order(:a).order_more(:b) # SELECT * FROM items ORDER BY a, b
    def order_more(*columns, &block)
      columns = @opts[:order] + columns if @opts[:order]
      order(*columns, &block)
    end
    
    # Returns a copy of the dataset with the order columns added
    # to the beginning of the existing order.
    #
    #   DB[:items].order(:a).order(:b) # SELECT * FROM items ORDER BY b
    #   DB[:items].order(:a).order_prepend(:b) # SELECT * FROM items ORDER BY b, a
    def order_prepend(*columns, &block)
      ds = order(*columns, &block)
      @opts[:order] ? ds.order_more(*@opts[:order]) : ds
    end
    
    # Qualify to the given table, or first source if no table is given.
    #
    #   DB[:items].filter(:id=>1).qualify
    #   # SELECT items.* FROM items WHERE (items.id = 1)
    #
    #   DB[:items].filter(:id=>1).qualify(:i)
    #   # SELECT i.* FROM items WHERE (i.id = 1)
    def qualify(table=first_source)
      qualify_to(table)
    end

    # Return a copy of the dataset with unqualified identifiers in the
    # SELECT, WHERE, GROUP, HAVING, and ORDER clauses qualified by the
    # given table. If no columns are currently selected, select all
    # columns of the given table.
    #
    #   DB[:items].filter(:id=>1).qualify_to(:i)
    #   # SELECT i.* FROM items WHERE (i.id = 1)
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
    # See +qualify_to+.
    #
    #   DB[:items].filter(:id=>1).qualify_to_first_source
    #   # SELECT items.* FROM items WHERE (items.id = 1)
    def qualify_to_first_source
      qualify_to(first_source)
    end
    
    # Modify the RETURNING clause, only supported on a few databases.  If returning
    # is used, instead of insert returning the autogenerated primary key or
    # update/delete returning the number of modified rows, results are
    # returned using +fetch_rows+.
    #
    #   DB[:items].returning # RETURNING *
    #   DB[:items].returning(nil) # RETURNING NULL
    #   DB[:items].returning(:id, :name) # RETURNING id, name
    def returning(*values)
      clone(:returning=>values)
    end

    # Returns a copy of the dataset with the order reversed. If no order is
    # given, the existing order is inverted.
    #
    #   DB[:items].reverse(:id) # SELECT * FROM items ORDER BY id DESC
    #   DB[:items].reverse{foo(bar)} # SELECT * FROM items ORDER BY foo(bar) DESC
    #   DB[:items].order(:id).reverse # SELECT * FROM items ORDER BY id DESC
    #   DB[:items].order(:id).reverse(Sequel.desc(:name)) # SELECT * FROM items ORDER BY name ASC
    def reverse(*order, &block)
      virtual_row_columns(order, block)
      order(*invert_order(order.empty? ? @opts[:order] : order))
    end

    # Alias of +reverse+
    def reverse_order(*order, &block)
      reverse(*order, &block)
    end

    # Returns a copy of the dataset with the columns selected changed
    # to the given columns. This also takes a virtual row block,
    # similar to +filter+.
    #
    #   DB[:items].select(:a) # SELECT a FROM items
    #   DB[:items].select(:a, :b) # SELECT a, b FROM items
    #   DB[:items].select{[a, sum(b)]} # SELECT a, sum(b) FROM items
    def select(*columns, &block)
      virtual_row_columns(columns, block)
      m = []
      columns.each do |i|
        if i.is_a?(Hash)
          Sequel::Deprecation.deprecate('Dataset#select will no longer treat an input hash as an alias specifier.  Switch to aliasing using Sequel.as or use the hash_aliases extension.')
          m.concat(i.map{|k, v| SQL::AliasedExpression.new(k,v)})
        else
          m << i
        end
      end
      clone(:select => m)
    end
    
    # Returns a copy of the dataset selecting the wildcard if no arguments
    # are given.  If arguments are given, treat them as tables and select
    # all columns (using the wildcard) from each table.
    #
    #   DB[:items].select(:a).select_all # SELECT * FROM items
    #   DB[:items].select_all(:items) # SELECT items.* FROM items
    #   DB[:items].select_all(:items, :foo) # SELECT items.*, foo.* FROM items
    def select_all(*tables)
      if tables.empty?
        clone(:select => nil)
      else
        select(*tables.map{|t| i, a = split_alias(t); a || i}.map{|t| SQL::ColumnAll.new(t)})
      end
    end
    
    # Returns a copy of the dataset with the given columns added
    # to the existing selected columns.  If no columns are currently selected,
    # it will select the columns given in addition to *.
    #
    #   DB[:items].select(:a).select(:b) # SELECT b FROM items
    #   DB[:items].select(:a).select_append(:b) # SELECT a, b FROM items
    #   DB[:items].select_append(:b) # SELECT *, b FROM items
    def select_append(*columns, &block)
      cur_sel = @opts[:select]
      if !cur_sel || cur_sel.empty?
        unless supports_select_all_and_column?
          return select_all(*(Array(@opts[:from]) + Array(@opts[:join]))).select_more(*columns, &block)
        end
        cur_sel = [WILDCARD]
      end
      select(*(cur_sel + columns), &block)
    end

    # Set both the select and group clauses with the given +columns+.
    # Column aliases may be supplied, and will be included in the select clause.
    # This also takes a virtual row block similar to +filter+.
    #
    #   DB[:items].select_group(:a, :b)
    #   # SELECT a, b FROM items GROUP BY a, b
    #
    #   DB[:items].select_group(:c___a){f(c2)}
    #   # SELECT c AS a, f(c2) FROM items GROUP BY c, f(c2)
    def select_group(*columns, &block)
      virtual_row_columns(columns, block)
      select(*columns).group(*columns.map{|c| unaliased_identifier(c)})
    end

    # Returns a copy of the dataset with the given columns added
    # to the existing selected columns. If no columns are currently selected
    # it will just select the columns given. 
    #
    #   DB[:items].select(:a).select(:b) # SELECT b FROM items
    #   DB[:items].select(:a).select_more(:b) # SELECT a, b FROM items
    #   DB[:items].select_more(:b) # SELECT b FROM items
    def select_more(*columns, &block)
      if @opts[:select]
        columns = @opts[:select] + columns
      else
        Sequel::Deprecation.deprecate('Dataset#select_more will no longer remove the wildcard selection from the Dataset starting in Sequel 4.  Switch to using Dataset#select if you want that behavior.')
      end
      select(*columns, &block)
    end
    
    # Set the server for this dataset to use.  Used to pick a specific database
    # shard to run a query against, or to override the default (where SELECT uses
    # :read_only database and all other queries use the :default database).  This
    # method is always available but is only useful when database sharding is being
    # used.
    #
    #   DB[:items].all # Uses the :read_only or :default server 
    #   DB[:items].delete # Uses the :default server
    #   DB[:items].server(:blah).delete # Uses the :blah server
    def server(servr)
      clone(:server=>servr)
    end

    # Set the default values for insert and update statements.  The values hash passed
    # to insert or update are merged into this hash, so any values in the hash passed
    # to insert or update will override values passed to this method.  
    #
    #   DB[:items].set_defaults(:a=>'a', :c=>'c').insert(:a=>'d', :b=>'b')
    #   # INSERT INTO items (a, c, b) VALUES ('d', 'c', 'b')
    def set_defaults(hash)
      clone(:defaults=>(@opts[:defaults]||{}).merge(hash))
    end

    # Set values that override hash arguments given to insert and update statements.
    # This hash is merged into the hash provided to insert or update, so values
    # will override any values given in the insert/update hashes.
    #
    #   DB[:items].set_overrides(:a=>'a', :c=>'c').insert(:a=>'d', :b=>'b')
    #   # INSERT INTO items (a, c, b) VALUES ('a', 'c', 'b')
    def set_overrides(hash)
      clone(:overrides=>hash.merge(@opts[:overrides]||{}))
    end
    
    # Unbind bound variables from this dataset's filter and return an array of two
    # objects.  The first object is a modified dataset where the filter has been
    # replaced with one that uses bound variable placeholders.  The second object
    # is the hash of unbound variables.  You can then prepare and execute (or just
    # call) the dataset with the bound variables to get results.
    #
    #   ds, bv = DB[:items].filter(:a=>1).unbind
    #   ds # SELECT * FROM items WHERE (a = $a)
    #   bv #  {:a => 1}
    #   ds.call(:select, bv)
    def unbind
      u = Unbinder.new
      ds = clone(:where=>u.transform(opts[:where]), :join=>u.transform(opts[:join]))
      [ds, u.binds]
    end

    # Returns a copy of the dataset with no filters (HAVING or WHERE clause) applied.
    # 
    #   DB[:items].group(:a).having(:a=>1).where(:b).unfiltered
    #   # SELECT * FROM items GROUP BY a
    def unfiltered
      clone(:where => nil, :having => nil)
    end

    # Returns a copy of the dataset with no grouping (GROUP or HAVING clause) applied.
    # 
    #   DB[:items].group(:a).having(:a=>1).where(:b).ungrouped
    #   # SELECT * FROM items WHERE b
    def ungrouped
      clone(:group => nil, :having => nil)
    end

    # Adds a UNION clause using a second dataset object.
    # A UNION compound dataset returns all rows in either the current dataset
    # or the given dataset.
    # Options:
    # :alias :: Use the given value as the from_self alias
    # :all :: Set to true to use UNION ALL instead of UNION, so duplicate rows can occur
    # :from_self :: Set to false to not wrap the returned dataset in a from_self, use with care.
    #
    #   DB[:items].union(DB[:other_items])
    #   # SELECT * FROM (SELECT * FROM items UNION SELECT * FROM other_items) AS t1
    #
    #   DB[:items].union(DB[:other_items], :all=>true, :from_self=>false)
    #   # SELECT * FROM items UNION ALL SELECT * FROM other_items
    #
    #   DB[:items].union(DB[:other_items], :alias=>:i)
    #   # SELECT * FROM (SELECT * FROM items UNION SELECT * FROM other_items) AS i
    def union(dataset, opts={})
      opts = {:all=>opts} unless opts.is_a?(Hash)
      compound_clone(:union, dataset, opts)
    end
    
    # Returns a copy of the dataset with no limit or offset.
    # 
    #   DB[:items].limit(10, 20).unlimited # SELECT * FROM items
    def unlimited
      clone(:limit=>nil, :offset=>nil)
    end

    # Returns a copy of the dataset with no order.
    # 
    #   DB[:items].order(:a).unordered # SELECT * FROM items
    def unordered
      order(nil)
    end
    
    # Add a condition to the WHERE clause.  See +filter+ for argument types.
    #
    #   DB[:items].group(:a).having(:a).filter(:b)
    #   # SELECT * FROM items GROUP BY a HAVING a AND b
    #
    #   DB[:items].group(:a).having(:a).where(:b)
    #   # SELECT * FROM items WHERE b GROUP BY a HAVING a
    def where(*cond, &block)
      _filter(:where, *cond, &block)
    end
    
    # Add a common table expression (CTE) with the given name and a dataset that defines the CTE.
    # A common table expression acts as an inline view for the query.
    # Options:
    # :args :: Specify the arguments/columns for the CTE, should be an array of symbols.
    # :recursive :: Specify that this is a recursive CTE
    #
    #   DB[:items].with(:items, DB[:syx].filter(:name.like('A%')))
    #   # WITH items AS (SELECT * FROM syx WHERE (name LIKE 'A%')) SELECT * FROM items
    def with(name, dataset, opts={})
      raise(Error, 'This datatset does not support common table expressions') unless supports_cte?
      if hoist_cte?(dataset)
        s, ds = hoist_cte(dataset)
        s.with(name, ds, opts)
      else
        clone(:with=>(@opts[:with]||[]) + [opts.merge(:name=>name, :dataset=>dataset)])
      end
    end

    # Add a recursive common table expression (CTE) with the given name, a dataset that
    # defines the nonrecursive part of the CTE, and a dataset that defines the recursive part
    # of the CTE.  Options:
    # :args :: Specify the arguments/columns for the CTE, should be an array of symbols.
    # :union_all :: Set to false to use UNION instead of UNION ALL combining the nonrecursive and recursive parts.
    #
    #   DB[:t].with_recursive(:t,
    #     DB[:i1].select(:id, :parent_id).filter(:parent_id=>nil),
    #     DB[:i1].join(:t, :id=>:parent_id).select(:i1__id, :i1__parent_id),
    #     :args=>[:id, :parent_id])
    #   
    #   # WITH RECURSIVE "t"("id", "parent_id") AS (
    #   #   SELECT "id", "parent_id" FROM "i1" WHERE ("parent_id" IS NULL)
    #   #   UNION ALL
    #   #   SELECT "i1"."id", "i1"."parent_id" FROM "i1" INNER JOIN "t" ON ("t"."id" = "i1"."parent_id")
    #   # ) SELECT * FROM "t"
    def with_recursive(name, nonrecursive, recursive, opts={})
      raise(Error, 'This datatset does not support common table expressions') unless supports_cte?
      if hoist_cte?(nonrecursive)
        s, ds = hoist_cte(nonrecursive)
        s.with_recursive(name, ds, recursive, opts)
      elsif hoist_cte?(recursive)
        s, ds = hoist_cte(recursive)
        s.with_recursive(name, nonrecursive, ds, opts)
      else
        clone(:with=>(@opts[:with]||[]) + [opts.merge(:recursive=>true, :name=>name, :dataset=>nonrecursive.union(recursive, {:all=>opts[:union_all] != false, :from_self=>false}))])
      end
    end
    
    # Returns a copy of the dataset with the static SQL used.  This is useful if you want
    # to keep the same row_proc/graph, but change the SQL used to custom SQL.
    #
    #   DB[:items].with_sql('SELECT * FROM foo') # SELECT * FROM foo
    #
    # You can use placeholders in your SQL and provide arguments for those placeholders:
    #
    #   DB[:items].with_sql('SELECT ? FROM foo', 1) # SELECT 1 FROM foo
    #
    # You can also provide a method name and arguments to call to get the SQL:
    #
    #   DB[:items].with_sql(:insert_sql, :b=>1) # INSERT INTO items (b) VALUES (1)
    def with_sql(sql, *args)
      if sql.is_a?(Symbol)
        sql = send(sql, *args)
      else
        sql = SQL::PlaceholderLiteralString.new(sql, args) unless args.empty?
      end
      clone(:sql=>sql)
    end
    
    protected

    # Add the dataset to the list of compounds
    def compound_clone(type, dataset, opts)
      if hoist_cte?(dataset)
        s, ds = hoist_cte(dataset)
        return s.compound_clone(type, ds, opts)
      end
      ds = compound_from_self.clone(:compounds=>Array(@opts[:compounds]).map{|x| x.dup} + [[type, dataset.compound_from_self, opts[:all]]])
      opts[:from_self] == false ? ds : ds.from_self(opts)
    end

    # Return true if the dataset has a non-nil value for any key in opts.
    def options_overlap(opts)
      !(@opts.collect{|k,v| k unless v.nil?}.compact & opts).empty?
    end

    # Whether this dataset is a simple SELECT * FROM table.
    def simple_select_all?
      o = @opts.reject{|k,v| v.nil? || NON_SQL_OPTIONS.include?(k)}
      o.length == 1 && (f = o[:from]) && f.length == 1 && (f.first.is_a?(Symbol) || f.first.is_a?(SQL::AliasedExpression))
    end

    private

    # Internal filter/exclude method so it works on either the having or where clauses.
    def _filter_or_exclude(invert, clause, *cond, &block)
      cond = cond.first if cond.size == 1
      if cond.respond_to?(:empty?) && cond.empty? && !block
        clone
      else
        cond = filter_expr(cond, &block)
        cond = SQL::BooleanExpression.invert(cond) if invert
        cond = SQL::BooleanExpression.new(:AND, @opts[clause], cond) if @opts[clause]
        clone(clause => cond)
      end
    end

    # Internal filter method so it works on either the having or where clauses.
    def _filter(clause, *cond, &block)
      _filter_or_exclude(false, clause, *cond, &block)
    end

    # The default :qualify option to use for join tables if one is not specified.
    def default_join_table_qualification
      :symbol
    end
    
    # SQL expression object based on the expr type.  See +filter+.
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
        if supports_where_true?
          SQL::BooleanExpression.new(:NOOP, expr)
        elsif expr
          SQL::Constants::SQLTRUE
        else
          SQL::Constants::SQLFALSE
        end
      when String
        LiteralString.new("(#{expr})")
      else
        raise(Error, "Invalid filter argument: #{expr.inspect}")
      end
    end
    
    # Return two datasets, the first a clone of the receiver with the WITH
    # clause from the given dataset added to it, and the second a clone of
    # the given dataset with the WITH clause removed.
    def hoist_cte(ds)
      [clone(:with => (opts[:with] || []) + ds.opts[:with]), ds.clone(:with => nil)]
    end

    # Whether CTEs need to be hoisted from the given ds into the current ds.
    def hoist_cte?(ds)
      ds.is_a?(Dataset) && ds.opts[:with] && !supports_cte_in_subqueries?
    end

    # Inverts the given order by breaking it into a list of column references
    # and inverting them.
    #
    #   DB[:items].invert_order([Sequel.desc(:id)]]) #=> [Sequel.asc(:id)]
    #   DB[:items].invert_order([:category, Sequel.desc(:price)]) #=> [Sequel.desc(:category), Sequel.asc(:price)]
    def invert_order(order)
      return nil unless order
      order.map do |f|
        case f
        when SQL::OrderedExpression
          f.invert
        else
          SQL::OrderedExpression.new(f)
        end
      end
    end

    # Return self if the dataset already has a server, or a cloned dataset with the
    # default server otherwise.
    def default_server
      @opts[:server] ? self : clone(:server=>:default)
    end

    # Treat the +block+ as a virtual_row block if not +nil+ and
    # add the resulting columns to the +columns+ array (modifies +columns+).
    def virtual_row_columns(columns, block)
      if block
        v = Sequel.virtual_row(&block)
        if v.is_a?(Array)
          columns.concat(v)
        else
          columns << v
        end
      end
    end
  end
end
