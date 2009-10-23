module Sequel
  class Dataset

    FROM_SELF_KEEP_OPTS = [:graph, :eager_graph, :graph_aliases]

    # Adds an further filter to an existing filter using AND. If no filter 
    # exists an error is raised. This method is identical to #filter except
    # it expects an existing filter.
    #
    #   ds.filter(:a).and(:b) # SQL: WHERE a AND b
    def and(*cond, &block)
      raise(InvalidOperation, "No existing filter found.") unless @opts[:having] || @opts[:where]
      filter(*cond, &block)
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
    # See doc/dataset_filtering.rdoc for more examples and details.
    def filter(*cond, &block)
      _filter(@opts[:having] ? :having : :where, *cond, &block)
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
    #   ds.from_self.sql               #=> "SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS 't1'"
    #   ds.from_self(:alias=>:foo).sql #=> "SELECT * FROM (SELECT id, name FROM items ORDER BY name) AS 'foo'"
    def from_self(opts={})
      fs = {}
      @opts.keys.each{|k| fs[k] = nil unless FROM_SELF_KEEP_OPTS.include?(k)}
      clone(fs).from(opts[:alias] ? as(opts[:alias]) : self)
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
      clone(:group => (columns.compact.empty? ? nil : columns))
    end
    alias group_by group

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
    alias_method :order_by, :order
    
    # Returns a copy of the dataset with the order columns added
    # to the existing order.
    #
    #   ds.order(:a).order(:b).sql #=> 'SELECT * FROM items ORDER BY b'
    #   ds.order(:a).order_more(:b).sql #=> 'SELECT * FROM items ORDER BY a, b'
    def order_more(*columns, &block)
      columns = @opts[:order] + columns if @opts[:order]
      order(*columns, &block)
    end
    
    # Returns a copy of the dataset with the order reversed. If no order is
    # given, the existing order is inverted.
    def reverse_order(*order)
      order(*invert_order(order.empty? ? @opts[:order] : order))
    end
    alias reverse reverse_order

    # Returns a copy of the dataset with the columns selected changed
    # to the given columns. This also takes a virtual row block,
    # similar to filter.
    #
    #   dataset.select(:a) # SELECT a FROM items
    #   dataset.select(:a, :b) # SELECT a, b FROM items
    #   dataset.select{|o| o.a, o.sum(:b)} # SELECT a, sum(b) FROM items
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
    # to the existing selected columns.
    #
    #   dataset.select(:a).select(:b) # SELECT b FROM items
    #   dataset.select(:a).select_more(:b) # SELECT a, b FROM items
    def select_more(*columns, &block)
      columns = @opts[:select] + columns if @opts[:select]
      select(*columns, &block)
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

    private

    # Internal filter method so it works on either the having or where clauses.
    def _filter(clause, *cond, &block)
      cond = cond.first if cond.size == 1
      cond = filter_expr(cond, &block)
      cond = SQL::BooleanExpression.new(:AND, @opts[clause], cond) if @opts[clause]
      clone(clause => cond)
    end
    
    # Add the dataset to the list of compounds
    def compound_clone(type, dataset, opts)
      ds = compound_from_self.clone(:compounds=>Array(@opts[:compounds]).map{|x| x.dup} + [[type, dataset.compound_from_self, opts[:all]]])
      opts[:from_self] == false ? ds : ds.from_self
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
