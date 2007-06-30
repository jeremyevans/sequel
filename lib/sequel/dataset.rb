require 'time'
require 'date'

module Sequel
  # A Dataset represents a view of a the data in a database, constrained by
  # specific parameters such as filtering conditions, order, etc. Datasets
  # can be used to create, retrieve, update and delete records.
  # 
  # Query results are always retrieved on demand, so a dataset can be kept
  # around and reused indefinitely:
  #   my_posts = DB[:posts].filter(:author => 'david') # no records are retrieved
  #   p my_posts.all # records are now retrieved
  #   ...
  #   p my_posts.all # records are retrieved again
  #
  # In order to provide this functionality, dataset methods such as where, 
  # select, order, etc. return modified copies of the dataset, so you can
  # use different datasets to access data:
  #   posts = DB[:posts]
  #   davids_posts = posts.filter(:author => 'david')
  #   old_posts = posts.filter('stamp < ?', 1.week.ago)
  #
  # Datasets are Enumerable objects, so they can be manipulated using any
  # of the Enumerable methods, such as map, inject, etc.
  class Dataset
    include Enumerable
    
    attr_reader :db, :opts
    attr_accessor :model_class
  
    # Constructs a new instance of a dataset with a database instance, initial
    # options and an optional record class. Datasets are usually constructed by
    # invoking Database methods:
    #   DB[:posts]
    # Or:
    #   DB.dataset # the returned dataset is blank
    #
    # Sequel::Dataset is an abstract class that is not useful by itself. Each
    # database adaptor should provide a descendant class of Sequel::Dataset.
    def initialize(db, opts = nil, model_class = nil)
      @db = db
      @opts = opts || {}
      @model_class = model_class
    end
    
    # Returns a new instance of the dataset with with the give options merged.
    def dup_merge(opts)
      self.class.new(@db, @opts.merge(opts), @model_class)
    end
    
    # Returns a dataset that fetches records as hashes (instead of model 
    # objects). If no record class is defined for the dataset, self is
    # returned.
    def naked
      @model_class ? self.class.new(@db, opts || @opts.dup) : self
    end
    
    # Returns a valid SQL fieldname as a string. Field names specified as 
    # symbols can include double underscores to denote a dot separator, e.g.
    # :posts__id will be converted into posts.id.
    def field_name(field)
      field.is_a?(Symbol) ? field.to_field_name : field
    end
    
    QUALIFIED_REGEXP = /(.*)\.(.*)/.freeze

    # Returns a qualified field name (including a table name) if the field
    # name isn't already qualified.
    def qualified_field_name(field, table)
      fn = field_name(field)
      fn =~ QUALIFIED_REGEXP ? fn : "#{table}.#{fn}"
    end
    
    WILDCARD = '*'.freeze
    COMMA_SEPARATOR = ", ".freeze
    
    # Converts an array of field names into a comma seperated string of 
    # field names. If the array is empty, a wildcard (*) is returned.
    def field_list(fields)
      if fields.empty?
        WILDCARD
      else
        fields.map {|i| field_name(i)}.join(COMMA_SEPARATOR)
      end
    end
    
    # Converts an array of sources names into into a comma separated list.
    def source_list(source)
      if source.nil? || source.empty?
        raise SequelError, 'No source specified for query'
      end
      source.map {|i| i.is_a?(Dataset) ? i.to_table_reference : i}.
        join(COMMA_SEPARATOR)
    end
    
    NULL = "NULL".freeze
    TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S'".freeze
    DATE_FORMAT = "DATE '%Y-%m-%d'".freeze
    
    # Returns a literal representation of a value to be used as part
    # of an SQL expression. The stock implementation supports literalization 
    # of String (with proper escaping to prevent SQL injections), numbers,
    # Symbol (as field references), Array (as a list of literalized values),
    # Time (as an SQL TIMESTAMP), Date (as an SQL DATE), Dataset (as a 
    # subquery) and nil (AS NULL).
    # 
    #   dataset.literal("abc'def") #=> "'abc''def'"
    #   dataset.literal(:items__id) #=> "items.id"
    #   dataset.literal([1, 2, 3]) => "(1, 2, 3)"
    #   dataset.literal(DB[:items]) => "(SELECT * FROM items)"
    #
    # If an unsupported object is given, an exception is raised.
    def literal(v)
      case v
      when ExpressionString: v
      when String: "'#{v.gsub(/'/, "''")}'"
      when Integer, Float: v.to_s
      when NilClass: NULL
      when Symbol: v.to_field_name
      when Array: v.empty? ? NULL : v.map {|i| literal(i)}.join(COMMA_SEPARATOR)
      when Time: v.strftime(TIMESTAMP_FORMAT)
      when Date: v.strftime(DATE_FORMAT)
      when Dataset: "(#{v.sql})"
      else
        raise SequelError, "can't express #{v.inspect} as a SQL literal"
      end
    end

    AND_SEPARATOR = " AND ".freeze
    
    # Formats an equality expression involving a left value and a right value.
    # Equality expressions differ according to the class of the right value.
    # The stock implementation supports Range (inclusive and exclusive), Array
    # (as a list of values to compare against), Dataset (as a subquery to
    # compare against), or a regular value.
    #
    #   dataset.format_eq_expression('id', 1..20) #=>
    #     "(id >= 1 AND id <= 20)"
    #   dataset.format_eq_expression('id', [3,6,10]) #=>
    #     "(id IN (3, 6, 10))"
    #   dataset.format_eq_expression('id', DB[:items].select(:id)) #=>
    #     "(id IN (SELECT id FROM items))"
    #   dataset.format_eq_expression('id', nil) #=>
    #     "(id IS NULL)"
    #   dataset.format_eq_expression('id', 3) #=>
    #     "(id = 3)"
    def format_eq_expression(left, right)
      case right
      when Range:
        right.exclude_end? ? \
          "(#{left} >= #{right.begin} AND #{left} < #{right.end})" : \
          "(#{left} >= #{right.begin} AND #{left} <= #{right.end})"
      when Array:
        "(#{left} IN (#{literal(right)}))"
      when Dataset:
        "(#{left} IN (#{right.sql}))"
      when NilClass:
        "(#{left} IS NULL)"
      else
        "(#{left} = #{literal(right)})"
      end
    end
    
    # Formats an expression comprising a left value, a binary operator and a
    # right value. The supported operators are :eql (=), :not (!=), :lt (<),
    # :lte (<=), :gt (>), :gte (>=) and :like (LIKE operator). Examples:
    #
    #   dataset.format_expression('price', :gte, 100) #=> "(price >= 100)"
    #   dataset.format_expression('id', :not, 30) #=> "NOT (id = 30)"
    #   dataset.format_expression('name', :like, 'abc%') #=>
    #     "(name LIKE 'abc%')"
    #
    # If an unsupported operator is given, an exception is raised.
    def format_expression(left, op, right)
      left = field_name(left)
      case op
      when :eql:
        format_eq_expression(left, right)
      when :not:
        "NOT #{format_eq_expression(left, right)}"
      when :lt:
        "(#{left} < #{literal(right)})"
      when :lte:
        "(#{left} <= #{literal(right)})"
      when :gt:
        "(#{left} > #{literal(right)})"
      when :gte:
        "(#{left} >= #{literal(right)})"
      when :like:
        "(#{left} LIKE #{literal(right)})"
      else
        raise SequelError, "Invalid operator specified: #{op}"
      end
    end
    
    QUESTION_MARK = '?'.freeze
    
    # Formats a where clause. If parenthesize is true, then the whole 
    # generated clause will be enclosed in a set of parentheses.
    def expression_list(where, parenthesize = false)
      case where
      when Hash:
        parenthesize = false if where.size == 1
        fmt = where.map {|i| format_expression(i[0], :eql, i[1])}.
          join(AND_SEPARATOR)
      when Array:
        fmt = where.shift.gsub(QUESTION_MARK) {literal(where.shift)}
      when Proc:
        fmt = where.to_expressions.map {|e| format_expression(e.left, e.op, e.right)}.
          join(AND_SEPARATOR)
      else
        # if the expression is compound, it should be parenthesized in order for 
        # things to be predictable (when using #or and #and.)
        parenthesize |= where =~ /\).+\(/
        fmt = where
      end
      parenthesize ? "(#{fmt})" : fmt
    end
    
    # Returns a copy of the dataset with the source changed.
    def from(*source)
      dup_merge(:from => source)
    end
    
    # Returns a copy of the dataset with the selected fields changed.
    def select(*fields)
      dup_merge(:select => fields)
    end

    # Returns a copy of the dataset with the distinct option.
    def uniq
      dup_merge(:distinct => true)
    end
    alias distinct uniq

    # Returns a copy of the dataset with the order changed.
    def order(*order)
      dup_merge(:order => order)
    end
    
    # Returns a copy of the dataset with the order reversed. If no order is
    # given, the existing order is inverted.
    def reverse_order(*order)
      order(invert_order(order.empty? ? @opts[:order] : order))
    end
    
    DESC_ORDER_REGEXP = /(.*)\sDESC/.freeze
    
    # Inverts the given order by breaking it into a list of field references
    # and inverting them.
    #
    #   dataset.invert_order('id DESC') #=> "id"
    #   dataset.invert_order('category, price DESC') #=>
    #     "category DESC, price"
    def invert_order(order)
      new_order = []
      order.each do |f|
        f.to_s.split(',').map do |p|
          p.strip!
          new_order << (p =~ DESC_ORDER_REGEXP ? $1 : p.to_sym.DESC)
        end
      end
      new_order
    end
    
    # Returns a copy of the dataset with the results grouped by the value of 
    # the given fields
    def group(*fields)
      dup_merge(:group => fields)
    end

    # Returns a copy of the dataset with the given conditions imposed upon it.  
    # If the query has been grouped, then the conditions are imposed in the 
    # HAVING clause. If not, then they are imposed in the WHERE clause. Filter
    # accepts a Hash (formated into a list of equality expressions), an Array
    # (formatted ala ActiveRecord conditions), a String (taken literally), or
    # a block that is converted into expressions.
    #
    #   dataset.filter(:id => 3).sql #=>
    #     "SELECT * FROM items WHERE (id = 3)"
    #   dataset.filter('price < ?', 100).sql #=>
    #     "SELECT * FROM items WHERE price < 100"
    #   dataset.filter('price < 100').sql #=>
    #     "SELECT * FROM items WHERE price < 100"
    #   dataset.filter {price < 100}.sql #=>
    #     "SELECT * FROM items WHERE (price < 100)"
    # 
    # Multiple filter calls can be chained for scoping:
    #
    #   software = dataset.filter(:category => 'software')
    #   software.filter {price < 100}.sql #=>
    #     "SELECT * FROM items WHERE (category = 'software') AND (price < 100)"
    def filter(*cond, &block)
      clause = (@opts[:group] ? :having : :where)
      cond = cond.first if cond.size == 1
      parenthesize = !(cond.is_a?(Hash) || cond.is_a?(Array))
      filter = cond.is_a?(Hash) && cond
      if @opts[clause]
        if filter && cond.is_a?(Hash)
          filter
        end
        filter = 
        l = expression_list(@opts[clause])
        r = expression_list(block || cond, parenthesize)
        dup_merge(clause => "#{l} AND #{r}")
      else
        dup_merge(:filter => cond, clause => expression_list(block || cond))
      end
    end
    
    # Adds an alternate filter to an existing filter using OR. If no filter 
    # exists an error is raised.
    def or(*cond, &block)
      clause = (@opts[:group] ? :having : :where)
      cond = cond.first if cond.size == 1
      parenthesize = !(cond.is_a?(Hash) || cond.is_a?(Array))
      if @opts[clause]
        l = expression_list(@opts[clause])
        r = expression_list(block || cond, parenthesize)
        dup_merge(clause => "#{l} OR #{r}")
      else
        raise SequelError, "No existing filter found."
      end
    end

    # Adds an further filter to an existing filter using AND. If no filter 
    # exists an error is raised. This method is identical to #filter except
    # it expects an existing filter.
    def and(*cond, &block)
      clause = (@opts[:group] ? :having : :where)
      unless @opts[clause]
        raise SequelError, "No existing filter found."
      end
      filter(*cond, &block)
    end

    # Performs the inverse of Dataset#filter.
    #
    #   dataset.exclude(:category => 'software').sql #=>
    #     "SELECT * FROM items WHERE NOT (category = 'software')"
    def exclude(*cond, &block)
      clause = (@opts[:group] ? :having : :where)
      cond = cond.first if cond.size == 1
      parenthesize = !(cond.is_a?(Hash) || cond.is_a?(Array))
      if @opts[clause]
        l = expression_list(@opts[clause])
        r = expression_list(block || cond, parenthesize)
        cond = "#{l} AND NOT #{r}"
      else
        cond = "NOT #{expression_list(block || cond, true)}"
      end
      dup_merge(clause => cond)
    end
    
    # Returns a copy of the dataset with the where conditions changed. Raises 
    # if the dataset has been grouped. See also #filter.
    def where(*cond, &block)
      if @opts[:group]
        raise SequelError, "Can't specify a WHERE clause once the dataset has been grouped"
      else
        filter(*cond, &block)
      end
    end

    # Returns a copy of the dataset with the having conditions changed. Raises 
    # if the dataset has not been grouped. See also #filter
    def having(*cond, &block)
      unless @opts[:group]
        raise SequelError, "Can only specify a HAVING clause on a grouped dataset"
      else
        filter(*cond, &block)
      end
    end
    
    # Adds a UNION clause using a second dataset object. If all is true the
    # clause used is UNION ALL, which may return duplicate rows.
    def union(dataset, all = false)
      dup_merge(:union => dataset, :union_all => all)
    end

    # Adds an INTERSECT clause using a second dataset object. If all is true 
    # the clause used is INTERSECT ALL, which may return duplicate rows.
    def intersect(dataset, all = false)
      dup_merge(:intersect => dataset, :intersect_all => all)
    end

    # Adds an EXCEPT clause using a second dataset object. If all is true the
    # clause used is EXCEPT ALL, which may return duplicate rows.
    def except(dataset, all = false)
      dup_merge(:except => dataset, :except_all => all)
    end
    
    JOIN_TYPES = {
      :left_outer => 'LEFT OUTER JOIN'.freeze,
      :right_outer => 'RIGHT OUTER JOIN'.freeze,
      :full_outer => 'FULL OUTER JOIN'.freeze,
      :inner => 'INNER JOIN'.freeze
    }
    
    def join_expr(type, table, expr)
      join_type = JOIN_TYPES[type || :inner]
      unless join_type
        raise SequelError, "Invalid join type: #{type}"
      end
      
      join_expr = expr.map do |k, v|
        l = qualified_field_name(k, table)
        r = qualified_field_name(v, @opts[:last_joined_table] || @opts[:from])
        "(#{l} = #{r})"
      end.join(AND_SEPARATOR)
      
      " #{join_type} #{table} ON #{join_expr}"
    end
    
    # Returns a joined dataset.
    def join_table(type, table, expr)
      unless expr.is_a?(Hash)
        expr = {expr => :id}
      end
      clause = join_expr(type, table, expr)
      join = @opts[:join] ? @opts[:join] + clause : clause
      dup_merge(:join => join, :last_joined_table => table)
    end
    
    def left_outer_join(table, expr); join_table(:left_outer, table, expr); end
    def right_outer_join(table, expr); join_table(:right_outer, table, expr); end
    def full_outer_join(table, expr); join_table(:full_outer, table, expr); end
    def inner_join(table, expr); join_table(:inner, table, expr); end
    alias_method :join, :inner_join

    alias_method :all, :to_a
    
    # Maps field values for each record in the dataset (if a field name is
    # given), or performs the stock mapping functionality of Enumerable.
    def map(field_name = nil, &block)
      if field_name
        super() {|r| r[field_name]}
      else
        super(&block)
      end
    end
    
    # Returns a hash with one column used as key and another used as value.
    def hash_column(key_column, value_column)
      inject({}) do |m, r|
        m[r[key_column]] = r[value_column]
        m
      end
    end
    
    # Inserts the given values into the table.
    def <<(values)
      insert(values)
    end
    
    # Inserts multiple values. If a block is given it is invoked for each
    # item in the given array before inserting it.
    def insert_multiple(array, &block)
      if block
        array.each {|i| insert(block[i])}
      else
        array.each {|i| insert(i)}
      end
    end

    EMPTY = ''.freeze
    SPACE = ' '.freeze
    
    # Formats a SELECT statement using the given options and the dataset
    # options.
    def select_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      fields = opts[:select]
      select_fields = fields ? field_list(fields) : WILDCARD
      select_source = source_list(opts[:from])
      sql = opts[:distinct] ? \
        "SELECT DISTINCT #{select_fields} FROM #{select_source}" : \
        "SELECT #{select_fields} FROM #{select_source}"
      
      if join = opts[:join]
        sql << join
      end
      
      if where = opts[:where]
        sql << " WHERE #{where}"
      end
      
      if group = opts[:group]
        sql << " GROUP BY #{field_list(group)}"
      end

      if order = opts[:order]
        sql << " ORDER BY #{field_list(order)}"
      end
      
      if having = opts[:having]
        sql << " HAVING #{having}"
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
    alias sql select_sql

    # Formats an INSERT statement using the given values. If a hash is given,
    # the resulting statement includes field names. If no values are given, 
    # the resulting statement includes a DEFAULT VALUES clause.
    #
    #   dataset.insert_sql() #=> 'INSERT INTO items DEFAULT VALUES'
    #   dataset.insert_sql(1,2,3) #=> 'INSERT INTO items VALUES (1, 2, 3)'
    #   dataset.insert_sql(:a => 1, :b => 2) #=>
    #     'INSERT INTO items (a, b) VALUES (1, 2)'
    def insert_sql(*values)
      if values.empty?
        "INSERT INTO #{@opts[:from]} DEFAULT VALUES"
      elsif (values.size == 1) && values[0].is_a?(Hash)
        field_list = []
        value_list = []
        values[0].each do |k, v|
          field_list << k
          value_list << literal(v)
        end
        fl = field_list.join(COMMA_SEPARATOR)
        vl = value_list.join(COMMA_SEPARATOR)
        "INSERT INTO #{@opts[:from]} (#{fl}) VALUES (#{vl})"
      else
        "INSERT INTO #{@opts[:from]} VALUES (#{literal(values)})"
      end
    end
    
    # Formats an UPDATE statement using the given values.
    #
    #   dataset.update_sql(:price => 100, :category => 'software') #=>
    #     "UPDATE items SET price = 100, category = 'software'"
    def update_sql(values, opts = nil)
      opts = opts ? @opts.merge(opts) : @opts
      
      if opts[:group]
        raise SequelError, "Can't update a grouped dataset" 
      elsif (opts[:from].size > 1) or opts[:join]
        raise SequelError, "Can't update a joined dataset"
      end

      set_list = values.map {|k, v| "#{k} = #{literal(v)}"}.
        join(COMMA_SEPARATOR)
      sql = "UPDATE #{@opts[:from]} SET #{set_list}"
      
      if where = opts[:where]
        sql << " WHERE #{where}"
      end

      sql
    end
    
    # Formats a DELETE statement using the given options and dataset options.
    # 
    #   dataset.filter {price >= 100}.delete_sql #=>
    #     "DELETE FROM items WHERE (price >= 100)"
    def delete_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      if opts[:group]
        raise SequelError, "Can't delete from a grouped dataset"
      elsif opts[:from].is_a?(Array) && opts[:from].size > 1
        raise SequelError, "Can't delete from a joined dataset"
      end

      sql = "DELETE FROM #{opts[:from]}"

      if where = opts[:where]
        sql << " WHERE #{where}"
      end

      sql
    end
    
    # Returns the first record in the dataset.
    def single_record(opts = nil)
      each(opts) {|r| return r}
      nil
    end
    
    # Returns the first value of the first reecord in the dataset.
    def single_value(opts = nil)
      naked.each(opts) {|r| return r.values.first}
    end
    
    SELECT_COUNT = {:select => ["COUNT(*)"], :order => nil}.freeze
    
    # Returns the number of records in the dataset.
    def count
      single_value(SELECT_COUNT).to_i
    end
    alias size count
    
    # returns a paginated dataset. The resulting dataset also provides the
    # total number of pages (Dataset#page_count) and the current page number
    # (Dataset#current_page), as well as Dataset#prev_page and Dataset#next_page
    # for implementing pagination controls.
    def paginate(page_no, page_size)
      total_pages = (count / page_size.to_f).ceil
      paginated = limit(page_size, (page_no - 1) * page_size)
      paginated.current_page = page_no
      paginated.page_count = total_pages
      paginated
    end
    
    attr_accessor :page_count, :current_page
    
    # Returns the previous page number or nil if the current page is the first
    def prev_page
      current_page > 1 ? (current_page - 1) : nil
    end
    
    # Returns the next page number or nil if the current page is the last page
    def next_page
      current_page < page_count ? (current_page + 1) : nil
    end
    
    # Returns a table reference for use in the FROM clause. If the dataset has
    # only a :from option refering to a single table, only the table name is 
    # returned. Otherwise a subquery is returned.
    def to_table_reference
      if opts.keys == [:from] && opts[:from].size == 1
        opts[:from].first.to_s
      else
        "(#{sql})"
      end
    end
    
    # Returns the minimum value for the given field.
    def min(field)
      single_value(:select => [field.MIN])
    end
    
    # Returns the maximum value for the given field.
    def max(field)
      single_value(:select => [field.MAX])
    end

    # Returns the sum for the given field.
    def sum(field)
      single_value(:select => [field.SUM])
    end
    
    # Returns the average value for the given field.
    def avg(field)
      single_value(:select => [field.AVG])
    end
    
    # Returns an EXISTS clause for the dataset.
    #
    #   dataset.exists #=> "EXISTS (SELECT 1 FROM items)"
    def exists(opts = nil)
      "EXISTS (#{sql({:select => [1]}.merge(opts || {}))})"
    end
    
    # If given an integer, the dataset will contain only the first l results.
    # If given a range, it will contain only those at offsets within that
    # range. If a second argument is given, it is used as an offset.
    def limit(l, o = nil)
      if l.is_a? Range
        lim = (l.exclude_end? ? l.last - l.first : l.last + 1 - l.first)
        dup_merge(:limit => lim, :offset=>l.first)
      elsif o
        dup_merge(:limit => l, :offset => o)
      else
        dup_merge(:limit => l)
      end
    end
    
    # Returns the first record in the dataset. If the num argument is specified,
    # an array is returned with the first <i>num</i> records.
    def first(*args)
      args = args.empty? ? 1 : (args.size == 1) ? args.first : args
      case args
      when 1: single_record(:limit => 1)
      when Fixnum: limit(args).all
      else
        filter(args).single_record(:limit => 1)
      end
    end
    
    # Returns the first record matching the condition.
    def [](*conditions)
      first(*conditions)
    end
    
    def []=(conditions, values)
      filter(conditions).update(values)
    end
    
    # Returns the last records in the dataset by inverting the order. If no
    # order is given, an exception is raised. If num is not given, the last
    # record is returned. Otherwise an array is returned with the last 
    # <i>num</i> records.
    def last(*args)
      raise SequelError, 'No order specified' unless 
        @opts[:order] || (opts && opts[:order])
      
      args = args.empty? ? 1 : (args.size == 1) ? args.first : args
      
      case args
      when Fixnum:
        l = {:limit => args}
        opts = {:order => invert_order(@opts[:order])}. \
          merge(opts ? opts.merge(l) : l)
        if args == 1
          single_record(opts)
        else
          dup_merge(opts).all
        end
      else
        filter(args).last(1)
      end
    end
    
    # Deletes all records in the dataset one at a time by invoking the destroy
    # method of the associated model class.
    def destroy
      raise SequelError, 'Dataset not associated with model' unless @model_class
      
      count = 0
      @db.transaction {each {|r| count += 1; r.destroy}}
      count
    end
    
    # Pretty prints the records in the dataset as plain-text table.
    def print(*columns)
      Sequel::PrettyTable.print(naked.all, columns.empty? ? nil : columns)
    end
  end
end

