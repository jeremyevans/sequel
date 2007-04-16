require 'time'

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
    attr_accessor :record_class
  
    # Constructs a new instance of a dataset with a database instance, initial
    # options and an optional record class. Datasets are usually constructed by
    # invoking Database methods:
    #   DB[:posts]
    # Or:
    #   DB.dataset # the returned dataset is blank
    #
    # Sequel::Dataset is an abstract class that is not useful by itself. Each
    # database adaptor should provide a descendant class of Sequel::Dataset.
    def initialize(db, opts = nil, record_class = nil)
      @db = db
      @opts = opts || {}
      @record_class = record_class
    end
    
    # Returns a new instance of the dataset with its options
    def dup_merge(opts)
      self.class.new(@db, @opts.merge(opts), @record_class)
    end
    
    # Returns a dataset that fetches records as hashes (instead of model 
    # objects). If no record class is defined for the dataset, self is
    # returned.
    def naked
      @record_class ? self.class.new(@db, @opts.dup) : self
    end
    
    AS_REGEXP = /(.*)___(.*)/.freeze
    AS_FORMAT = "%s AS %s".freeze
    DOUBLE_UNDERSCORE = '__'.freeze
    PERIOD = '.'.freeze
    
    # Returns a valid SQL fieldname as a string. Field names specified as 
    # symbols can include double underscores to denote a dot separator, e.g.
    # :posts__id will be converted into posts.id.
    def field_name(field)
      field.is_a?(Symbol) ? field.to_field_name : field
    end
    
    QUALIFIED_REGEXP = /(.*)\.(.*)/.freeze
    QUALIFIED_FORMAT = "%s.%s".freeze

    # Returns a qualified field name (including a table name) if the field
    # name isn't already qualified.
    def qualified_field_name(field, table)
      fn = field_name(field)
      fn =~ QUALIFIED_REGEXP ? fn : QUALIFIED_FORMAT % [table, fn]
    end
    
    WILDCARD = '*'.freeze
    COMMA_SEPARATOR = ", ".freeze
    
    # Converts a field list into a comma seperated string of field names.
    def field_list(fields)
      if fields.empty?
        WILDCARD
      else
        fields.map {|i| field_name(i)}.join(COMMA_SEPARATOR)
      end
    end
    
    # Converts an array of sources into a comma separated list.
    def source_list(source)
      raise SequelError, 'No source specified for query' unless source
      source.map {|i| i.is_a?(Dataset) ? i.to_table_reference : i}.
        join(COMMA_SEPARATOR)
    end
    
    NULL = "NULL".freeze
    SUBQUERY = "(%s)".freeze
    
    # Returns a literal representation of a value to be used as part
    # of an SQL expression. This method is overriden in descendants.
    def literal(v)
      case v
      when String: "'%s'" % v.gsub(/'/, "''")
      when Integer, Float: v.to_s
      when NilClass: NULL
      when Symbol: v.to_field_name
      when Array: v.empty? ? NULL : v.map {|i| literal(i)}.join(COMMA_SEPARATOR)
      when Dataset: SUBQUERY % v.sql
      else
        raise SequelError, "can't express #{v.inspect}:#{v.class} as a SQL literal"
      end
    end
    
    AND_SEPARATOR = " AND ".freeze
    EQUAL_COND = "(%s = %s)".freeze
    IN_EXPR = "(%s IN (%s))".freeze
#    BETWEEN_EXPR = "(%s BETWEEN %s AND %s)".freeze
    INCLUSIVE_RANGE_EXPR = "(%s >= %s AND %s <= %s)".freeze
    EXCLUSIVE_RANGE_EXPR = "(%s >= %s AND %s < %s)".freeze
    NULL_EXPR = "(%s IS NULL)".freeze
    
    # Formats an equality condition SQL expression.
    def where_condition(left, right)
      left = field_name(left)
      case right
      when Range:
        (right.exclude_end? ? EXCLUSIVE_RANGE_EXPR : INCLUSIVE_RANGE_EXPR) %
          [left, literal(right.begin), left, literal(right.end)]
#        BETWEEN_EXPR % [field_name(left), literal(right.begin), literal(right.end)]
      when Array:
        IN_EXPR % [left, literal(right)]
      when NilClass:
        NULL_EXPR % left
      when Dataset:
        IN_EXPR % [left, right.sql]
      else
        EQUAL_COND % [left, literal(right)]
      end
    end
    
    # Formats a where clause. If parenthesize is true, then the whole 
    # generated clause will be enclosed in a set of parentheses.
    def where_list(where, parenthesize = false)
      case where
      when Hash:
        parenthesize = false if where.size == 1
        fmt = where.map {|kv| where_condition(*kv)}.join(AND_SEPARATOR)
      when Array:
        fmt = where.shift
        fmt.gsub!('?') {|i| literal(where.shift)}
      else
        fmt = where
      end
      parenthesize ? "(#{fmt})" : fmt
    end
    
    # Formats a join condition.
    def join_cond_list(cond, join_table)
      cond.map do |kv|
        EQUAL_COND % [
          qualified_field_name(kv[0], join_table), 
          qualified_field_name(kv[1], @opts[:from])]
      end.join(AND_SEPARATOR)
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

    AND_WHERE = "%s AND %s".freeze
    
    # Returns a copy of the dataset with the given conditions imposed upon it.  
    # If the query has been grouped, then the conditions are imposed in the 
    # HAVING clause. If not, then they are imposed in the WHERE clause.
    def filter(*cond)
      clause = (@opts[:group] ? :having : :where)
      cond = cond.first if cond.size == 1
      parenthesize = !(cond.is_a?(Hash) || cond.is_a?(Array))
      if @opts[clause]
        cond = AND_WHERE % [where_list(@opts[clause]), where_list(cond, parenthesize)]
      end
      dup_merge(clause => where_list(cond))
    end

    NOT_WHERE = "NOT %s".freeze
    AND_NOT_WHERE = "%s AND NOT %s".freeze
    
    def exclude(*cond)
      clause = (@opts[:group] ? :having : :where)
      cond = cond.first if cond.size == 1
      parenthesize = !(cond.is_a?(Hash) || cond.is_a?(Array))
      if @opts[clause]
        cond = AND_NOT_WHERE % [where_list(@opts[clause]), where_list(cond, parenthesize)]
      else
        cond = NOT_WHERE % where_list(cond, true)
      end
      dup_merge(clause => cond)
    end
    
    # Returns a copy of the dataset with the where conditions changed. Raises 
    # if the dataset has been grouped. See also #filter
    def where(*cond)
      if @opts[:group]
        raise SequelError, "Can't specify a WHERE clause once the dataset has been grouped"
      else
        filter(*cond)
      end
    end

    # Returns a copy of the dataset with the having conditions changed. Raises 
    # if the dataset has not been grouped. See also #filter
    def having(*cond)
      unless @opts[:group]
        raise SequelError, "Can only specify a HAVING clause on a grouped dataset"
      else
        filter(*cond)
       end
     end
    
    LEFT_OUTER_JOIN = 'LEFT OUTER JOIN'.freeze
    INNER_JOIN = 'INNER JOIN'.freeze
    RIGHT_OUTER_JOIN = 'RIGHT OUTER JOIN'.freeze
    FULL_OUTER_JOIN = 'FULL OUTER JOIN'.freeze
        
    def join(table, cond)
      dup_merge(:join_type => LEFT_OUTER_JOIN, :join_table => table,
        :join_cond => cond)
    end

    alias_method :all, :to_a
    
    alias_method :enum_map, :map
    def map(field_name = nil, &block)
      if block
        enum_map(&block)
      elsif field_name
        enum_map {|r| r[field_name]}
      else
        []
      end
    end
    
    def hash_column(key_column, value_column)
      inject({}) do |m, r|
        m[r[key_column]] = r[value_column]
        m
      end
    end
    
    def <<(values)
      insert(values)
    end
    
    def insert_multiple(array, &block)
      if block
        array.each {|i| insert(block[i])}
      else
        array.each {|i| insert(i)}
      end
    end

    SELECT = "SELECT %s FROM %s".freeze
    SELECT_DISTINCT = "SELECT DISTINCT %s FROM %s".freeze
    LIMIT = " LIMIT %s".freeze
    OFFSET = " OFFSET %s".freeze
    ORDER = " ORDER BY %s".freeze
    WHERE = " WHERE %s".freeze
    GROUP = " GROUP BY %s".freeze
    HAVING = " HAVING %s".freeze
    JOIN_CLAUSE = " %s %s ON %s".freeze
    
    EMPTY = ''.freeze
    
    SPACE = ' '.freeze
    
    def select_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      fields = opts[:select]
      select_fields = fields ? field_list(fields) : WILDCARD
      select_source = source_list(opts[:from])
      sql = (opts[:distinct] ? SELECT_DISTINCT : SELECT) % [select_fields, select_source]
      
      if join_type = opts[:join_type]
        join_table = opts[:join_table]
        join_cond = join_cond_list(opts[:join_cond], join_table)
        sql << (JOIN_CLAUSE % [join_type, join_table, join_cond])
      end
      
      if where = opts[:where]
        sql << (WHERE % where)
      end
      
      if group = opts[:group]
        sql << (GROUP % field_list(group))
      end

      if order = opts[:order]
        sql << (ORDER % field_list(order))
      end
      
      if having = opts[:having]
        sql << (HAVING % having)
      end

      if limit = opts[:limit]
        sql << (LIMIT % limit)
        if offset = opts[:offset]
          sql << (OFFSET % offset)
        end
      end
            
      sql
    end
    
    alias_method :sql, :select_sql
    
    INSERT = "INSERT INTO %s (%s) VALUES (%s)".freeze
    INSERT_VALUES = "INSERT INTO %s VALUES (%s)".freeze
    INSERT_EMPTY = "INSERT INTO %s DEFAULT VALUES".freeze
    
    def insert_sql(*values)
      if values.empty?
        INSERT_EMPTY % @opts[:from]
      elsif (values.size == 1) && values[0].is_a?(Hash)
        field_list = []
        value_list = []
        values[0].each do |k, v|
          field_list << k
          value_list << literal(v)
        end
        INSERT % [
          @opts[:from], 
          field_list.join(COMMA_SEPARATOR), 
          value_list.join(COMMA_SEPARATOR)]
      else
        INSERT_VALUES % [@opts[:from], literal(values)]
      end
    end
    
    UPDATE = "UPDATE %s SET %s".freeze
    SET_FORMAT = "%s = %s".freeze
    
    def update_sql(values, opts = nil)
      opts = opts ? @opts.merge(opts) : @opts
      
      if opts[:group]
        raise SequelError, "Can't update a grouped dataset" 
      elsif (opts[:from].size > 1) or opts[:join_type]
        raise SequelError, "Can't update a joined dataset"
      end

      set_list = values.map {|kv| SET_FORMAT % [kv[0], literal(kv[1])]}.
        join(COMMA_SEPARATOR)
      sql = UPDATE % [opts[:from], set_list]
      
      if where = opts[:where]
        sql << WHERE % where_list(where)
      end

      sql
    end
    
    DELETE = "DELETE FROM %s".freeze
    
    def delete_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      if opts[:group]
        raise SequelError, "Can't delete from a grouped dataset" 
      elsif opts[:from].is_a?(Array) && opts[:from].size > 1
        raise SequelError, "Can't delete from a joined dataset"
      end

      sql = DELETE % opts[:from]

      if where = opts[:where]
        sql << WHERE % where_list(where)
      end

      sql
    end
    
    COUNT = "COUNT(*)".freeze
    SELECT_COUNT = {:select => COUNT, :order => nil}.freeze
    
    def count_sql(opts = nil)
      select_sql(opts ? opts.merge(SELECT_COUNT) : SELECT_COUNT)
    end

    def to_table_reference
      if opts.keys == [:from] && opts[:from].size == 1
        opts[:from].first.to_s
      else
        SUBQUERY % sql
      end
    end
    
    # aggregates
    def min(field)
      select(field.MIN).naked.first.values.first
    end
    
    def max(field)
      select(field.MAX).naked.first.values.first
    end

    def sum(field)
      select(field.SUM).naked.first.values.first
    end
    
    def avg(field)
      select(field.AVG).naked.first.values.first
    end
    
    EXISTS_EXPR = "EXISTS (%s)".freeze
    
    def exists(opts = nil)
      EXISTS_EXPR % sql({:select => 1}.merge(opts || {}))
    end
    
    LIMIT_1 = {:limit => 1}.freeze
    
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
    def first(num = 1)
      if num == 1
        first_record
      else
        limit(num).all
      end
    end
    
    # Returns the first record matching the condition.
    def [](*conditions)
      where(*conditions).first
    end
    
    # Updates all records matching the condition with the values specified.
    def []=(condition, values)
      where(condition).update(values)
    end
  
    def last(num = 1)
      raise SequelError, 'No order specified' unless 
        @opts[:order] || (opts && opts[:order])
      
      l = {:limit => num}
      opts = {:order => invert_order(@opts[:order])}.
        merge(opts ? opts.merge(l) : l)

      if num == 1
        first_record(opts)
      else
        dup_merge(opts).all
      end
    end
    
    # Deletes all records in the dataset one at a time by invoking the destroy
    # method of the associated model class.
    def destroy
      raise SequelError, 'Dataset not associated with model' unless @record_class
      
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

class Symbol
  def DESC
    "#{to_s} DESC"
  end
  
  def AS(target)
    "#{to_field_name} AS #{target}"
  end
  
  def MIN; "min(#{to_field_name})"; end
  def MAX; "max(#{to_field_name})"; end
  def SUM; "sum(#{to_field_name})"; end
  def AVG; "avg(#{to_field_name})"; end

  AS_REGEXP = /(.*)___(.*)/.freeze
  AS_FORMAT = "%s AS %s".freeze
  DOUBLE_UNDERSCORE = '__'.freeze
  PERIOD = '.'.freeze
  
  def to_field_name
    s = to_s
    if s =~ AS_REGEXP
      s = AS_FORMAT % [$1, $2]
    end
    s.split(DOUBLE_UNDERSCORE).join(PERIOD)
  end
  
  def ALL
    "#{to_s}.*"
  end
end
