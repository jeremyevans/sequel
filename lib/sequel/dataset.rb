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
    def initialize(db, opts = {}, record_class = nil)
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
      @record_class ? self.class.new(@db, @opts) : self
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
      case fields
      when Array:
        if fields.empty?
          WILDCARD
        else
          fields.map {|i| field_name(i)}.join(COMMA_SEPARATOR)
        end
      when Symbol:
        fields.to_field_name
      else
        fields
      end
    end
    
    # Converts an array of sources into a comma separated list.
    def source_list(source)
      case source
      when Array: source.join(COMMA_SEPARATOR)
      else source
      end 
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
      when self.class: SUBQUERY % v.sql
      else
        raise "can't express #{v.inspect}:#{v.class} as a SQL literal"
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
      when self.class:
        IN_EXPR % [left, right.sql]
      else
        EQUAL_COND % [left, literal(right)]
      end
    end
    
    # Formats a where clause.
    def where_list(where)
      case where
      when Hash:
        where.map {|kv| where_condition(*kv)}.join(AND_SEPARATOR)
      when Array:
        fmt = where.shift
        fmt.gsub('?') {|i| literal(where.shift)}
      else
        where
      end
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
    def from(source)
      dup_merge(:from => source)
    end
    
    # Returns a copy of the dataset with the selected fields changed.
    def select(*fields)
      fields = fields.first if fields.size == 1
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
      order.map do |f|
        f.to_s =~ DESC_ORDER_REGEXP ? $1 : f.DESC
      end
    end
    
    AND_WHERE = "%s AND %s".freeze
    
    # Returns a copy of the dataset with the where conditions changed.
    def where(*cond)
      cond = cond.first if cond.size == 1
      if @opts[:where]
        if @opts[:where].is_a?(Hash) && cond.is_a?(Hash)
          cond = @opts[:where].merge(cond)
        else
          cond = AND_WHERE % [where_list(@opts[:where]), where_list(cond)]
        end
      end
      dup_merge(:where => cond)
    end
    
    NOT_WHERE = "NOT %s".freeze
    
    def exclude(*cond)
      cond = cond.first if cond.size == 1
      where(NOT_WHERE % where_list(cond))
    end
    
    LEFT_OUTER_JOIN = 'LEFT OUTER JOIN'.freeze
    INNER_JOIN = 'INNER JOIN'.freeze
    RIGHT_OUTER_JOIN = 'RIGHT OUTER JOIN'.freeze
    FULL_OUTER_JOIN = 'FULL OUTER JOIN'.freeze
        
    def join(table, cond)
      dup_merge(:join_type => LEFT_OUTER_JOIN, :join_table => table,
        :join_cond => cond)
    end

    alias_method :filter, :where
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
        sql << (WHERE % where_list(where))
      end
      
      if order = opts[:order]
        sql << (ORDER % order.join(COMMA_SEPARATOR))
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
      
      set_list = values.map {|kv| SET_FORMAT % [kv[0], literal(kv[1])]}.
        join(COMMA_SEPARATOR)
      update_clause = UPDATE % [opts[:from], set_list]
      
      where = opts[:where]
      where_clause = where ? WHERE % where_list(where) : EMPTY

      [update_clause, where_clause].join(SPACE)
    end
    
    DELETE = "DELETE FROM %s".freeze
    
    def delete_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      delete_source = opts[:from] 
      
      where = opts[:where]
      where_clause = where ? WHERE % where_list(where) : EMPTY
      
      [DELETE % delete_source, where_clause].join(SPACE)
    end
    
    COUNT = "COUNT(*)".freeze
    SELECT_COUNT = {:select => COUNT, :order => nil}.freeze
    
    def count_sql(opts = nil)
      select_sql(opts ? opts.merge(SELECT_COUNT) : SELECT_COUNT)
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
    
    def first(num = 1)
      if num == 1
        first_record
      else
        limit(num).all
      end
    end
    
    def [](condition)
      where(condition).first
    end
  
    def last(num = 1)
      raise 'No order specified' unless @opts[:order] || (opts && opts[:order])
      
      l = {:limit => num}
      opts = {:order => invert_order(@opts[:order])}.
        merge(opts ? opts.merge(l) : l)

      if num == 1
        first_record(opts)
      else
        dup_merge(opts).all
      end
    end
    
    def destroy
      raise 'Dataset not associated with model' unless @record_class
      
      count = 0
      @db.transaction {each {|r| count += 1; r.destroy}}
      count
    end
    
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
    "#{field_name} AS #{target}"
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

