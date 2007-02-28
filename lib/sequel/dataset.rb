module ServerSide
  class Dataset
    include Enumerable
    
    attr_reader :db
    attr_accessor :record_class
  
    def initialize(db, opts = {}, record_class = nil)
      @db = db
      @opts = opts || {}
      @record_class = record_class
    end
    
    def dup_merge(opts)
      self.class.new(@db, @opts.merge(opts), @record_class)
    end
    
    AS_REGEXP = /(.*)___(.*)/.freeze
    AS_FORMAT = "%s AS %s".freeze
    DOUBLE_UNDERSCORE = '__'.freeze
    PERIOD = '.'.freeze
    
    # sql helpers
    def field_name(field)
      field.is_a?(Symbol) ? field.to_field_name : field
    end
    
    QUALIFIED_REGEXP = /(.*)\.(.*)/.freeze
    QUALIFIED_FORMAT = "%s.%s".freeze
    
    def qualified_field_name(field, table)
      fn = field_name(field)
      fn = QUALIFIED_FORMAT % [table, fn] unless fn =~ QUALIFIED_REGEXP
    end
    
    WILDCARD = '*'.freeze
    COMMA_SEPARATOR = ", ".freeze
    
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
    
    def source_list(source)
      case source
      when Array: source.join(COMMA_SEPARATOR)
      else source
      end 
    end
    
    def literal(v)
      case v
      when String: "'%s'" % v
      else v.to_s
      end
    end
    
    AND_SEPARATOR = " AND ".freeze
    EQUAL_COND = "(%s = %s)".freeze
    
    def where_equal_condition(left, right)
      EQUAL_COND % [field_name(left), literal(right)]
    end
    
    def where_list(where)
      case where
      when Hash:
        where.map {|kv| where_equal_condition(kv[0], kv[1])}.join(AND_SEPARATOR)
      when Array:
        fmt = where.shift
        fmt.gsub('?') {|i| literal(where.shift)}
      else
        where
      end
    end
    
    def join_cond_list(cond, join_table)
      cond.map do |kv|
        EQUAL_COND % [
          qualified_field_name(kv[0], join_table), 
          qualified_field_name(kv[1], @opts[:from])]
      end.join(AND_SEPARATOR)
    end
    
    # DSL constructors
    def from(source)
      dup_merge(:from => source)
    end
    
    def select(*fields)
      fields = fields.first if fields.size == 1
      dup_merge(:select => fields)
    end

    def order(*order)
      dup_merge(:order => order)
    end
    
    DESC_ORDER_REGEXP = /(.*)\sDESC/.freeze
    
    def reverse_order(order)
      order.map do |f|
        if f.to_s =~ DESC_ORDER_REGEXP
          $1
        else
          f.DESC
        end
      end
    end
    
    def where(*where)
      if where.size == 1
        where = where.first
        if @opts[:where] && @opts[:where].is_a?(Hash) && where.is_a?(Hash)
          where = @opts[:where].merge(where)
        end
      end
      dup_merge(:where => where)
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

    def from!(source)
      @sql = nil
      @opts[:from] = source
      self
    end
    
    def select!(*fields)
      @sql = nil
      fields = fields.first if fields.size == 1
      @opts[:select] = fields
      self
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

    SELECT = "SELECT %s FROM %s".freeze
    LIMIT = " LIMIT %s".freeze
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
      sql = SELECT % [select_fields, select_source]
      
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
      end
      
      sql
    end
    
    INSERT = "INSERT INTO %s (%s) VALUES (%s)".freeze
    INSERT_EMPTY = "INSERT INTO %s DEFAULT VALUES".freeze
    
    def insert_sql(values, opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      if values.nil? || values.empty?
        INSERT_EMPTY % opts[:from]
      else
        field_list = []
        value_list = []
        values.each do |k, v|
          field_list << k
          value_list << literal(v)
        end
      
        INSERT % [
          opts[:from], 
          field_list.join(COMMA_SEPARATOR), 
          value_list.join(COMMA_SEPARATOR)]
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
      select(field.MIN).first[:min]
    end
    
    def max(field)
      select(field.MAX).first[:max]
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
  
  def MIN; "MIN(#{to_field_name})"; end
  def MAX; "MAX(#{to_field_name})"; end

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

