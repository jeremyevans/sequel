require 'mysql'

# Monkey patch Mysql::Result to yield hashes with symbol keys
class Mysql::Result
  MYSQL_TYPES = {
    0   => :to_d,     # MYSQL_TYPE_DECIMAL
    1   => :to_i,     # MYSQL_TYPE_TINY
    2   => :to_i,     # MYSQL_TYPE_SHORT
    3   => :to_i,     # MYSQL_TYPE_LONG
    4   => :to_f,     # MYSQL_TYPE_FLOAT
    5   => :to_f,     # MYSQL_TYPE_DOUBLE
    # 6   => ??,        # MYSQL_TYPE_NULL
    7   => :to_time,  # MYSQL_TYPE_TIMESTAMP
    8   => :to_i,     # MYSQL_TYPE_LONGLONG
    9   => :to_i,     # MYSQL_TYPE_INT24
    10  => :to_date,  # MYSQL_TYPE_DATE
    11  => :to_time,  # MYSQL_TYPE_TIME
    12  => :to_time,  # MYSQL_TYPE_DATETIME
    13  => :to_i,     # MYSQL_TYPE_YEAR
    14  => :to_date,  # MYSQL_TYPE_NEWDATE
    # 15  => :to_s      # MYSQL_TYPE_VARCHAR
    # 16  => :to_s,     # MYSQL_TYPE_BIT
    246 => :to_d,     # MYSQL_TYPE_NEWDECIMAL
    247 => :to_i,     # MYSQL_TYPE_ENUM
    248 => :to_i      # MYSQL_TYPE_SET
    # 249 => :to_s,     # MYSQL_TYPE_TINY_BLOB
    # 250 => :to_s,     # MYSQL_TYPE_MEDIUM_BLOB
    # 251 => :to_s,     # MYSQL_TYPE_LONG_BLOB
    # 252 => :to_s,     # MYSQL_TYPE_BLOB
    # 253 => :to_s,     # MYSQL_TYPE_VAR_STRING
    # 254 => :to_s,     # MYSQL_TYPE_STRING
    # 255 => :to_s      # MYSQL_TYPE_GEOMETRY
  }

  def convert_type(v, type)
    v ? ((t = MYSQL_TYPES[type]) ? v.send(t) : v) : nil
  end

  def columns(with_table = nil)
    unless @columns
      @column_types = []
      @columns = fetch_fields.map do |f|
        @column_types << f.type
        (with_table ? "#{f.table}.#{f.name}" : f.name).to_sym
      end
    end
    @columns
  end

  def each_array(with_table = nil)
    c = columns
    while row = fetch_row
      c.each_with_index do |f, i|
        if (t = MYSQL_TYPES[@column_types[i]]) && (v = row[i])
          row[i] = v.send(t)
        end
      end
      row.keys = c
      yield row
    end
  end

  def each_hash(with_table = nil)
    c = columns
    while row = fetch_row
      h = {}
      c.each_with_index {|f, i| h[f] = convert_type(row[i], @column_types[i])}
      yield h
    end
  end
end

module Sequel
  module MySQL
    class Database < Sequel::Database
      set_adapter_scheme :mysql

      def serial_primary_key_options
        {:primary_key => true, :type => :integer, :auto_increment => true}
      end

      AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze

      def auto_increment_sql
        AUTO_INCREMENT
      end

      def connect
        conn = Mysql.real_connect(
          @opts[:host] || 'localhost',
          @opts[:user],
          @opts[:password],
          @opts[:database],
          @opts[:port],
          @opts[:socket],
          Mysql::CLIENT_MULTI_RESULTS
        )
        conn.query_with_result = false
        if encoding = @opts[:encoding] || @opts[:charset]
          conn.query("set character_set_connection = '#{encoding}'")
          conn.query("set character_set_client = '#{encoding}'")
          conn.query("set character_set_results = '#{encoding}'")
        end
        conn.reconnect = true
        conn
      end

      def disconnect
        @pool.disconnect {|c| c.close}
      end

      def tables
        @pool.hold do |conn|
          conn.list_tables.map {|t| t.to_sym}
        end
      end

      def dataset(opts = nil)
        MySQL::Dataset.new(self, opts)
      end

      def execute(sql, &block)
        @logger.info(sql) if @logger
        @pool.hold do |conn|
          conn.query(sql)
          block[conn] if block
        end
      end

      def execute_select(sql, &block)
        execute(sql) do |c|
          r = c.use_result
          begin
            block[r]
          ensure
            r.free
          end
        end
      end

      def alter_table_sql(table, op)
        case op[:op]
        when :rename_column
          "ALTER TABLE #{table} CHANGE COLUMN #{literal(op[:name])} #{literal(op[:new_name])} #{op[:type]}"
        when :set_column_type
          "ALTER TABLE #{table} CHANGE COLUMN #{literal(op[:name])} #{literal(op[:name])} #{op[:type]}"
        when :drop_index
          "DROP INDEX #{default_index_name(table, op[:columns])} ON #{table}"
        else
          super(table, op)
        end
      end
      
      def column_definition_sql(column)
        if column[:type] == :check
          return constraint_definition_sql(column)
        end
        sql = "#{literal(column[:name].to_sym)} #{TYPES[column[:type]]}"
        column[:size] ||= 255 if column[:type] == :varchar
        elements = column[:size] || column[:elements]
        sql << "(#{literal(elements)})" if elements
        sql << UNIQUE if column[:unique]
        sql << NOT_NULL if column[:null] == false
        sql << UNSIGNED if column[:unsigned]
        sql << " DEFAULT #{literal(column[:default])}" if column.include?(:default)
        sql << PRIMARY_KEY if column[:primary_key]
        sql << " #{auto_increment_sql}" if column[:auto_increment]
        if column[:table]
          sql << ", FOREIGN KEY (#{literal(column[:name].to_sym)}) REFERENCES #{column[:table]}"
          sql << "(#{literal(column[:key])})" if column[:key]
          sql << " ON DELETE #{on_delete_clause(column[:on_delete])}" if column[:on_delete]
        end
        sql
      end

      def transaction
        @pool.hold do |conn|
          @transactions ||= []
          if @transactions.include? Thread.current
            return yield(conn)
          end
          conn.query(SQL_BEGIN)
          begin
            @transactions << Thread.current
            result = yield(conn)
            conn.query(SQL_COMMIT)
            result
          rescue => e
            conn.query(SQL_ROLLBACK)
            raise e unless Error::Rollback === e
          ensure
            @transactions.delete(Thread.current)
          end
        end
      end
    end

    class Dataset < Sequel::Dataset
      def quote_column_ref(c); "`#{c}`"; end

      TRUE = '1'
      FALSE = '0'

      def literal(v)
        case v
        when LiteralString
          v
        when String
          "'#{v.gsub(/'|\\/, '\&\&')}'"
        when true
          TRUE
        when false
          FALSE
        else
          super
        end
      end

      def match_expr(l, r)
        case r
        when Regexp
          r.casefold? ? \
          "(#{literal(l)} REGEXP #{literal(r.source)})" :
          "(#{literal(l)} REGEXP BINARY #{literal(r.source)})"
        else
          super
        end
      end

      # MySQL expects the having clause before the order by clause.
      def select_sql(opts = nil)
        opts = opts ? @opts.merge(opts) : @opts
        
        if sql = opts[:sql]
          return sql
        end

        columns = opts[:select]
        select_columns = columns ? column_list(columns) : WILDCARD

        if distinct = opts[:distinct]
          distinct_clause = distinct.empty? ? "DISTINCT" : "DISTINCT ON (#{column_list(distinct)})"
          sql = "SELECT #{distinct_clause} #{select_columns}"
        else
          sql = "SELECT #{select_columns}"
        end
        
        if opts[:from]
          sql << " FROM #{source_list(opts[:from])}"
        end
        
        if join = opts[:join]
          sql << join
        end

        if where = opts[:where]
          sql << " WHERE #{where}"
        end

        if group = opts[:group]
          sql << " GROUP BY #{column_list(group)}"
        end

        if having = opts[:having]
          sql << " HAVING #{having}"
        end

        if order = opts[:order]
          sql << " ORDER BY #{column_list(order)}"
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

      # MySQL allows HAVING clause on ungrouped datasets.
      def having(*cond, &block)
        @opts[:having] = {}
        filter(*cond, &block)
      end

      # MySQL supports ORDER and LIMIT clauses in UPDATE statements.
      def update_sql(values, opts = nil)
        sql = super

        opts = opts ? @opts.merge(opts) : @opts

        if order = opts[:order]
          sql << " ORDER BY #{column_list(order)}"
        end

        if limit = opts[:limit]
          sql << " LIMIT #{limit}"
        end

        sql
      end

      def insert(*values)
        @db.execute(insert_sql(*values)) {|c| c.insert_id}
      end

      def update(*args, &block)
        @db.execute(update_sql(*args, &block)) {|c| c.affected_rows}
      end

      def delete(opts = nil)
        @db.execute(delete_sql(opts)) {|c| c.affected_rows}
      end

      def fetch_rows(sql)
        @db.execute_select(sql) do |r|
          @columns = r.columns
          r.each_hash {|row| yield row}
        end
        self
      end

      def array_tuples_fetch_rows(sql, &block)
        @db.execute_select(sql) do |r|
          @columns = r.columns
          r.each_array(&block)
        end
        self
      end
    end
  end
end