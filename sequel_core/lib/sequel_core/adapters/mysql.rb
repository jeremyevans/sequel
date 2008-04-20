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

      def server_version
        @server_version ||= pool.hold do |conn|
          if conn.respond_to?(:server_version)
            pool.hold {|c| c.server_version}
          else
            get(:version[]) =~ /(\d+)\.(\d+)\.(\d+)/
            ($1.to_i * 10000) + ($2.to_i * 100) + $3.to_i
          end
        end
      end
      
      def serial_primary_key_options
        {:primary_key => true, :type => :integer, :auto_increment => true}
      end

      AUTO_INCREMENT = 'AUTO_INCREMENT'.freeze

      def auto_increment_sql
        AUTO_INCREMENT
      end

      def connect
        conn = Mysql.init
        conn.options(Mysql::OPT_LOCAL_INFILE, "client")
        conn.real_connect(
          @opts[:host] || 'localhost',
          @opts[:user],
          @opts[:password],
          @opts[:database],
          @opts[:port],
          @opts[:socket],
          Mysql::CLIENT_MULTI_RESULTS +
          Mysql::CLIENT_MULTI_STATEMENTS +
          Mysql::CLIENT_COMPRESS
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
        type = type_literal(op[:type])
        type << '(255)' if type == 'varchar'
        case op[:op]
        when :rename_column
          "ALTER TABLE #{table} CHANGE COLUMN #{literal(op[:name])} #{literal(op[:new_name])} #{type}"
        when :set_column_type
          "ALTER TABLE #{table} CHANGE COLUMN #{literal(op[:name])} #{literal(op[:name])} #{type}"
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
        sql << UNSIGNED if column[:unsigned]
        sql << UNIQUE if column[:unique]
        sql << NOT_NULL if column[:null] == false
        sql << NULL if column[:null] == true
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

      def index_definition_sql(table_name, index)
        index_name = index[:name] || default_index_name(table_name, index[:columns])
        unique = "UNIQUE " if index[:unique]
        case index[:type]
        when :full_text
          "CREATE FULLTEXT INDEX #{index_name} ON #{table_name} (#{literal(index[:columns])})"
        when :spatial
          "CREATE SPATIAL INDEX #{index_name} ON #{table_name} (#{literal(index[:columns])})"
        when nil
          "CREATE #{unique}INDEX #{index_name} ON #{table_name} (#{literal(index[:columns])})"
        else
          "CREATE #{unique}INDEX #{index_name} ON #{table_name} (#{literal(index[:columns])}) USING #{index[:type]}"
        end
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

      # Changes the database in use by issuing a USE statement.
      def use(db_name)
        disconnect
        @opts[:database] = db_name if self << "USE #{db_name}"
        self
      end
    end

    class Dataset < Sequel::Dataset
      def quote_column_ref(c); "`#{c}`"; end

      TRUE = '1'
      FALSE = '0'

      # Join processing changed after MySQL v5.0.12. NATURAL
      # joins are SQL:2003 consistent.
      JOIN_TYPES  =  { :cross => 'INNER JOIN'.freeze,
        :straight => 'STRAIGHT_JOIN'.freeze,
        :natural_left => 'NATURAL LEFT JOIN'.freeze,
        :natural_right => 'NATURAL RIGHT JOIN'.freeze,
        :natural_left_outer => 'NATURAL LEFT OUTER JOIN'.freeze,
        :natural_right_outer => 'NATURAL RIGHT OUTER JOIN'.freeze,
        :left => 'LEFT JOIN'.freeze,
        :right => 'RIGHT JOIN'.freeze,
        :left_outer => 'LEFT OUTER JOIN'.freeze,
        :right_outer => 'RIGHT OUTER JOIN'.freeze,
        :natural_inner => 'NATURAL LEFT JOIN'.freeze,
        # :full_outer => 'FULL OUTER JOIN'.freeze,
        #
        # A full outer join, nor a workaround implementation of
        # :full_outer, is not yet possible in Sequel. See issue
        # #195 which probably depends on issue #113 being
        # resolved.
        :inner => 'INNER JOIN'.freeze
      }

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

      # Returns a join clause based on the specified join type
      # and condition.  MySQL's NATURAL join is 'semantically
      # equivalent to a JOIN with a USING clause that names all
      # columns that exist in both tables.  The constraint
      # expression may be nil, so join expression can accept two
      # arguments.
      #
      # === Note
      # Full outer joins (:full_outer) are not implemented in
      # MySQL (as of v6.0), nor is there currently a work around
      # implementation in Sequel.  Straight joins with 'ON
      # <condition>' are not yet implemented.
      #
      # === Example
      #   @ds = MYSQL_DB[:nodes]
      #   @ds.join_expr(:natural_left_outer, :nodes)
      #   # 'NATURAL LEFT OUTER JOIN nodes'
      #
      def join_expr(type, table, expr = nil)
        join_type = JOIN_TYPES[type || :inner]
        unless join_type
          raise Error::InvalidJoinType, "Invalid join type: #{type}"
        end
        @opts[:server_version] = @db.server_version unless @opts[:server_version]
        type = :inner if type == :cross && !expr.nil?
        if type.to_s =~ /natural|cross|straight/ &&  @opts[:server_version] >= 50014
          tbl_factor = table.to_s
          if table.is_a?(Dataset)
            tbl_factor = table.sql << " AS t1"
          end
          if table.is_a?(Array)
            tbl_factor = "( #{literal(table)} )"
          end
          join_string = "#{join_type} " << tbl_factor
          return join_string
        end

        super
      end

      def insert_default_values_sql
        "INSERT INTO #{@opts[:from]} () VALUES ()"
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
      
      def full_text_search(cols, terms, opts = {})
        mode = opts[:boolean] ? " IN BOOLEAN MODE" : ""
        filter("MATCH (#{literal(cols)}) AGAINST (#{literal(terms)}#{mode})")
      end

      # MySQL allows HAVING clause on ungrouped datasets.
      def having(*cond, &block)
        @opts[:having] = {}
        filter(*cond, &block)
      end

      # MySQL supports ORDER and LIMIT clauses in UPDATE statements.
      def update_sql(values, opts = nil, &block)
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
      
      def replace_sql(*values)
        if values.empty?
          "REPLACE INTO #{@opts[:from]} DEFAULT VALUES"
        else
          values = values[0] if values.size == 1
          
          # if hash or array with keys we need to transform the values
          if @transform && (values.is_a?(Hash) || (values.is_a?(Array) && values.keys))
            values = transform_save(values)
          end

          case values
          when Array
            if values.empty?
              "REPLACE INTO #{@opts[:from]} DEFAULT VALUES"
            elsif values.keys
              fl = values.keys.map {|f| literal(f.is_a?(String) ? f.to_sym : f)}
              vl = values.values.map {|v| literal(v)}
              "REPLACE INTO #{@opts[:from]} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
            else
              "REPLACE INTO #{@opts[:from]} VALUES (#{literal(values)})"
            end
          when Hash
            if values.empty?
              "REPLACE INTO #{@opts[:from]} DEFAULT VALUES"
            else
              fl, vl = [], []
              values.each {|k, v| fl << literal(k.is_a?(String) ? k.to_sym : k); vl << literal(v)}
              "REPLACE INTO #{@opts[:from]} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
            end
          when Dataset
            "REPLACE INTO #{@opts[:from]} #{literal(values)}"
          else
            if values.respond_to?(:values)
              replace_sql(values.values)
            else  
              "REPLACE INTO #{@opts[:from]} VALUES (#{literal(values)})"
            end
          end
        end
      end
      
      # MySQL supports ORDER and LIMIT clauses in DELETE statements.
      def delete_sql(opts = nil)
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
      
      def replace(*args)
        @db.execute(replace_sql(*args)) {|c| c.insert_id}
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

      def multi_insert_sql(columns, values)
        columns = literal(columns)
        values = values.map {|r| "(#{literal(r)})"}.join(COMMA_SEPARATOR)
        ["INSERT INTO #{@opts[:from]} (#{columns}) VALUES #{values}"]
      end
    end
  end
end
