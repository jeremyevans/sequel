module Sequel
  class Dataset
    # The Dataset SQL module implements all the dataset methods concerned with
    # generating SQL statements for retrieving and manipulating records.
    module SQL
      # Adds quoting to column references. This method is just a stub and can
      # be overriden in adapters in order to provide correct column quoting
      # behavior.
      def quote_column_ref(name); name.to_s; end
      
      ALIASED_REGEXP = /^(.*)\s(.*)$/.freeze
      QUALIFIED_REGEXP = /^(.*)\.(.*)$/.freeze

      # Returns a qualified column name (including a table name) if the column
      # name isn't already qualified.
      def qualified_column_name(column, table)
        s = literal(column)
        if s =~ QUALIFIED_REGEXP
          return column
        else
          if (table =~ ALIASED_REGEXP)
            table = $2
          end
          Sequel::SQL::QualifiedColumnRef.new(table, column)
          # "#{table}.#{column}"
        end
      end

      WILDCARD = '*'.freeze
      COMMA_SEPARATOR = ", ".freeze

      # Converts an array of column names into a comma seperated string of 
      # column names. If the array is empty, a wildcard (*) is returned.
      def column_list(columns)
        if columns.empty?
          WILDCARD
        else
          m = columns.map do |i|
            i.is_a?(Hash) ? i.map {|kv| "#{literal(kv[0])} AS #{kv[1]}"} : literal(i)
          end
          m.join(COMMA_SEPARATOR)
        end
      end

      # Converts an array of sources names into into a comma separated list.
      def source_list(source)
        if source.nil? || source.empty?
          raise SequelError, 'No source specified for query'
        end
        auto_alias_count = 0
        m = source.map do |i|
          case i
          when Dataset:
            auto_alias_count += 1
            i.to_table_reference(auto_alias_count)
          when Hash:
            i.map {|k, v| "#{k.is_a?(Dataset) ? k.to_table_reference : k} #{v}"}.
              join(COMMA_SEPARATOR)
          else
            i
          end
        end
        m.join(COMMA_SEPARATOR)
      end

      NULL = "NULL".freeze
      TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S'".freeze
      DATE_FORMAT = "DATE '%Y-%m-%d'".freeze
      TRUE = "'t'".freeze
      FALSE = "'f'".freeze

      # Returns a literal representation of a value to be used as part
      # of an SQL expression. The stock implementation supports literalization 
      # of String (with proper escaping to prevent SQL injections), numbers,
      # Symbol (as column references), Array (as a list of literalized values),
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
        when LiteralString: v
        when String: "'#{v.gsub(/'/, "''")}'"
        when Integer, Float: v.to_s
        when BigDecimal: v.to_s("F")
        when NilClass: NULL
        when TrueClass: TRUE
        when FalseClass: FALSE
        when Symbol: v.to_column_ref(self)
        when Sequel::SQL::Expression: v.to_s(self)
        when Array: v.empty? ? NULL : v.map {|i| literal(i)}.join(COMMA_SEPARATOR)
        when Time: v.strftime(TIMESTAMP_FORMAT)
        when Date: v.strftime(DATE_FORMAT)
        when Dataset: "(#{v.sql})"
        else
          raise SequelError, "can't express #{v.inspect} as a SQL literal"
        end
      end

      AND_SEPARATOR = " AND ".freeze
      QUESTION_MARK = '?'.freeze

      # Formats a where clause. If parenthesize is true, then the whole 
      # generated clause will be enclosed in a set of parentheses.
      def expression_list(expr, parenthesize = false)
        case expr
        when Hash:
          parenthesize = false if expr.size == 1
          fmt = expr.map {|i| compare_expr(i[0], i[1])}.join(AND_SEPARATOR)
        when Array:
          fmt = expr.shift.gsub(QUESTION_MARK) {literal(expr.shift)}
        when Proc:
          fmt = proc_to_sql(expr)
        else
          # if the expression is compound, it should be parenthesized in order for 
          # things to be predictable (when using #or and #and.)
          parenthesize |= expr =~ /\).+\(/
          fmt = expr
        end
        parenthesize ? "(#{fmt})" : fmt
      end

      # Returns a copy of the dataset with the source changed.
      def from(*source)
        clone_merge(:from => source)
      end

      # Returns a copy of the dataset with the selected columns changed.
      def select(*columns)
        clone_merge(:select => columns)
      end

      # Returns a copy of the dataset with the distinct option.
      def uniq(*args)
        clone_merge(:distinct => args)
      end
      alias_method :distinct, :uniq

      # Returns a copy of the dataset with the order changed.
      def order(*order)
        clone_merge(:order => order)
      end
      
      alias_method :order_by, :order

      # Returns a copy of the dataset with the order reversed. If no order is
      # given, the existing order is inverted.
      def reverse_order(*order)
        order(*invert_order(order.empty? ? @opts[:order] : order))
      end

      # Inverts the given order by breaking it into a list of column references
      # and inverting them.
      #
      #   dataset.invert_order([:id.desc]]) #=> [:id]
      #   dataset.invert_order(:category, :price.desc]) #=>
      #     [:category.desc, :price]
      def invert_order(order)
        new_order = []
        order.map do |f|
          if f.is_a?(Sequel::SQL::ColumnExpr) && (f.op == Sequel::SQL::ColumnMethods::DESC)
            f.l
          else
            f.desc
          end
        end
      end

      # Returns a copy of the dataset with the results grouped by the value of 
      # the given columns
      def group(*columns)
        clone_merge(:group => columns)
      end
      
      alias_method :group_by, :group

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
          l = expression_list(@opts[clause])
          r = expression_list(block || cond, parenthesize)
          clone_merge(clause => "#{l} AND #{r}")
        else
          clone_merge(:filter => cond, clause => expression_list(block || cond))
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
          clone_merge(clause => "#{l} OR #{r}")
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
          cond = "#{l} AND (NOT #{r})"
        else
          cond = "(NOT #{expression_list(block || cond, true)})"
        end
        clone_merge(clause => cond)
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
        clone_merge(:union => dataset, :union_all => all)
      end

      # Adds an INTERSECT clause using a second dataset object. If all is true 
      # the clause used is INTERSECT ALL, which may return duplicate rows.
      def intersect(dataset, all = false)
        clone_merge(:intersect => dataset, :intersect_all => all)
      end

      # Adds an EXCEPT clause using a second dataset object. If all is true the
      # clause used is EXCEPT ALL, which may return duplicate rows.
      def except(dataset, all = false)
        clone_merge(:except => dataset, :except_all => all)
      end

      JOIN_TYPES = {
        :left_outer => 'LEFT OUTER JOIN'.freeze,
        :right_outer => 'RIGHT OUTER JOIN'.freeze,
        :full_outer => 'FULL OUTER JOIN'.freeze,
        :inner => 'INNER JOIN'.freeze
      }

      # Returns a join clause based on the specified join type and condition.
      def join_expr(type, table, expr)
        join_type = JOIN_TYPES[type || :inner]
        unless join_type
          raise SequelError, "Invalid join type: #{type}"
        end

        join_conditions = {}
        expr.each do |k, v|
          k = qualified_column_name(k, table) if k.is_a?(Symbol)
          v = qualified_column_name(v, @opts[:last_joined_table] || @opts[:from].first) if v.is_a?(Symbol)
          join_conditions[k] = v
        end
        " #{join_type} #{table} ON #{expression_list(join_conditions)}"
      end

      # Returns a joined dataset with the specified join type and condition.
      def join_table(type, table, expr)
        unless expr.is_a?(Hash)
          expr = {expr => :id}
        end
        clause = join_expr(type, table, expr)
        join = @opts[:join] ? @opts[:join] + clause : clause
        clone_merge(:join => join, :last_joined_table => table)
      end

      # Returns a LEFT OUTER joined dataset.
      def left_outer_join(table, expr); join_table(:left_outer, table, expr); end
      
      # Returns a RIGHT OUTER joined dataset.
      def right_outer_join(table, expr); join_table(:right_outer, table, expr); end
      
      # Returns an OUTER joined dataset.
      def full_outer_join(table, expr); join_table(:full_outer, table, expr); end
      
      # Returns an INNER joined dataset.
      def inner_join(table, expr); join_table(:inner, table, expr); end
      alias join inner_join

      # Inserts multiple values. If a block is given it is invoked for each
      # item in the given array before inserting it.
      def insert_multiple(array, &block)
        if block
          array.each {|i| insert(block[i])}
        else
          array.each {|i| insert(i)}
        end
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

        if order = opts[:order]
          sql << " ORDER BY #{column_list(order)}"
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
      alias_method :sql, :select_sql

      # Formats an INSERT statement using the given values. If a hash is given,
      # the resulting statement includes column names. If no values are given, 
      # the resulting statement includes a DEFAULT VALUES clause.
      #
      #   dataset.insert_sql() #=> 'INSERT INTO items DEFAULT VALUES'
      #   dataset.insert_sql(1,2,3) #=> 'INSERT INTO items VALUES (1, 2, 3)'
      #   dataset.insert_sql(:a => 1, :b => 2) #=>
      #     'INSERT INTO items (a, b) VALUES (1, 2)'
      def insert_sql(*values)
        if values.empty?
          "INSERT INTO #{@opts[:from]} DEFAULT VALUES"
        else
          values = values[0] if values.size == 1
          case values
          when Sequel::Model
            insert_sql(values.values)
          when Array
            if values.empty?
              "INSERT INTO #{@opts[:from]} DEFAULT VALUES"
            elsif values.keys
              fl = values.keys.map {|f| literal(f.to_sym)}
              vl = transform_save(values.values).map {|v| literal(v)}
              "INSERT INTO #{@opts[:from]} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
            else
              "INSERT INTO #{@opts[:from]} VALUES (#{literal(values)})"
            end
          when Hash
            values = transform_save(values) if @transform
            if values.empty?
              "INSERT INTO #{@opts[:from]} DEFAULT VALUES"
            else
              fl, vl = [], []
              values.each {|k, v| fl << literal(k.to_sym); vl << literal(v)}
              "INSERT INTO #{@opts[:from]} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
            end
          when Dataset
            "INSERT INTO #{@opts[:from]} #{literal(values)}"
          else
            "INSERT INTO #{@opts[:from]} VALUES (#{literal(values)})"
          end
        end
      end

      # Formats an UPDATE statement using the given values.
      #
      #   dataset.update_sql(:price => 100, :category => 'software') #=>
      #     "UPDATE items SET price = 100, category = 'software'"
      def update_sql(values = {}, opts = nil, &block)
        opts = opts ? @opts.merge(opts) : @opts

        if opts[:group]
          raise SequelError, "Can't update a grouped dataset" 
        elsif (opts[:from].size > 1) or opts[:join]
          raise SequelError, "Can't update a joined dataset"
        end
        
        sql = "UPDATE #{@opts[:from]} SET "
        if block
          sql << proc_to_sql(block, true)
        else
          if values.is_a?(Array) && values.keys
            values = values.to_hash
          end
          values = transform_save(values) if @transform
          sql << values.map {|k, v| "#{literal(k)} = #{literal(v)}"}.join(COMMA_SEPARATOR)
        end
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

      # Returns a table reference for use in the FROM clause. If the dataset has
      # only a :from option refering to a single table, only the table name is 
      # returned. Otherwise a subquery is returned.
      def to_table_reference(idx = nil)
        if opts.keys == [:from] && opts[:from].size == 1
          opts[:from].first.to_s
        else
          idx ? "(#{sql}) t#{idx}" : "(#{sql})"
        end
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
          clone_merge(:limit => lim, :offset=>l.first)
        elsif o
          clone_merge(:limit => l, :offset => o)
        else
          clone_merge(:limit => l)
        end
      end
      
      STOCK_COUNT_OPTS = {:select => ["COUNT(*)".lit], :order => nil}.freeze

      # Returns the number of records in the dataset.
      def count
        opts = @opts[:sql] ? \
          {:sql => "SELECT COUNT(*) FROM (#{@opts[:sql]}) AS c", :order => nil} : \
          STOCK_COUNT_OPTS

        single_value(opts).to_i
      end
    end
  end
end