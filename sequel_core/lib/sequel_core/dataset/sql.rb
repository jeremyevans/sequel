# This file includes all the dataset methods concerned with
# generating SQL statements for retrieving and manipulating records.

module Sequel
  class Dataset
    AND_SEPARATOR = " AND ".freeze
    BOOL_FALSE = "'f'".freeze
    BOOL_TRUE = "'t'".freeze
    COLUMN_REF_RE1 = /\A([\w ]+)__([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE2 = /\A([\w ]+)___([\w ]+)\z/.freeze
    COLUMN_REF_RE3 = /\A([\w ]+)__([\w ]+)\z/.freeze
    DATE_FORMAT = "DATE '%Y-%m-%d'".freeze
    JOIN_TYPES = {
      :left_outer => 'LEFT OUTER JOIN'.freeze,
      :right_outer => 'RIGHT OUTER JOIN'.freeze,
      :full_outer => 'FULL OUTER JOIN'.freeze,
      :inner => 'INNER JOIN'.freeze
    }
    N_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::N_ARITY_OPERATORS
    TWO_ARITY_OPERATORS = ::Sequel::SQL::ComplexExpression::TWO_ARITY_OPERATORS
    NULL = "NULL".freeze
    QUESTION_MARK = '?'.freeze
    STOCK_COUNT_OPTS = {:select => ["COUNT(*)".lit], :order => nil}.freeze
    TIMESTAMP_FORMAT = "TIMESTAMP '%Y-%m-%d %H:%M:%S'".freeze
    WILDCARD = '*'.freeze

    # Adds an further filter to an existing filter using AND. If no filter 
    # exists an error is raised. This method is identical to #filter except
    # it expects an existing filter.
    def and(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      unless @opts[clause]
        raise Error::NoExistingFilter, "No existing filter found."
      end
      filter(*cond, &block)
    end

    def column_all_sql(ca)
      "#{quote_identifier(ca.table)}.*"
    end

    def column_expr_sql(ce)
      r = ce.r
      "#{literal(ce.l)} #{ce.op}#{" #{literal(r)}" if r}"
    end

    def complex_expression_sql(ce)
      op = ce.op
      args = ce.args
      case op
      when *TWO_ARITY_OPERATORS
        "(#{literal(args.at(0))} #{op} #{literal(args.at(1))})"
      when *N_ARITY_OPERATORS
        "(#{args.collect{|a| literal(a)}.join(" #{op} ")})"
      when :NOT
        "NOT #{literal(args.at(0))}"
      else
        raise(Sequel::Error, "invalid operator #{op}")
      end
    end

    # Returns the number of records in the dataset.
    def count
      if @opts[:sql] || @opts[:group]
        from_self.count
      else
        single_value(STOCK_COUNT_OPTS).to_i
      end
    end
    alias_method :size, :count

    # Formats a DELETE statement using the given options and dataset options.
    # 
    #   dataset.filter {price >= 100}.delete_sql #=>
    #     "DELETE FROM items WHERE (price >= 100)"
    def delete_sql(opts = nil)
      opts = opts ? @opts.merge(opts) : @opts

      if opts[:group]
        raise Error::InvalidOperation, "Grouped datasets cannot be deleted from"
      elsif opts[:from].is_a?(Array) && opts[:from].size > 1
        raise Error::InvalidOperation, "Joined datasets cannot be deleted from"
      end

      sql = "DELETE FROM #{source_list(opts[:from])}"

      if where = opts[:where]
        sql << " WHERE #{literal(where)}"
      end

      sql
    end

    # Adds an EXCEPT clause using a second dataset object. If all is true the
    # clause used is EXCEPT ALL, which may return duplicate rows.
    def except(dataset, all = false)
      clone(:except => dataset, :except_all => all)
    end

    # Performs the inverse of Dataset#filter.
    #
    #   dataset.exclude(:category => 'software').sql #=>
    #     "SELECT * FROM items WHERE NOT (category = 'software')"
    def exclude(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      cond = cond.first if cond.size == 1
      if (Hash === cond) || ((Array === cond) && (cond.all_two_pairs?))
        cond = cond.sql_or
      end
      cond = if @opts[clause]
        @opts[clause] & ~filter_expr(block || cond)
      else
        ~filter_expr(block || cond)
      end
      clone(clause => cond)
    end

    # Returns an EXISTS clause for the dataset.
    #
    #   DB.select(1).where(DB[:items].exists).sql
    #   #=> "SELECT 1 WHERE EXISTS (SELECT * FROM items)"
    def exists(opts = nil)
      "EXISTS (#{select_sql(opts)})"
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
      clause = (@opts[:having] ? :having : :where)
      cond = cond.first if cond.size == 1
      if cond === true || cond === false
        raise Error::InvalidFilter, "Invalid filter specified. Did you mean to supply a block?"
      end
      
      if cond.is_a?(Hash)
        cond = transform_save(cond) if @transform
        filter = cond
      end

      if @opts[clause].blank?
        clone(:filter => cond, clause => filter_expr(block || cond))
      else
        clone(clause => @opts[clause] & filter_expr(block || cond))
      end
    end
    alias_method :where, :filter

    def first_source
      source = @opts[:from]
      if source.nil? || source.empty?
        raise Error, 'No source specified for query'
      end
      case s = source.first
      when Hash
        s.values.first
      else
        s
      end
    end

    # Returns a copy of the dataset with the source changed.
    def from(*source)
      clone(:from => source)
    end
    
    # Returns a dataset selecting from the current dataset.
    #
    #   ds = DB[:items].order(:name)
    #   ds.sql #=> "SELECT * FROM items ORDER BY name"
    #   ds.from_self.sql #=> "SELECT * FROM (SELECT * FROM items ORDER BY name)"
    def from_self
      fs = {}
      @opts.keys.each{|k| fs[k] = nil} 
      fs[:from] = [self]
      clone(fs)
    end

    def function_sql(f)
      args = f.args
      "#{f.f}#{args.empty? ? '()' : literal(args)}"
    end

    def grep(cols, terms)
      filter(::Sequel::SQL::ComplexExpression.new(:OR, *Array(cols).collect{|c| ::Sequel::SQL::ComplexExpression.like(c, *terms)}))
    end

    # Returns a copy of the dataset with the results grouped by the value of 
    # the given columns
    def group(*columns)
      clone(:group => columns)
    end
    alias_method :group_by, :group

    # Returns a copy of the dataset with the having conditions changed. Raises 
    # if the dataset has not been grouped. See also #filter
    def having(*cond, &block)
      unless @opts[:group]
        raise Error::InvalidOperation, "Can only specify a HAVING clause on a grouped dataset"
      else
        @opts[:having] = {}
        filter(*cond, &block)
      end
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
        insert_default_values_sql
      else
        values = values[0] if values.size == 1
        
        # if hash or array with keys we need to transform the values
        if @transform && (values.is_a?(Hash) || (values.is_a?(Array) && values.keys))
          values = transform_save(values)
        end
        from = source_list(@opts[:from])

        case values
        when Array
          if values.empty?
            insert_default_values_sql
          else
            "INSERT INTO #{from} VALUES #{literal(values)}"
          end
        when Hash
          if values.empty?
            insert_default_values_sql
          else
            fl, vl = [], []
            values.each {|k, v| fl << literal(k.is_a?(String) ? k.to_sym : k); vl << literal(v)}
            "INSERT INTO #{from} (#{fl.join(COMMA_SEPARATOR)}) VALUES (#{vl.join(COMMA_SEPARATOR)})"
          end
        when Dataset
          "INSERT INTO #{from} #{literal(values)}"
        else
          if values.respond_to?(:values)
            insert_sql(values.values)
          else
            "INSERT INTO #{from} VALUES (#{literal(values)})"
          end
        end
      end
    end
    
    # Adds an INTERSECT clause using a second dataset object. If all is true 
    # the clause used is INTERSECT ALL, which may return duplicate rows.
    def intersect(dataset, all = false)
      clone(:intersect => dataset, :intersect_all => all)
    end

    # Returns a joined dataset with the specified join type and condition.
    def join_table(type, table, expr=nil, table_alias=nil)
      raise(Error::InvalidJoinType, "Invalid join type: #{type}") unless join_type = JOIN_TYPES[type || :inner]

      table = if Dataset === table
        table_alias = unless table_alias
          table_alias_num = (@opts[:num_dataset_sources] || 0) + 1
          "t#{table_alias_num}"
        end
        table.to_table_reference
      else
        table = table.table_name if table.respond_to?(:table_name)
        table_alias ||= table
        quote_identifier(table)
      end

      expr = [[expr, :id]] unless expr.is_one_of?(Hash, Array)
      join_conditions = expr.collect do |k, v|
        k = qualified_column_name(k, table_alias) if k.is_a?(Symbol)
        v = qualified_column_name(v, @opts[:last_joined_table] || first_source) if v.is_a?(Symbol)
        [k,v]
      end

      quoted_table_alias = quote_identifier(table_alias) 
      clause = "#{@opts[:join]} #{join_type} #{table}#{" #{quoted_table_alias}" if quoted_table_alias != table} ON #{literal(filter_expr(join_conditions))}"
      opts = {:join => clause, :last_joined_table => table_alias}
      opts[:num_dataset_sources] = table_alias_num if table_alias_num
      clone(opts)
    end

    # If given an integer, the dataset will contain only the first l results.
    # If given a range, it will contain only those at offsets within that
    # range. If a second argument is given, it is used as an offset.
    def limit(l, o = nil)
      return from_self.limit(l, o) if @opts[:sql]

      if Range === l
        o = l.first
        l = l.interval + 1
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
    
    # Returns a literal representation of a value to be used as part
    # of an SQL expression. The stock implementation supports literalization 
    # of String (with proper escaping to prevent SQL injections), numbers,
    # Symbol (as column references), Array (as a list of literalized values),
    # Time (as an SQL TIMESTAMP), Date (as an SQL DATE), Dataset (as a 
    # subquery) and nil (AS NULL).
    # 
    #   dataset.literal("abc'def\\") #=> "'abc''def\\\\'"
    #   dataset.literal(:items__id) #=> "items.id"
    #   dataset.literal([1, 2, 3]) => "(1, 2, 3)"
    #   dataset.literal(DB[:items]) => "(SELECT * FROM items)"
    #
    # If an unsupported object is given, an exception is raised.
    def literal(v)
      case v
      when LiteralString
        v
      when String
        "'#{v.gsub(/\\/, "\\\\\\\\").gsub(/'/, "''")}'"
      when Integer, Float
        v.to_s
      when BigDecimal
        v.to_s("F")
      when NilClass
        NULL
      when TrueClass
        BOOL_TRUE
      when FalseClass
        BOOL_FALSE
      when Symbol
        symbol_to_column_ref(v)
      when ::Sequel::SQL::Expression
        v.to_s(self)
      when Array
        v.all_two_pairs? ? literal(v.to_complex_expr) : "(#{v.collect{|i| literal(i)}.join(COMMA_SEPARATOR)})"
      when Hash
        literal(v.to_complex_expr)
      when Time
        v.strftime(TIMESTAMP_FORMAT)
      when Date
        v.strftime(DATE_FORMAT)
      when Dataset
        "(#{v.sql})"
      else
        raise Error, "can't express #{v.inspect} as a SQL literal"
      end
    end

    # Returns an array of insert statements for inserting multiple records.
    # This method is used by #multi_insert to format insert statements and
    # expects a keys array and and an array of value arrays.
    #
    # This method may be overriden by descendants.
    def multi_insert_sql(columns, values)
      table = quote_identifier(@opts[:from].first)
      columns = literal(columns)
      values.map do |r|
        "INSERT INTO #{table} #{columns} VALUES #{literal(r)}"
      end
    end
    
    # Adds an alternate filter to an existing filter using OR. If no filter 
    # exists an error is raised.
    def or(*cond, &block)
      clause = (@opts[:having] ? :having : :where)
      cond = cond.first if cond.size == 1
      if @opts[clause]
        clone(clause => @opts[clause] | filter_expr(block || cond))
      else
        raise Error::NoExistingFilter, "No existing filter found."
      end
    end

    # Returns a copy of the dataset with the order changed. If a nil is given
    # the returned dataset has no order. This can accept multiple arguments
    # of varying kinds, and even SQL functions.
    #
    #   ds.order(:name).sql #=> 'SELECT * FROM items ORDER BY name'
    #   ds.order(:a, :b).sql #=> 'SELECT * FROM items ORDER BY a, b'
    #   ds.order('a + b'.lit).sql #=> 'SELECT * FROM items ORDER BY a + b'
    #   ds.order(:name.desc).sql #=> 'SELECT * FROM items ORDER BY name DESC'
    #   ds.order(:name.asc).sql #=> 'SELECT * FROM items ORDER BY name ASC'
    #   ds.order(:arr|1).sql #=> 'SELECT * FROM items ORDER BY arr[1]'
    #   ds.order(nil).sql #=> 'SELECT * FROM items'
    def order(*order)
      clone(:order => (order.compact.empty?) ? nil : order)
    end
    alias_method :order_by, :order
    
    # Returns a copy of the dataset with the order changed.
    def order_more(*order)
      if @opts[:order]
        clone(:order => @opts[:order] + order)
      else
        clone(:order => order)
      end
    end
    
    def qualified_column_ref_sql(qcr)
      "#{quote_identifier(qcr.table)}.#{quote_identifier(qcr.column)}"
    end

    # Adds quoting to identifiers (columns and tables). If identifiers are not
    # being quoted, returns name as a string.  If identifiers are being quoted
    # quote the name with the SQL standard double quote. This method
    # should be overriden by subclasses to provide quoting not matching the
    # SQL standard, such as backtick (used by MySQL and SQLite). 
    def quote_identifier(name)
      quote_identifiers? ? quoted_identifier(name) : name.to_s
    end
    alias_method :quote_column_ref, :quote_identifier

    def quoted_identifier(name)
      "\"#{name}\""
    end

    # Returns a copy of the dataset with the order reversed. If no order is
    # given, the existing order is inverted.
    def reverse_order(*order)
      order(*invert_order(order.empty? ? @opts[:order] : order))
    end
    alias_method :reverse, :reverse_order

    # Returns a copy of the dataset with the selected columns changed.
    def select(*columns)
      clone(:select => columns)
    end
    
    # Returns a copy of the dataset selecting the wildcard.
    def select_all
      clone(:select => nil)
    end

    # Returns a copy of the dataset with additional selected columns.
    def select_more(*columns)
      if @opts[:select]
        clone(:select => @opts[:select] + columns)
      else
        clone(:select => columns)
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
        sql << " WHERE #{literal(where)}"
      end

      if group = opts[:group]
        sql << " GROUP BY #{column_list(group)}"
      end

      if order = opts[:order]
        sql << " ORDER BY #{column_list(order)}"
      end

      if having = opts[:having]
        sql << " HAVING #{literal(having)}"
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

    def subscript_sql(s)
      "#{s.f}[#{s.sub.join(COMMA_SEPARATOR)}]"
    end

    # Converts a symbol into a column name. This method supports underscore
    # notation in order to express qualified (two underscores) and aliased
    # (three underscores) columns:
    #
    #   ds = DB[:items]
    #   :abc.to_column_ref(ds) #=> "abc"
    #   :abc___a.to_column_ref(ds) #=> "abc AS a"
    #   :items__abc.to_column_ref(ds) #=> "items.abc"
    #   :items__abc___a.to_column_ref(ds) #=> "items.abc AS a"
    #
    def symbol_to_column_ref(sym, table=(qualify=false; nil))
      s = sym.to_s
      if m = COLUMN_REF_RE1.match(s)
        table, column, aliaz = m[1], m[2], m[3]
      elsif m = COLUMN_REF_RE2.match(s)
        column, aliaz = m[1], m[2]
      elsif m = COLUMN_REF_RE3.match(s)
        table, column = m[1], m[2]
      else
        column = s
      end
      if qualify == false
        "#{"#{quote_identifier(table)}." if table}#{quote_identifier(column)}#{" AS #{quote_identifier(aliaz)}" if aliaz}"
      else
        ::Sequel::SQL::QualifiedColumnRef.new(table, column)
      end
    end

    # Returns a copy of the dataset with no filters (HAVING or WHERE clause) applied.
    def unfiltered
      clone(:where => nil, :having => nil)
    end

    # Adds a UNION clause using a second dataset object. If all is true the
    # clause used is UNION ALL, which may return duplicate rows.
    def union(dataset, all = false)
      clone(:union => dataset, :union_all => all)
    end

    # Returns a copy of the dataset with the distinct option.
    def uniq(*args)
      clone(:distinct => args)
    end
    alias_method :distinct, :uniq

    # Returns a copy of the dataset with no order.
    def unordered
      clone(:order => nil)
    end

    # Formats an UPDATE statement using the given values.
    #
    #   dataset.update_sql(:price => 100, :category => 'software') #=>
    #     "UPDATE items SET price = 100, category = 'software'"
    def update_sql(values = {}, opts = nil, &block)
      opts = opts ? @opts.merge(opts) : @opts

      if opts[:group]
        raise Error::InvalidOperation, "A grouped dataset cannot be updated"
      elsif (opts[:from].size > 1) or opts[:join]
        raise Error::InvalidOperation, "A joined dataset cannot be updated"
      end
      
      sql = "UPDATE #{source_list(@opts[:from])} SET "
      if block
        sql << block.to_sql(self, :comma_separated => true)
      else
        # check if array with keys
        values = values.to_hash if values.is_a?(Array) && values.keys
        if values.is_a?(Hash)
          # get values from hash
          values = transform_save(values) if @transform
          set = values.map do |k, v|
            # convert string key into symbol
            k = k.to_sym if String === k
            "#{literal(k)} = #{literal(v)}"
          end.join(COMMA_SEPARATOR)
        else
          # copy values verbatim
          set = values
        end
        sql << set
      end
      if where = opts[:where]
        sql << " WHERE #{literal(where)}"
      end

      sql
    end

    [:inner, :full_outer, :right_outer, :left_outer].each do |jtype|
      define_method("#{jtype}_join"){|*args| join_table(jtype, *args)}
    end
    alias_method :join, :inner_join

    protected

    # Returns a table reference for use in the FROM clause. If the dataset has
    # only a :from option refering to a single table, only the table name is 
    # returned. Otherwise a subquery is returned.
    def to_table_reference(table_alias=nil)
      table_alias ? "(#{sql}) #{quote_identifier(table_alias)}" : "(#{sql})"
    end

    private
    # Converts an array of column names into a comma seperated string of 
    # column names. If the array is empty, a wildcard (*) is returned.
    def column_list(columns)
      if columns.empty?
        WILDCARD
      else
        m = columns.map do |i|
          i.is_a?(Hash) ? i.map{|kv| "#{literal(kv[0])} AS #{quote_identifier(kv[1])}"} : literal(i)
        end
        m.join(COMMA_SEPARATOR)
      end
    end
    
    def filter_expr(expr)
      case expr
      when Hash
        ::Sequel::SQL::ComplexExpression.from_value_pairs(expr)
      when Array
        if String === expr[0]
          filter_expr(expr.shift.gsub(QUESTION_MARK){literal(expr.shift)}.lit)
        else
          ::Sequel::SQL::ComplexExpression.from_value_pairs(expr)
        end
      when Proc
        expr.to_sql(self).lit
      when Symbol, ::Sequel::SQL::Expression
        expr
      when String, ::Sequel::LiteralString
        "(#{expr})".lit
      else
        raise(Sequel::Error, 'Invalid filter argument')
      end
    end

    # Returns the SQL for formatting an insert statement with default values
    def insert_default_values_sql
      "INSERT INTO #{source_list(@opts[:from])} DEFAULT VALUES"
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
        if f.is_a?(Sequel::SQL::ColumnExpr) && (f.op == Sequel::SQL::ColumnMethods::DESC)
          f.l
        elsif f.is_a?(Sequel::SQL::ColumnExpr) && (f.op == Sequel::SQL::ColumnMethods::ASC)
          f.l.desc
        else
          f.desc
        end
      end
    end
    
    # Returns a qualified column name (including a table name) if the column
    # name isn't already qualified.
    def qualified_column_name(column, table)
      Symbol === column ? symbol_to_column_ref(column, table) : column
    end

    # Converts an array of sources names into into a comma separated list.
    def source_list(source)
      if source.nil? || source.empty?
        raise Error, 'No source specified for query'
      end
      auto_alias_count = @opts[:num_dataset_sources] || 0
      m = source.map do |s|
        case s
        when Dataset
          auto_alias_count += 1
          s.to_table_reference("t#{auto_alias_count}")
        else
          table_ref(s)
        end
      end
      m.join(COMMA_SEPARATOR)
    end
    
    def table_ref(t)
      case t
      when Dataset
        t.to_table_reference
      when Hash
        t.map {|k, v| "#{table_ref(k)} #{table_ref(v)}"}.join(COMMA_SEPARATOR)
      when Symbol, String
        quote_identifier(t)
      else
        literal(t)
      end
    end
  end
end
