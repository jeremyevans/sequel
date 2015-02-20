module Sequel
  class Dataset
    # ---------------------
    # :section: 2 - Methods that execute code on the database
    # These methods all execute the dataset's SQL on the database.
    # They don't return modified datasets, so if used in a method chain
    # they should be the last method called.
    # ---------------------
    
    # Action methods defined by Sequel that execute code on the database.
    ACTION_METHODS = (<<-METHS).split.map{|x| x.to_sym}
      << [] all avg count columns columns! delete each
      empty? fetch_rows first first! get import insert interval last
      map max min multi_insert paged_each range select_hash select_hash_groups select_map select_order_map
      single_record single_value sum to_hash to_hash_groups truncate update
    METHS

    # Inserts the given argument into the database.  Returns self so it
    # can be used safely when chaining:
    # 
    #   DB[:items] << {:id=>0, :name=>'Zero'} << DB[:old_items].select(:id, name)
    def <<(arg)
      insert(arg)
      self
    end
    
    # Returns the first record matching the conditions. Examples:
    #
    #   DB[:table][:id=>1] # SELECT * FROM table WHERE (id = 1) LIMIT 1
    #   # => {:id=1}
    def [](*conditions)
      raise(Error, ARRAY_ACCESS_ERROR_MSG) if (conditions.length == 1 and conditions.first.is_a?(Integer)) or conditions.length == 0
      first(*conditions)
    end

    # Returns an array with all records in the dataset. If a block is given,
    # the array is iterated over after all items have been loaded.
    #
    #   DB[:table].all # SELECT * FROM table
    #   # => [{:id=>1, ...}, {:id=>2, ...}, ...]
    #
    #   # Iterate over all rows in the table
    #   DB[:table].all{|row| p row}
    def all(&block)
      _all(block){|a| each{|r| a << r}}
    end
    
    # Returns the average value for the given column/expression.
    # Uses a virtual row block if no argument is given.
    #
    #   DB[:table].avg(:number) # SELECT avg(number) FROM table LIMIT 1
    #   # => 3
    #   DB[:table].avg{function(column)} # SELECT avg(function(column)) FROM table LIMIT 1
    #   # => 1
    def avg(column=Sequel.virtual_row(&Proc.new))
      aggregate_dataset.get{avg(column).as(:avg)}
    end
  
    # Returns the columns in the result set in order as an array of symbols.
    # If the columns are currently cached, returns the cached value. Otherwise,
    # a SELECT query is performed to retrieve a single row in order to get the columns.
    #
    # If you are looking for all columns for a single table and maybe some information about
    # each column (e.g. database type), see <tt>Database#schema</tt>.
    #
    #   DB[:table].columns
    #   # => [:id, :name]
    def columns
      return @columns if @columns
      ds = unfiltered.unordered.naked.clone(:distinct => nil, :limit => 1, :offset=>nil)
      ds.each{break}
      @columns = ds.instance_variable_get(:@columns)
      @columns || []
    end
        
    # Ignore any cached column information and perform a query to retrieve
    # a row in order to get the columns.
    #
    #   DB[:table].columns!
    #   # => [:id, :name]
    def columns!
      @columns = nil
      columns
    end
    
    # Returns the number of records in the dataset. If an argument is provided,
    # it is used as the argument to count.  If a block is provided, it is
    # treated as a virtual row, and the result is used as the argument to
    # count.
    #
    #   DB[:table].count # SELECT count(*) AS count FROM table LIMIT 1
    #   # => 3
    #   DB[:table].count(:column) # SELECT count(column) AS count FROM table LIMIT 1
    #   # => 2
    #   DB[:table].count{foo(column)} # SELECT count(foo(column)) AS count FROM table LIMIT 1
    #   # => 1
    def count(arg=(no_arg=true), &block)
      if no_arg
        if block
          arg = Sequel.virtual_row(&block)
          aggregate_dataset.get{count(arg).as(:count)}
        else
          aggregate_dataset.get{count{}.*.as(:count)}.to_i
        end
      elsif block
        raise Error, 'cannot provide both argument and block to Dataset#count'
      else
        aggregate_dataset.get{count(arg).as(:count)}
      end
    end
    
    # Deletes the records in the dataset.  The returned value should be 
    # number of records deleted, but that is adapter dependent.
    #
    #   DB[:table].delete # DELETE * FROM table
    #   # => 3
    def delete(&block)
      sql = delete_sql
      if uses_returning?(:delete)
        returning_fetch_rows(sql, &block)
      else
        execute_dui(sql)
      end
    end

    # Iterates over the records in the dataset as they are yielded from the
    # database adapter, and returns self.
    #
    #   DB[:table].each{|row| p row} # SELECT * FROM table
    #
    # Note that this method is not safe to use on many adapters if you are
    # running additional queries inside the provided block.  If you are
    # running queries inside the block, you should use +all+ instead of +each+
    # for the outer queries, or use a separate thread or shard inside +each+.
    def each
      if row_proc = @row_proc
        fetch_rows(select_sql){|r| yield row_proc.call(r)}
      else
        fetch_rows(select_sql){|r| yield r}
      end
      self
    end
    
    # Returns true if no records exist in the dataset, false otherwise
    #
    #   DB[:table].empty? # SELECT 1 AS one FROM table LIMIT 1
    #   # => false
    def empty?
      ds = @opts[:order] ? unordered : self
      ds.get(Sequel::SQL::AliasedExpression.new(1, :one)).nil?
    end

    # If a integer argument is given, it is interpreted as a limit, and then returns all 
    # matching records up to that limit.  If no argument is passed,
    # it returns the first matching record.  If any other type of
    # argument(s) is passed, it is given to filter and the
    # first matching record is returned.  If a block is given, it is used
    # to filter the dataset before returning anything.
    #
    # If there are no records in the dataset, returns nil (or an empty
    # array if an integer argument is given).
    #
    # Examples:
    # 
    #   DB[:table].first # SELECT * FROM table LIMIT 1
    #   # => {:id=>7}
    #
    #   DB[:table].first(2) # SELECT * FROM table LIMIT 2
    #   # => [{:id=>6}, {:id=>4}]
    #
    #   DB[:table].first(:id=>2) # SELECT * FROM table WHERE (id = 2) LIMIT 1
    #   # => {:id=>2}
    #
    #   DB[:table].first("id = 3") # SELECT * FROM table WHERE (id = 3) LIMIT 1
    #   # => {:id=>3}
    #
    #   DB[:table].first("id = ?", 4) # SELECT * FROM table WHERE (id = 4) LIMIT 1
    #   # => {:id=>4}
    #
    #   DB[:table].first{id > 2} # SELECT * FROM table WHERE (id > 2) LIMIT 1
    #   # => {:id=>5}
    #
    #   DB[:table].first("id > ?", 4){id < 6} # SELECT * FROM table WHERE ((id > 4) AND (id < 6)) LIMIT 1
    #   # => {:id=>5}
    #
    #   DB[:table].first(2){id < 2} # SELECT * FROM table WHERE (id < 2) LIMIT 2
    #   # => [{:id=>1}]
    def first(*args, &block)
      ds = block ? filter(&block) : self

      if args.empty?
        ds.single_record
      else
        args = (args.size == 1) ? args.first : args
        if args.is_a?(Integer)
          ds.limit(args).all
        else
          ds.filter(args).single_record
        end
      end
    end

    # Calls first.  If first returns nil (signaling that no
    # row matches), raise a Sequel::NoMatchingRow exception.
    def first!(*args, &block)
      first(*args, &block) || raise(Sequel::NoMatchingRow)
    end

    # Return the column value for the first matching record in the dataset.
    # Raises an error if both an argument and block is given.
    #
    #   DB[:table].get(:id) # SELECT id FROM table LIMIT 1
    #   # => 3
    #
    #   ds.get{sum(id)} # SELECT sum(id) AS v FROM table LIMIT 1
    #   # => 6
    #
    # You can pass an array of arguments to return multiple arguments,
    # but you must make sure each element in the array has an alias that
    # Sequel can determine:
    #
    #   DB[:table].get([:id, :name]) # SELECT id, name FROM table LIMIT 1
    #   # => [3, 'foo']
    #
    #   DB[:table].get{[sum(id).as(sum), name]} # SELECT sum(id) AS sum, name FROM table LIMIT 1
    #   # => [6, 'foo']
    def get(column=(no_arg=true; nil), &block)
      ds = naked
      if block
        raise(Error, ARG_BLOCK_ERROR_MSG) unless no_arg
        ds = ds.select(&block)
        column = ds.opts[:select]
        column = nil if column.is_a?(Array) && column.length < 2
      else
        ds = if column.is_a?(Array)
          ds.select(*column)
        else
          ds.select(auto_alias_expression(column))
        end
      end

      if column.is_a?(Array)
       if r = ds.single_record
         r.values_at(*hash_key_symbols(column))
       end
      else
        ds.single_value
      end
    end
    
    # Inserts multiple records into the associated table. This method can be
    # used to efficiently insert a large number of records into a table in a
    # single query if the database supports it. Inserts
    # are automatically wrapped in a transaction.
    # 
    # This method is called with a columns array and an array of value arrays:
    #
    #   DB[:table].import([:x, :y], [[1, 2], [3, 4]])
    #   # INSERT INTO table (x, y) VALUES (1, 2) 
    #   # INSERT INTO table (x, y) VALUES (3, 4) 
    #
    # This method also accepts a dataset instead of an array of value arrays:
    #
    #   DB[:table].import([:x, :y], DB[:table2].select(:a, :b))
    #   # INSERT INTO table (x, y) SELECT a, b FROM table2 
    #
    # Options:
    # :commit_every :: Open a new transaction for every given number of records.
    #                  For example, if you provide a value of 50, will commit
    #                  after every 50 records.
    # :return :: When the :value is :primary_key, returns an array of
    #            autoincremented primary key values for the rows inserted.
    # :server :: Set the server/shard to use for the transaction and insert
    #            queries.
    # :slice :: Same as :commit_every, :commit_every takes precedence.
    def import(columns, values, opts=OPTS)
      return @db.transaction{insert(columns, values)} if values.is_a?(Dataset)

      return if values.empty?
      raise(Error, IMPORT_ERROR_MSG) if columns.empty?
      ds = opts[:server] ? server(opts[:server]) : self
      
      if slice_size = opts.fetch(:commit_every, opts.fetch(:slice, default_import_slice))
        offset = 0
        rows = []
        while offset < values.length
          rows << ds._import(columns, values[offset, slice_size], opts)
          offset += slice_size
        end
        rows.flatten
      else
        ds._import(columns, values, opts)
      end
    end

    # Inserts values into the associated table.  The returned value is generally
    # the value of the primary key for the inserted row, but that is adapter dependent.
    #
    # +insert+ handles a number of different argument formats:
    # no arguments or single empty hash :: Uses DEFAULT VALUES
    # single hash :: Most common format, treats keys as columns an values as values
    # single array :: Treats entries as values, with no columns
    # two arrays :: Treats first array as columns, second array as values
    # single Dataset :: Treats as an insert based on a selection from the dataset given,
    #                   with no columns
    # array and dataset :: Treats as an insert based on a selection from the dataset
    #                      given, with the columns given by the array.
    #
    # Examples:
    #
    #   DB[:items].insert
    #   # INSERT INTO items DEFAULT VALUES
    #
    #   DB[:items].insert({})
    #   # INSERT INTO items DEFAULT VALUES
    #
    #   DB[:items].insert([1,2,3])
    #   # INSERT INTO items VALUES (1, 2, 3)
    #
    #   DB[:items].insert([:a, :b], [1,2])
    #   # INSERT INTO items (a, b) VALUES (1, 2)
    #
    #   DB[:items].insert(:a => 1, :b => 2)
    #   # INSERT INTO items (a, b) VALUES (1, 2)
    #
    #   DB[:items].insert(DB[:old_items])
    #   # INSERT INTO items SELECT * FROM old_items
    #
    #   DB[:items].insert([:a, :b], DB[:old_items])
    #   # INSERT INTO items (a, b) SELECT * FROM old_items
    def insert(*values, &block)
      sql = insert_sql(*values)
      if uses_returning?(:insert)
        returning_fetch_rows(sql, &block)
      else
        execute_insert(sql)
      end
    end
    
    # Returns the interval between minimum and maximum values for the given 
    # column/expression. Uses a virtual row block if no argument is given.
    #
    #   DB[:table].interval(:id) # SELECT (max(id) - min(id)) FROM table LIMIT 1
    #   # => 6
    #   DB[:table].interval{function(column)} # SELECT (max(function(column)) - min(function(column))) FROM table LIMIT 1
    #   # => 7
    def interval(column=Sequel.virtual_row(&Proc.new))
      aggregate_dataset.get{(max(column) - min(column)).as(:interval)}
    end

    # Reverses the order and then runs #first with the given arguments and block.  Note that this
    # will not necessarily give you the last record in the dataset,
    # unless you have an unambiguous order.  If there is not
    # currently an order for this dataset, raises an +Error+.
    #
    #   DB[:table].order(:id).last # SELECT * FROM table ORDER BY id DESC LIMIT 1
    #   # => {:id=>10}
    #
    #   DB[:table].order(Sequel.desc(:id)).last(2) # SELECT * FROM table ORDER BY id ASC LIMIT 2
    #   # => [{:id=>1}, {:id=>2}]
    def last(*args, &block)
      raise(Error, 'No order specified') unless @opts[:order]
      reverse.first(*args, &block)
    end
    
    # Maps column values for each record in the dataset (if a column name is
    # given), or performs the stock mapping functionality of +Enumerable+ otherwise. 
    # Raises an +Error+ if both an argument and block are given.
    #
    #   DB[:table].map(:id) # SELECT * FROM table
    #   # => [1, 2, 3, ...]
    #
    #   DB[:table].map{|r| r[:id] * 2} # SELECT * FROM table
    #   # => [2, 4, 6, ...]
    #
    # You can also provide an array of column names:
    #
    #   DB[:table].map([:id, :name]) # SELECT * FROM table
    #   # => [[1, 'A'], [2, 'B'], [3, 'C'], ...]
    def map(column=nil, &block)
      if column
        raise(Error, ARG_BLOCK_ERROR_MSG) if block
        return naked.map(column) if row_proc
        if column.is_a?(Array)
          super(){|r| r.values_at(*column)}
        else
          super(){|r| r[column]}
        end
      else
        super(&block)
      end
    end

    # Returns the maximum value for the given column/expression.
    # Uses a virtual row block if no argument is given.
    #
    #   DB[:table].max(:id) # SELECT max(id) FROM table LIMIT 1
    #   # => 10
    #   DB[:table].max{function(column)} # SELECT max(function(column)) FROM table LIMIT 1
    #   # => 7
    def max(column=Sequel.virtual_row(&Proc.new))
      aggregate_dataset.get{max(column).as(:max)}
    end

    # Returns the minimum value for the given column/expression.
    # Uses a virtual row block if no argument is given.
    #
    #   DB[:table].min(:id) # SELECT min(id) FROM table LIMIT 1
    #   # => 1
    #   DB[:table].min{function(column)} # SELECT min(function(column)) FROM table LIMIT 1
    #   # => 0
    def min(column=Sequel.virtual_row(&Proc.new))
      aggregate_dataset.get{min(column).as(:min)}
    end

    # This is a front end for import that allows you to submit an array of
    # hashes instead of arrays of columns and values:
    # 
    #   DB[:table].multi_insert([{:x => 1}, {:x => 2}])
    #   # INSERT INTO table (x) VALUES (1)
    #   # INSERT INTO table (x) VALUES (2)
    #
    # Be aware that all hashes should have the same keys if you use this calling method,
    # otherwise some columns could be missed or set to null instead of to default
    # values.
    #
    # This respects the same options as #import.
    def multi_insert(hashes, opts=OPTS)
      return if hashes.empty?
      columns = hashes.first.keys
      import(columns, hashes.map{|h| columns.map{|c| h[c]}}, opts)
    end

    # Yields each row in the dataset, but interally uses multiple queries as needed to
    # process the entire result set without keeping all rows in the dataset in memory,
    # even if the underlying driver buffers all query results in memory.
    #
    # Because this uses multiple queries internally, in order to remain consistent,
    # it also uses a transaction internally.  Additionally, to work correctly, the dataset
    # must have unambiguous order.  Using an ambiguous order can result in an infinite loop,
    # as well as subtler bugs such as yielding duplicate rows or rows being skipped.
    #
    # Sequel checks that the datasets using this method have an order, but it cannot
    # ensure that the order is unambiguous.
    #
    # Options:
    # :rows_per_fetch :: The number of rows to fetch per query.  Defaults to 1000.
    # :strategy :: The strategy to use for paging of results.  By default this is :offset,
    #              for using an approach with a limit and offset for every page.  This can
    #              be set to :filter, which uses a limit and a filter that excludes
    #              rows from previous pages.  In order for this strategy to work, you must be
    #              selecting the columns you are ordering by, and none of the columns can contain
    #              NULLs.  Note that some Sequel adapters have optimized implementations that will
    #              use cursors or streaming regardless of the :strategy option used.
    # :filter_values :: If the :strategy=>:filter option is used, this option should be a proc
    #                   that accepts the last retreived row for the previous page and an array of
    #                   ORDER BY expressions, and returns an array of values relating to those
    #                   expressions for the last retrieved row.  You will need to use this option
    #                   if your ORDER BY expressions are not simple columns, if they contain
    #                   qualified identifiers that would be ambiguous unqualified, if they contain
    #                   any identifiers that are aliased in SELECT, and potentially other cases.
    #
    # Examples:
    #
    #   DB[:table].order(:id).paged_each{|row| }
    #   # SELECT * FROM table ORDER BY id LIMIT 1000
    #   # SELECT * FROM table ORDER BY id LIMIT 1000 OFFSET 1000
    #   # ...
    #
    #   DB[:table].order(:id).paged_each(:rows_per_fetch=>100){|row| }
    #   # SELECT * FROM table ORDER BY id LIMIT 100
    #   # SELECT * FROM table ORDER BY id LIMIT 100 OFFSET 100
    #   # ...
    #
    #   DB[:table].order(:id).paged_each(:strategy=>:filter){|row| }
    #   # SELECT * FROM table ORDER BY id LIMIT 1000
    #   # SELECT * FROM table WHERE id > 1001 ORDER BY id LIMIT 1000
    #   # ...
    #
    #   DB[:table].order(:table__id).paged_each(:strategy=>:filter,
    #     :filter_values=>proc{|row, exprs| [row[:id]]}){|row| }
    #   # SELECT * FROM table ORDER BY id LIMIT 1000
    #   # SELECT * FROM table WHERE id > 1001 ORDER BY id LIMIT 1000
    #   # ...
    def paged_each(opts=OPTS)
      unless @opts[:order]
        raise Sequel::Error, "Dataset#paged_each requires the dataset be ordered"
      end
      unless block_given?
        return enum_for(:paged_each, opts)
      end

      total_limit = @opts[:limit]
      offset = @opts[:offset]
      if server = @opts[:server]
        opts = opts.merge(:server=>server)
      end

      rows_per_fetch = opts[:rows_per_fetch] || 1000
      strategy = if offset || total_limit
        :offset
      else
        opts[:strategy] || :offset
      end

      db.transaction(opts) do
        case strategy
        when :filter
          filter_values = opts[:filter_values] || proc{|row, exprs| exprs.map{|e| row[hash_key_symbol(e)]}}
          base_ds = ds = limit(rows_per_fetch)
          while ds
            last_row = nil
            ds.each do |row|
              last_row = row
              yield row
            end
            ds = (base_ds.where(ignore_values_preceding(last_row, &filter_values)) if last_row)
          end
        else
          offset ||= 0
          num_rows_yielded = rows_per_fetch
          total_rows = 0

          while num_rows_yielded == rows_per_fetch && (total_limit.nil? || total_rows < total_limit)
            if total_limit && total_rows + rows_per_fetch > total_limit
              rows_per_fetch = total_limit - total_rows
            end

            num_rows_yielded = 0
            limit(rows_per_fetch, offset).each do |row|
              num_rows_yielded += 1
              total_rows += 1 if total_limit
              yield row
            end

            offset += rows_per_fetch
          end
        end
      end

      self
    end

    # Returns a +Range+ instance made from the minimum and maximum values for the
    # given column/expression.  Uses a virtual row block if no argument is given.
    #
    #   DB[:table].range(:id) # SELECT max(id) AS v1, min(id) AS v2 FROM table LIMIT 1
    #   # => 1..10
    #   DB[:table].interval{function(column)} # SELECT max(function(column)) AS v1, min(function(column)) AS v2 FROM table LIMIT 1
    #   # => 0..7
    def range(column=Sequel.virtual_row(&Proc.new))
      if r = aggregate_dataset.select{[min(column).as(v1), max(column).as(v2)]}.first
        (r[:v1]..r[:v2])
      end
    end
    
    # Returns a hash with key_column values as keys and value_column values as
    # values.  Similar to to_hash, but only selects the columns given.
    #
    #   DB[:table].select_hash(:id, :name) # SELECT id, name FROM table
    #   # => {1=>'a', 2=>'b', ...}
    #
    # You can also provide an array of column names for either the key_column,
    # the value column, or both:
    #
    #   DB[:table].select_hash([:id, :foo], [:name, :bar]) # SELECT * FROM table
    #   # {[1, 3]=>['a', 'c'], [2, 4]=>['b', 'd'], ...}
    #
    # When using this method, you must be sure that each expression has an alias
    # that Sequel can determine.  Usually you can do this by calling the #as method
    # on the expression and providing an alias.
    def select_hash(key_column, value_column)
      _select_hash(:to_hash, key_column, value_column)
    end
    
    # Returns a hash with key_column values as keys and an array of value_column values.
    # Similar to to_hash_groups, but only selects the columns given.
    #
    #   DB[:table].select_hash_groups(:name, :id) # SELECT id, name FROM table
    #   # => {'a'=>[1, 4, ...], 'b'=>[2, ...], ...}
    #
    # You can also provide an array of column names for either the key_column,
    # the value column, or both:
    #
    #   DB[:table].select_hash_groups([:first, :middle], [:last, :id]) # SELECT * FROM table
    #   # {['a', 'b']=>[['c', 1], ['d', 2], ...], ...}
    #
    # When using this method, you must be sure that each expression has an alias
    # that Sequel can determine.  Usually you can do this by calling the #as method
    # on the expression and providing an alias.
    def select_hash_groups(key_column, value_column)
      _select_hash(:to_hash_groups, key_column, value_column)
    end

    # Selects the column given (either as an argument or as a block), and
    # returns an array of all values of that column in the dataset.  If you
    # give a block argument that returns an array with multiple entries,
    # the contents of the resulting array are undefined.  Raises an Error
    # if called with both an argument and a block.
    #
    #   DB[:table].select_map(:id) # SELECT id FROM table
    #   # => [3, 5, 8, 1, ...]
    #
    #   DB[:table].select_map{id * 2} # SELECT (id * 2) FROM table
    #   # => [6, 10, 16, 2, ...]
    #
    # You can also provide an array of column names:
    #
    #   DB[:table].select_map([:id, :name]) # SELECT id, name FROM table
    #   # => [[1, 'A'], [2, 'B'], [3, 'C'], ...]
    #
    # If you provide an array of expressions, you must be sure that each entry
    # in the array has an alias that Sequel can determine.  Usually you can do this
    # by calling the #as method on the expression and providing an alias.
    def select_map(column=nil, &block)
      _select_map(column, false, &block)
    end
    
    # The same as select_map, but in addition orders the array by the column.
    #
    #   DB[:table].select_order_map(:id) # SELECT id FROM table ORDER BY id
    #   # => [1, 2, 3, 4, ...]
    #
    #   DB[:table].select_order_map{id * 2} # SELECT (id * 2) FROM table ORDER BY (id * 2)
    #   # => [2, 4, 6, 8, ...]
    #
    # You can also provide an array of column names:
    #
    #   DB[:table].select_order_map([:id, :name]) # SELECT id, name FROM table ORDER BY id, name
    #   # => [[1, 'A'], [2, 'B'], [3, 'C'], ...]
    #
    # If you provide an array of expressions, you must be sure that each entry
    # in the array has an alias that Sequel can determine.  Usually you can do this
    # by calling the #as method on the expression and providing an alias.
    def select_order_map(column=nil, &block)
      _select_map(column, true, &block)
    end

    # Returns the first record in the dataset, or nil if the dataset
    # has no records. Users should probably use +first+ instead of
    # this method.
    def single_record
      clone(:limit=>1).each{|r| return r}
      nil
    end

    # Returns the first value of the first record in the dataset.
    # Returns nil if dataset is empty.  Users should generally use
    # +get+ instead of this method.
    def single_value
      if r = ungraphed.naked.single_record
        r.values.first
      end
    end
    
    # Returns the sum for the given column/expression.
    # Uses a virtual row block if no column is given.
    #
    #   DB[:table].sum(:id) # SELECT sum(id) FROM table LIMIT 1
    #   # => 55
    #   DB[:table].sum{function(column)} # SELECT sum(function(column)) FROM table LIMIT 1
    #   # => 10
    def sum(column=Sequel.virtual_row(&Proc.new))
      aggregate_dataset.get{sum(column).as(:sum)}
    end

    # Returns a hash with one column used as key and another used as value.
    # If rows have duplicate values for the key column, the latter row(s)
    # will overwrite the value of the previous row(s). If the value_column
    # is not given or nil, uses the entire hash as the value.
    #
    #   DB[:table].to_hash(:id, :name) # SELECT * FROM table
    #   # {1=>'Jim', 2=>'Bob', ...}
    #
    #   DB[:table].to_hash(:id) # SELECT * FROM table
    #   # {1=>{:id=>1, :name=>'Jim'}, 2=>{:id=>2, :name=>'Bob'}, ...}
    #
    # You can also provide an array of column names for either the key_column,
    # the value column, or both:
    #
    #   DB[:table].to_hash([:id, :foo], [:name, :bar]) # SELECT * FROM table
    #   # {[1, 3]=>['Jim', 'bo'], [2, 4]=>['Bob', 'be'], ...}
    #
    #   DB[:table].to_hash([:id, :name]) # SELECT * FROM table
    #   # {[1, 'Jim']=>{:id=>1, :name=>'Jim'}, [2, 'Bob'=>{:id=>2, :name=>'Bob'}, ...}
    def to_hash(key_column, value_column = nil)
      h = {}
      if value_column
        return naked.to_hash(key_column, value_column) if row_proc
        if value_column.is_a?(Array)
          if key_column.is_a?(Array)
            each{|r| h[r.values_at(*key_column)] = r.values_at(*value_column)}
          else
            each{|r| h[r[key_column]] = r.values_at(*value_column)}
          end
        else
          if key_column.is_a?(Array)
            each{|r| h[r.values_at(*key_column)] = r[value_column]}
          else
            each{|r| h[r[key_column]] = r[value_column]}
          end
        end
      elsif key_column.is_a?(Array)
        each{|r| h[r.values_at(*key_column)] = r}
      else
        each{|r| h[r[key_column]] = r}
      end
      h
    end

    # Returns a hash with one column used as key and the values being an
    # array of column values. If the value_column is not given or nil, uses
    # the entire hash as the value.
    #
    #   DB[:table].to_hash_groups(:name, :id) # SELECT * FROM table
    #   # {'Jim'=>[1, 4, 16, ...], 'Bob'=>[2], ...}
    #
    #   DB[:table].to_hash_groups(:name) # SELECT * FROM table
    #   # {'Jim'=>[{:id=>1, :name=>'Jim'}, {:id=>4, :name=>'Jim'}, ...], 'Bob'=>[{:id=>2, :name=>'Bob'}], ...}
    #
    # You can also provide an array of column names for either the key_column,
    # the value column, or both:
    #
    #   DB[:table].to_hash_groups([:first, :middle], [:last, :id]) # SELECT * FROM table
    #   # {['Jim', 'Bob']=>[['Smith', 1], ['Jackson', 4], ...], ...}
    #
    #   DB[:table].to_hash_groups([:first, :middle]) # SELECT * FROM table
    #   # {['Jim', 'Bob']=>[{:id=>1, :first=>'Jim', :middle=>'Bob', :last=>'Smith'}, ...], ...}
    def to_hash_groups(key_column, value_column = nil)
      h = {}
      if value_column
        return naked.to_hash_groups(key_column, value_column) if row_proc
        if value_column.is_a?(Array)
          if key_column.is_a?(Array)
            each{|r| (h[r.values_at(*key_column)] ||= []) << r.values_at(*value_column)}
          else
            each{|r| (h[r[key_column]] ||= []) << r.values_at(*value_column)}
          end
        else
          if key_column.is_a?(Array)
            each{|r| (h[r.values_at(*key_column)] ||= []) << r[value_column]}
          else
            each{|r| (h[r[key_column]] ||= []) << r[value_column]}
          end
        end
      elsif key_column.is_a?(Array)
        each{|r| (h[r.values_at(*key_column)] ||= []) << r}
      else
        each{|r| (h[r[key_column]] ||= []) << r}
      end
      h
    end

    # Truncates the dataset.  Returns nil.
    #
    #   DB[:table].truncate # TRUNCATE table
    #   # => nil
    def truncate
      execute_ddl(truncate_sql)
    end

    # Updates values for the dataset.  The returned value is generally the
    # number of rows updated, but that is adapter dependent. +values+ should
    # a hash where the keys are columns to set and values are the values to
    # which to set the columns.
    #
    #   DB[:table].update(:x=>nil) # UPDATE table SET x = NULL
    #   # => 10
    #
    #   DB[:table].update(:x=>Sequel.expr(:x)+1, :y=>0) # UPDATE table SET x = (x + 1), y = 0
    #   # => 10
    def update(values=OPTS, &block)
      sql = update_sql(values)
      if uses_returning?(:update)
        returning_fetch_rows(sql, &block)
      else
        execute_dui(sql)
      end
    end

    # Run the given SQL and return an array of all rows.  If a block is given,
    # each row is yielded to the block after all rows are loaded. See with_sql_each.
    def with_sql_all(sql, &block)
      _all(block){|a| with_sql_each(sql){|r| a << r}}
    end

    # Execute the given SQL and return the number of rows deleted.  This exists
    # solely as an optimization, replacing with_sql(sql).delete.  It's significantly
    # faster as it does not require cloning the current dataset.
    def with_sql_delete(sql)
      execute_dui(sql)
    end
    alias with_sql_update with_sql_delete

    # Run the given SQL and yield each returned row to the block.
    #
    # This method should not be called on a shared dataset if the columns selected
    # in the given SQL do not match the columns in the receiver.
    def with_sql_each(sql)
      if row_proc = @row_proc
        fetch_rows(sql){|r| yield row_proc.call(r)}
      else
        fetch_rows(sql){|r| yield r}
      end
      self
    end
    
    # Run the given SQL and return the first row, or nil if no rows were returned.
    # See with_sql_each.
    def with_sql_first(sql)
      with_sql_each(sql){|r| return r}
      nil
    end

    # Run the given SQL and return the first value in the first row, or nil if no
    # rows were returned.  For this to make sense, the SQL given should select
    # only a single value.  See with_sql_each.
    def with_sql_single_value(sql)
      if r = with_sql_first(sql)
        r.values.first
      end
    end

    # Execute the given SQL and (on most databases) return the primary key of the
    # inserted row.
    def with_sql_insert(sql)
      execute_insert(sql)
    end

    protected

    # Internals of #import.  If primary key values are requested, use
    # separate insert commands for each row.  Otherwise, call #multi_insert_sql
    # and execute each statement it gives separately.
    def _import(columns, values, opts)
      trans_opts = opts.merge(:server=>@opts[:server])
      if opts[:return] == :primary_key
        @db.transaction(trans_opts){values.map{|v| insert(columns, v)}}
      else
        stmts = multi_insert_sql(columns, values)
        @db.transaction(trans_opts){stmts.each{|st| execute_dui(st)}}
      end
    end
  
    # Return an array of arrays of values given by the symbols in ret_cols.
    def _select_map_multiple(ret_cols)
      map{|r| r.values_at(*ret_cols)}
    end
  
    # Returns an array of the first value in each row.
    def _select_map_single
      map{|r| r.values.first}
    end
  
    private
    
    # Internals of all and with_sql_all
    def _all(block)
      a = []
      yield a
      post_load(a)
      a.each(&block) if block
      a
    end
    
    # Internals of +select_hash+ and +select_hash_groups+
    def _select_hash(meth, key_column, value_column)
      select(*(key_column.is_a?(Array) ? key_column : [key_column]) + (value_column.is_a?(Array) ? value_column : [value_column])).
        send(meth, hash_key_symbols(key_column), hash_key_symbols(value_column))
    end
    
    # Internals of +select_map+ and +select_order_map+
    def _select_map(column, order, &block)
      ds = ungraphed.naked
      columns = Array(column)
      virtual_row_columns(columns, block)
      select_cols = order ? columns.map{|c| c.is_a?(SQL::OrderedExpression) ? c.expression : c} : columns
      ds = ds.order(*columns.map{|c| unaliased_identifier(c)}) if order
      if column.is_a?(Array) || (columns.length > 1)
        ds.select(*select_cols)._select_map_multiple(hash_key_symbols(select_cols))
      else
        ds.select(auto_alias_expression(select_cols.first))._select_map_single
      end
    end

    # Automatically alias the given expression if it does not have an identifiable alias.
    def auto_alias_expression(v)
      case v
      when LiteralString, Symbol, SQL::Identifier, SQL::QualifiedIdentifier, SQL::AliasedExpression
        v
      else
        SQL::AliasedExpression.new(v, :v)
      end
    end

    # The default number of rows that can be inserted in a single INSERT statement via import.
    # The default is for no limit.
    def default_import_slice
      nil
    end

    # Set the server to use to :default unless it is already set in the passed opts
    def default_server_opts(opts)
      @db.sharded? ? {:server=>@opts[:server] || :default}.merge(opts) : opts
    end

    # Execute the given select SQL on the database using execute. Use the
    # :read_only server unless a specific server is set.
    def execute(sql, opts=OPTS, &block)
      db = @db
      db.execute(sql, db.sharded? ? {:server=>@opts[:server] || :read_only}.merge(opts) : opts, &block)
    end
    
    # Execute the given SQL on the database using execute_ddl.
    def execute_ddl(sql, opts=OPTS, &block)
      @db.execute_ddl(sql, default_server_opts(opts), &block)
      nil
    end
    
    # Execute the given SQL on the database using execute_dui.
    def execute_dui(sql, opts=OPTS, &block)
      @db.execute_dui(sql, default_server_opts(opts), &block)
    end
    
    # Execute the given SQL on the database using execute_insert.
    def execute_insert(sql, opts=OPTS, &block)
      @db.execute_insert(sql, default_server_opts(opts), &block)
    end
    
    # Return a plain symbol given a potentially qualified or aliased symbol,
    # specifying the symbol that is likely to be used as the hash key
    # for the column when records are returned.  Return nil if no hash key
    # can be determined
    def _hash_key_symbol(s, recursing=false)
      case s
      when Symbol
        _, c, a = split_symbol(s)
        (a || c).to_sym
      when SQL::Identifier, SQL::Wrapper
        _hash_key_symbol(s.value, true)
      when SQL::QualifiedIdentifier
        _hash_key_symbol(s.column, true)
      when SQL::AliasedExpression
        _hash_key_symbol(s.alias, true)
      when String
        s.to_sym if recursing
      end
    end

    # Return a plain symbol given a potentially qualified or aliased symbol,
    # specifying the symbol that is likely to be used as the hash key
    # for the column when records are returned.  Raise Error if the hash key
    # symbol cannot be returned.
    def hash_key_symbol(s)
      if v = _hash_key_symbol(s)
        v
      else
        raise(Error, "#{s.inspect} is not supported, should be a Symbol, SQL::Identifier, SQL::QualifiedIdentifier, or SQL::AliasedExpression")
      end
    end

    # If s is an array, return an array with the given hash key symbols.
    # Otherwise, return a hash key symbol for the given expression 
    # If a hash key symbol cannot be determined, raise an error.
    def hash_key_symbols(s)
      s.is_a?(Array) ? s.map{|c| hash_key_symbol(c)} : hash_key_symbol(s)
    end
    
    # Returns an expression that will ignore values preceding the given row, using the
    # receiver's current order. This yields the row and the array of order expressions
    # to the block, which should return an array of values to use.
    def ignore_values_preceding(row)
      @opts[:order].map{|v| v.is_a?(SQL::OrderedExpression) ? v.expression : v}

      order_exprs = @opts[:order].map do |v|
        if v.is_a?(SQL::OrderedExpression)
          descending = v.descending
          v = v.expression
        else
          descending = false
        end
        [v, descending]
      end

      row_values = yield(row, order_exprs.map{|e| e.first})

      last_expr = []
      cond = order_exprs.zip(row_values).map do |(v, descending), value|
        expr =  last_expr + [SQL::BooleanExpression.new(descending ? :< : :>, v, value)]
        last_expr += [SQL::BooleanExpression.new(:'=', v, value)]
        Sequel.&(*expr)
      end
      Sequel.|(*cond)
    end

    # Modify the identifier returned from the database based on the
    # identifier_output_method.
    def output_identifier(v)
      v = 'untitled' if v == ''
      (i = identifier_output_method) ? v.to_s.send(i).to_sym : v.to_sym
    end
    
    # This is run inside .all, after all of the records have been loaded
    # via .each, but before any block passed to all is called.  It is called with
    # a single argument, an array of all returned records.  Does nothing by
    # default, added to make the model eager loading code simpler.
    def post_load(all_records)
    end

    # Called by insert/update/delete when returning is used.
    # Yields each row as a plain hash to the block if one is given, or returns
    # an array of plain hashes for all rows if a block is not given
    def returning_fetch_rows(sql, &block)
      if block
        default_server.fetch_rows(sql, &block)
        nil
      else
        rows = []
        default_server.fetch_rows(sql){|r| rows << r}
        rows
      end
    end
    
    # Return the unaliased part of the identifier.  Handles both
    # implicit aliases in symbols, as well as SQL::AliasedExpression
    # objects.  Other objects are returned as is.
    def unaliased_identifier(c)
      case c
      when Symbol
        table, column, aliaz = split_symbol(c)
        if aliaz
          table ? SQL::QualifiedIdentifier.new(table, column) : Sequel.identifier(column)
        else
          c
        end
      when SQL::AliasedExpression
        c.expression
      when SQL::OrderedExpression
        case expr = c.expression
        when Symbol, SQL::AliasedExpression
          SQL::OrderedExpression.new(unaliased_identifier(expr), c.descending, :nulls=>c.nulls)
        else
          c
        end
      else
        c
      end
    end
  end
end
