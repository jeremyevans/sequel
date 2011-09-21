module Sequel
  class Dataset
    # ---------------------
    # :section: 2 - Methods that execute code on the database
    # These methods all execute the dataset's SQL on the database.
    # They don't return modified datasets, so if used in a method chain
    # they should be the last method called.
    # ---------------------
    
    # Action methods defined by Sequel that execute code on the database.
    ACTION_METHODS = %w'<< [] []= all avg count columns columns! delete each
    empty? fetch_rows first get import insert insert_multiple interval last
    map max min multi_insert range select_hash select_map select_order_map
    set single_record single_value sum to_csv to_hash truncate update'.map{|x| x.to_sym}

    # Alias for insert, but not aliased directly so subclasses
    # don't have to override both methods.
    def <<(*args)
      insert(*args)
    end
    
    # Returns the first record matching the conditions. Examples:
    #
    #   DB[:table][:id=>1] # SELECT * FROM table WHERE (id = 1) LIMIT 1
    #   # => {:id=1}
    def [](*conditions)
      raise(Error, ARRAY_ACCESS_ERROR_MSG) if (conditions.length == 1 and conditions.first.is_a?(Integer)) or conditions.length == 0
      first(*conditions)
    end

    # Update all records matching the conditions with the values specified.
    # Returns the number of rows affected.
    #
    #   DB[:table][:id=>1] = {:id=>2} # UPDATE table SET id = 2 WHERE id = 1
    #   # => 1 # number of rows affected
    def []=(conditions, values)
      filter(conditions).update(values)
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
      a = []
      each{|r| a << r}
      post_load(a)
      a.each(&block) if block
      a
    end
    
    # Returns the average value for the given column.
    #
    #   DB[:table].avg(:number) # SELECT avg(number) FROM table LIMIT 1
    #   # => 3
    def avg(column)
      aggregate_dataset.get{avg(column)}
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
      ds = unfiltered.unordered.clone(:distinct => nil, :limit => 1)
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
    
    # Returns the number of records in the dataset.
    #
    #   DB[:table].count # SELECT COUNT(*) AS count FROM table LIMIT 1
    #   # => 3
    def count
      aggregate_dataset.get{COUNT(:*){}.as(count)}.to_i
    end
    
    # Deletes the records in the dataset.  The returned value should be 
    # number of records deleted, but that is adapter dependent.
    #
    #   DB[:table].delete # DELETE * FROM table
    #   # => 3
    def delete(&block)
      sql = delete_sql
      if uses_returning?(:delete)
        default_server.fetch_rows(sql, &block)
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
    # for the outer queries, or use a separate thread or shard inside +each+:
    def each(&block)
      if @opts[:graph]
        graph_each(&block)
      elsif row_proc = @row_proc
        fetch_rows(select_sql){|r| yield row_proc.call(r)}
      else
        fetch_rows(select_sql, &block)
      end
      self
    end
    
    # Returns true if no records exist in the dataset, false otherwise
    #
    #   DB[:table].empty? # SELECT 1 FROM table LIMIT 1
    #   # => false
    def empty?
      get(1).nil?
    end

    # Executes a select query and fetches records, yielding each record to the
    # supplied block.  The yielded records should be hashes with symbol keys.
    # This method should probably should not be called by user code, use +each+
    # instead.
    def fetch_rows(sql)
      raise NotImplemented, NOTIMPL_MSG
    end
    
    # If a integer argument is given, it is interpreted as a limit, and then returns all 
    # matching records up to that limit.  If no argument is passed,
    # it returns the first matching record.  If any other type of
    # argument(s) is passed, it is given to filter and the
    # first matching record is returned. If a block is given, it is used
    # to filter the dataset before returning anything.  Examples:
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
        if Integer === args
          ds.limit(args).all
        else
          ds.filter(args).single_record
        end
      end
    end

    # Return the column value for the first matching record in the dataset.
    # Raises an error if both an argument and block is given.
    #
    #   DB[:table].get(:id) # SELECT id FROM table LIMIT 1
    #   # => 3
    #
    #   ds.get{sum(id)} # SELECT sum(id) FROM table LIMIT 1
    #   # => 6
    def get(column=nil, &block)
      if column
        raise(Error, ARG_BLOCK_ERROR_MSG) if block
        select(column).single_value
      else
        select(&block).single_value
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
    # The method also accepts a :slice or :commit_every option that specifies
    # the number of records to insert per transaction. This is useful especially
    # when inserting a large number of records, e.g.:
    #
    #   # this will commit every 50 records
    #   dataset.import([:x, :y], [[1, 2], [3, 4], ...], :slice => 50)
    def import(columns, values, opts={})
      return @db.transaction{insert(columns, values)} if values.is_a?(Dataset)

      return if values.empty?
      raise(Error, IMPORT_ERROR_MSG) if columns.empty?
      
      if slice_size = opts[:commit_every] || opts[:slice]
        offset = 0
        loop do
          @db.transaction(opts){multi_insert_sql(columns, values[offset, slice_size]).each{|st| execute_dui(st)}}
          offset += slice_size
          break if offset >= values.length
        end
      else
        statements = multi_insert_sql(columns, values)
        @db.transaction{statements.each{|st| execute_dui(st)}}
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
        default_server.fetch_rows(sql, &block)
      else
        execute_insert(sql)
      end
    end
    
    # Inserts multiple values. If a block is given it is invoked for each
    # item in the given array before inserting it.  See +multi_insert+ as
    # a possible faster version that inserts multiple records in one
    # SQL statement.
    #
    #   DB[:table].insert_multiple([{:x=>1}, {:x=>2}])
    #   # INSERT INTO table (x) VALUES (1)
    #   # INSERT INTO table (x) VALUES (2)
    #
    #   DB[:table].insert_multiple([{:x=>1}, {:x=>2}]){|row| row[:y] = row[:x] * 2}
    #   # INSERT INTO table (x, y) VALUES (1, 2)
    #   # INSERT INTO table (x, y) VALUES (2, 4)
    def insert_multiple(array, &block)
      if block
        array.each {|i| insert(block[i])}
      else
        array.each {|i| insert(i)}
      end
    end
    
    # Returns the interval between minimum and maximum values for the given 
    # column.
    #
    #   DB[:table].interval(:id) # SELECT (max(id) - min(id)) FROM table LIMIT 1
    #   # => 6
    def interval(column)
      aggregate_dataset.get{max(column) - min(column)}
    end

    # Reverses the order and then runs #first with the given arguments and block.  Note that this
    # will not necessarily give you the last record in the dataset,
    # unless you have an unambiguous order.  If there is not
    # currently an order for this dataset, raises an +Error+.
    #
    #   DB[:table].order(:id).last # SELECT * FROM table ORDER BY id DESC LIMIT 1
    #   # => {:id=>10}
    #
    #   DB[:table].order(:id.desc).last(2) # SELECT * FROM table ORDER BY id ASC LIMIT 2
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
    def map(column=nil, &block)
      if column
        raise(Error, ARG_BLOCK_ERROR_MSG) if block
        super(){|r| r[column]}
      else
        super(&block)
      end
    end

    # Returns the maximum value for the given column.
    #
    #   DB[:table].max(:id) # SELECT max(id) FROM table LIMIT 1
    #   # => 10
    def max(column)
      aggregate_dataset.get{max(column)}
    end

    # Returns the minimum value for the given column.
    #
    #   DB[:table].min(:id) # SELECT min(id) FROM table LIMIT 1
    #   # => 1
    def min(column)
      aggregate_dataset.get{min(column)}
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
    # You can also use the :slice or :commit_every option that import accepts.
    def multi_insert(hashes, opts={})
      return if hashes.empty?
      columns = hashes.first.keys
      import(columns, hashes.map{|h| columns.map{|c| h[c]}}, opts)
    end

    # Returns a +Range+ instance made from the minimum and maximum values for the
    # given column.
    #
    #   DB[:table].range(:id) # SELECT max(id) AS v1, min(id) AS v2 FROM table LIMIT 1
    #   # => 1..10
    def range(column)
      if r = aggregate_dataset.select{[min(column).as(v1), max(column).as(v2)]}.first
        (r[:v1]..r[:v2])
      end
    end
    
    # Returns a hash with key_column values as keys and value_column values as
    # values.  Similar to to_hash, but only selects the two columns.
    #
    #   DB[:table].select_hash(:id, :name) # SELECT id, name FROM table
    #   # => {1=>'a', 2=>'b', ...}
    def select_hash(key_column, value_column)
      select(key_column, value_column).to_hash(hash_key_symbol(key_column), hash_key_symbol(value_column))
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
    def select_map(column=nil, &block)
      ds = naked.ungraphed
      ds = if column
        raise(Error, ARG_BLOCK_ERROR_MSG) if block
        ds.select(column)
      else
        ds.select(&block)
      end
      ds.map{|r| r.values.first}
    end
    
    # The same as select_map, but in addition orders the array by the column.
    #
    #   DB[:table].select_order_map(:id) # SELECT id FROM table ORDER BY id
    #   # => [1, 2, 3, 4, ...]
    #
    #   DB[:table].select_order_map{abs(id)} # SELECT (id * 2) FROM table ORDER BY (id * 2)
    #   # => [2, 4, 6, 8, ...]
    def select_order_map(column=nil, &block)
      ds = naked.ungraphed
      ds = if column
        raise(Error, ARG_BLOCK_ERROR_MSG) if block
        ds.select(column).order(unaliased_identifier(column))
      else
        ds.select(&block).order(&block)
      end
      ds.map{|r| r.values.first}
    end
  
    # Alias for update, but not aliased directly so subclasses
    # don't have to override both methods.
    def set(*args)
      update(*args)
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
      if r = naked.ungraphed.single_record
        r.values.first
      end
    end
    
    # Returns the sum for the given column.
    #
    #   DB[:table].sum(:id) # SELECT sum(id) FROM table LIMIT 1
    #   # => 55
    def sum(column)
      aggregate_dataset.get{sum(column)}
    end

    # Returns a string in CSV format containing the dataset records. By 
    # default the CSV representation includes the column titles in the
    # first line. You can turn that off by passing false as the 
    # include_column_titles argument.
    #
    # This does not use a CSV library or handle quoting of values in
    # any way.  If any values in any of the rows could include commas or line
    # endings, you shouldn't use this.
    #
    #   puts DB[:table].to_csv # SELECT * FROM table
    #   # id,name
    #   # 1,Jim
    #   # 2,Bob
    def to_csv(include_column_titles = true)
      n = naked
      cols = n.columns
      csv = ''
      csv << "#{cols.join(COMMA_SEPARATOR)}\r\n" if include_column_titles
      n.each{|r| csv << "#{cols.collect{|c| r[c]}.join(COMMA_SEPARATOR)}\r\n"}
      csv
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
    def to_hash(key_column, value_column = nil)
      inject({}) do |m, r|
        m[r[key_column]] = value_column ? r[value_column] : r
        m
      end
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
    #   DB[:table].update(:x=>:x+1, :y=>0) # UPDATE table SET x = (x + 1), y = 0
    #   # => 10
    def update(values={}, &block)
      sql = update_sql(values)
      if uses_returning?(:update)
        default_server.fetch_rows(sql, &block)
      else
        execute_dui(sql)
      end
    end

    private
    
    # Set the server to use to :default unless it is already set in the passed opts
    def default_server_opts(opts)
      {:server=>@opts[:server] || :default}.merge(opts)
    end

    # Execute the given select SQL on the database using execute. Use the
    # :read_only server unless a specific server is set.
    def execute(sql, opts={}, &block)
      @db.execute(sql, {:server=>@opts[:server] || :read_only}.merge(opts), &block)
    end
    
    # Execute the given SQL on the database using execute_ddl.
    def execute_ddl(sql, opts={}, &block)
      @db.execute_ddl(sql, default_server_opts(opts), &block)
      nil
    end
    
    # Execute the given SQL on the database using execute_dui.
    def execute_dui(sql, opts={}, &block)
      @db.execute_dui(sql, default_server_opts(opts), &block)
    end
    
    # Execute the given SQL on the database using execute_insert.
    def execute_insert(sql, opts={}, &block)
      @db.execute_insert(sql, default_server_opts(opts), &block)
    end
    
    # Return a plain symbol given a potentially qualified or aliased symbol,
    # specifying the symbol that is likely to be used as the hash key
    # for the column when records are returned.
    def hash_key_symbol(s)
      raise(Error, "#{s.inspect} is not a symbol") unless s.is_a?(Symbol)
      _, c, a = split_symbol(s)
      (a || c).to_sym
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

    # Return the unaliased part of the identifier.  Handles both
    # implicit aliases in symbols, as well as SQL::AliasedExpression
    # objects.  Other objects are returned as is.
    def unaliased_identifier(c)
      case c
      when Symbol
        c_table, column, _ = split_symbol(c)
        c_table ? SQL::QualifiedIdentifier.new(c_table, column.to_sym) : column.to_sym
      when SQL::AliasedExpression
        c.expression
      else
        c
      end
    end
  end
end
