module Sequel
  class Dataset
    # ---------------------
    # :section: Methods that execute code on the database
    # These methods all execute the dataset's SQL on the database.
    # They don't return modified datasets, so if used in a method chain
    # they should be the last method called.
    # ---------------------
    
    # Alias for insert, but not aliased directly so subclasses
    # don't have to override both methods.
    def <<(*args)
      insert(*args)
    end
    
    # Returns the first record matching the conditions. Examples:
    #
    #   ds[:id=>1] => {:id=1}
    def [](*conditions)
      raise(Error, ARRAY_ACCESS_ERROR_MSG) if (conditions.length == 1 and conditions.first.is_a?(Integer)) or conditions.length == 0
      first(*conditions)
    end

    # Update all records matching the conditions
    # with the values specified. Examples:
    #
    #   ds[:id=>1] = {:id=>2} # SQL: UPDATE ... SET id = 2 WHERE id = 1
    def []=(conditions, values)
      filter(conditions).update(values)
    end

    # Returns an array with all records in the dataset. If a block is given,
    # the array is iterated over after all items have been loaded.
    def all(&block)
      a = []
      each{|r| a << r}
      post_load(a)
      a.each(&block) if block
      a
    end
    
    # Returns the average value for the given column.
    def avg(column)
      aggregate_dataset.get{avg(column)}
    end
  
    # Returns the columns in the result set in order.
    # If the columns are currently cached, returns the cached value. Otherwise,
    # a SELECT query is performed to get a single row. Adapters are expected
    # to fill the columns cache with the column information when a query is performed.
    # If the dataset does not have any rows, this may be an empty array depending on how
    # the adapter is programmed.
    #
    # If you are looking for all columns for a single table and maybe some information about
    # each column (e.g. type), see Database#schema.
    def columns
      return @columns if @columns
      ds = unfiltered.unordered.clone(:distinct => nil, :limit => 1)
      ds.each{break}
      @columns = ds.instance_variable_get(:@columns)
      @columns || []
    end
        
    # Remove the cached list of columns and do a SELECT query to find
    # the columns.
    def columns!
      @columns = nil
      columns
    end
    
    # Returns the number of records in the dataset.
    def count
      aggregate_dataset.get{COUNT(:*){}.as(count)}.to_i
    end
    
    # Deletes the records in the dataset.  The returned value is generally the
    # number of records deleted, but that is adapter dependent.  See delete_sql.
    def delete
      execute_dui(delete_sql)
    end
    
    # Iterates over the records in the dataset as they are yielded from the
    # database adapter, and returns self.
    #
    # Note that this method is not safe to use on many adapters if you are
    # running additional queries inside the provided block.  If you are
    # running queries inside the block, you use should all instead of each.
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
    def empty?
      get(1).nil?
    end

    # Executes a select query and fetches records, passing each record to the
    # supplied block.  The yielded records should be hashes with symbol keys.
    def fetch_rows(sql, &block)
      raise NotImplementedError, NOTIMPL_MSG
    end
    
    # If a integer argument is
    # given, it is interpreted as a limit, and then returns all 
    # matching records up to that limit.  If no argument is passed,
    # it returns the first matching record.  If any other type of
    # argument(s) is passed, it is given to filter and the
    # first matching record is returned. If a block is given, it is used
    # to filter the dataset before returning anything.  Examples:
    # 
    #   ds.first => {:id=>7}
    #   ds.first(2) => [{:id=>6}, {:id=>4}]
    #   ds.order(:id).first(2) => [{:id=>1}, {:id=>2}]
    #   ds.first(:id=>2) => {:id=>2}
    #   ds.first("id = 3") => {:id=>3}
    #   ds.first("id = ?", 4) => {:id=>4}
    #   ds.first{|o| o.id > 2} => {:id=>5}
    #   ds.order(:id).first{|o| o.id > 2} => {:id=>3}
    #   ds.first{|o| o.id > 2} => {:id=>5}
    #   ds.first("id > ?", 4){|o| o.id < 6} => {:id=>5}
    #   ds.order(:id).first(2){|o| o.id < 2} => [{:id=>1}]
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
    #   ds.get(:id)
    #   ds.get{|o| o.sum(:id)}
    def get(column=nil, &block)
      if column
        raise(Error, ARG_BLOCK_ERROR_MSG) if block
        select(column).single_value
      else
        select(&block).single_value
      end
    end
    
    # Inserts multiple records into the associated table. This method can be
    # to efficiently insert a large amounts of records into a table. Inserts
    # are automatically wrapped in a transaction.
    # 
    # This method is called with a columns array and an array of value arrays:
    #
    #   dataset.import([:x, :y], [[1, 2], [3, 4]])
    #
    # This method also accepts a dataset instead of an array of value arrays:
    #
    #   dataset.import([:x, :y], other_dataset.select(:a___x, :b___y))
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
    # See insert_sql.
    def insert(*values)
      execute_insert(insert_sql(*values))
    end
    
    # Inserts multiple values. If a block is given it is invoked for each
    # item in the given array before inserting it.  See #multi_insert as
    # a possible faster version that inserts multiple records in one
    # SQL statement.
    def insert_multiple(array, &block)
      if block
        array.each {|i| insert(block[i])}
      else
        array.each {|i| insert(i)}
      end
    end
    
    # Returns the interval between minimum and maximum values for the given 
    # column.
    def interval(column)
      aggregate_dataset.get{max(column) - min(column)}
    end

    # Reverses the order and then runs first.  Note that this
    # will not necessarily give you the last record in the dataset,
    # unless you have an unambiguous order.  If there is not
    # currently an order for this dataset, raises an Error.
    def last(*args, &block)
      raise(Error, 'No order specified') unless @opts[:order]
      reverse.first(*args, &block)
    end
    
    # Maps column values for each record in the dataset (if a column name is
    # given), or performs the stock mapping functionality of Enumerable. 
    # Raises an error if both an argument and block are given. Examples:
    #
    #   ds.map(:id) => [1, 2, 3, ...]
    #   ds.map{|r| r[:id] * 2} => [2, 4, 6, ...]
    def map(column=nil, &block)
      if column
        raise(Error, ARG_BLOCK_ERROR_MSG) if block
        super(){|r| r[column]}
      else
        super(&block)
      end
    end

    # Returns the maximum value for the given column.
    def max(column)
      aggregate_dataset.get{max(column)}
    end

    # Returns the minimum value for the given column.
    def min(column)
      aggregate_dataset.get{min(column)}
    end

    # This is a front end for import that allows you to submit an array of
    # hashes instead of arrays of columns and values:
    # 
    #   dataset.multi_insert([{:x => 1}, {:x => 2}])
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

    # Returns a Range object made from the minimum and maximum values for the
    # given column.
    def range(column)
      if r = aggregate_dataset.select{[min(column).as(v1), max(column).as(v2)]}.first
        (r[:v1]..r[:v2])
      end
    end
    
    # Returns a hash with key_column values as keys and value_column values as
    # values.  Similar to to_hash, but only selects the two columns.
    def select_hash(key_column, value_column)
      select(key_column, value_column).to_hash(hash_key_symbol(key_column), hash_key_symbol(value_column))
    end
    
    # Selects the column given (either as an argument or as a block), and
    # returns an array of all values of that column in the dataset.  If you
    # give a block argument that returns an array with multiple entries,
    # the contents of the resulting array are undefined.
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
    
    # Returns the first record in the dataset.
    def single_record
      clone(:limit=>1).each{|r| return r}
      nil
    end

    # Returns the first value of the first record in the dataset.
    # Returns nil if dataset is empty.
    def single_value
      if r = naked.ungraphed.single_record
        r.values.first
      end
    end
    
    # Returns the sum for the given column.
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
    def to_hash(key_column, value_column = nil)
      inject({}) do |m, r|
        m[r[key_column]] = value_column ? r[value_column] : r
        m
      end
    end

    # Truncates the dataset.  Returns nil.
    def truncate
      execute_ddl(truncate_sql)
    end

    # Updates values for the dataset.  The returned value is generally the
    # number of rows updated, but that is adapter dependent.  See update_sql.
    def update(values={})
      execute_dui(update_sql(values))
    end

    private
    
    # Set the server to use to :default unless it is already set in the passed opts
    def default_server_opts(opts)
      {:server=>@opts[:server] || :default}.merge(opts)
    end

    # Execute the given SQL on the database using execute.
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
        c_table ? column.to_sym.qualify(c_table) : column.to_sym
      when SQL::AliasedExpression
        c.expression
      else
        c
      end
    end
  end
end
