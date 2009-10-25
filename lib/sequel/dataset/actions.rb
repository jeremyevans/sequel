module Sequel
  class Dataset

    # Alias for insert, but not aliased directly so subclasses
    # don't have to override both methods.
    def <<(*args)
      insert(*args)
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
      else
        if row_proc = @row_proc
          fetch_rows(select_sql){|r| yield row_proc.call(r)}
        else
          fetch_rows(select_sql, &block)
        end
      end
      self
    end

    # Executes a select query and fetches records, passing each record to the
    # supplied block.  The yielded records should be hashes with symbol keys.
    def fetch_rows(sql, &block)
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    # Inserts values into the associated table.  The returned value is generally
    # the value of the primary key for the inserted row, but that is adapter dependent.
    # See insert_sql.
    def insert(*values)
      execute_insert(insert_sql(*values))
    end
  
    # Alias for set, but not aliased directly so subclasses
    # don't have to override both methods.
    def set(*args)
      update(*args)
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
  
  end
end
