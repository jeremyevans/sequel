module Sequel
  class Dataset
    COMMA_SEPARATOR = ', '.freeze
    COUNT_OF_ALL_AS_COUNT = :count['*'.lit].as(:count)

    # Returns the first record matching the conditions.
    def [](*conditions)
      first(*conditions)
    end

    # Update all records matching the conditions
    # with the values specified.
    def []=(conditions, values)
      filter(conditions).update(values)
    end

    # Returns the average value for the given column.
    def avg(column)
      get(:avg[column])
    end
    
    # Returns true if no records exists in the dataset
    def empty?
      get(1).nil?
    end
    
    # Returns the first record in the dataset. If a numeric argument is
    # given, it is interpreted as a limit, and then returns all 
    # matching records up to that limit.  If no argument is passed,
    # it returns the first matching record.  If any other type of
    # argument(s) is passed, it is given to filter and the
    # first matching record is returned. If a block is given, it is used
    # to filter the dataset before returning anything.
    #
    # Examples:
    # 
    #   ds.first => {:id=>7}
    #   ds.first(2) => [{:id=>6}, {:id=>4}]
    #   ds.order(:id).first(2) => [{:id=>1}, {:id=>2}]
    #   ds.first(:id=>2) => {:id=>2}
    #   ds.first("id = 3") => {:id=>3}
    #   ds.first("id = ?", 4) => {:id=>4}
    #   ds.first{:id > 2} => {:id=>5}
    #   ds.order(:id).first{:id > 2} => {:id=>3}
    #   ds.first{:id > 2} => {:id=>5}
    #   ds.first("id > ?", 4){:id < 6) => {:id=>5}
    #   ds.order(:id).first(2){:id < 2} => [{:id=>1}]
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
    def get(column)
      select(column).single_value
    end

    # Returns a dataset grouped by the given column with count by group.
    def group_and_count(*columns)
      group(*columns).select(*(columns + [COUNT_OF_ALL_AS_COUNT])).order(:count)
    end
    
    # Returns the interval between minimum and maximum values for the given 
    # column.
    def interval(column)
      get("(max(#{literal(column)}) - min(#{literal(column)}))".lit)
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
    def map(column_name = nil, &block)
      if column_name
        super() {|r| r[column_name]}
      else
        super(&block)
      end
    end

    # Returns the maximum value for the given column.
    def max(column)
      get(:max[column])
    end

    # Returns the minimum value for the given column.
    def min(column)
      get(:min[column])
    end

    # Inserts multiple records into the associated table. This method can be
    # to efficiently insert a large amounts of records into a table. Inserts
    # are automatically wrapped in a transaction.
    # 
    # This method should be called with a columns array and an array of value arrays:
    #
    #   dataset.multi_insert([:x, :y], [[1, 2], [3, 4]])
    #
    # This method can also be called with an array of hashes:
    # 
    #   dataset.multi_insert({:x => 1}, {:x => 2})
    #
    # Be aware that all hashes should have the same keys if you use this calling method,
    # otherwise some columns could be missed or set to null instead of to default
    # values.
    #
    # The method also accepts a :slice or :commit_every option that specifies
    # the number of records to insert per transaction. This is useful especially
    # when inserting a large number of records, e.g.:
    #
    #   # this will commit every 50 records
    #   dataset.multi_insert(lots_of_records, :slice => 50)
    def multi_insert(*args)
      if args.empty?
        return
      elsif args[0].is_a?(Array) && args[1].is_a?(Array)
        columns, values, opts = *args
      elsif args[0].is_a?(Array) && args[1].is_a?(Dataset)
        table = @opts[:from].first
        columns, dataset = *args
        sql = "INSERT INTO #{quote_identifier(table)} #{literal(columns)} VALUES #{literal(dataset)}"
        return @db.transaction{execute_dui(sql)}
      else
        # we assume that an array of hashes is given
        hashes, opts = *args
        return if hashes.empty?
        columns = hashes.first.keys
        # convert the hashes into arrays
        values = hashes.map {|h| columns.map {|c| h[c]}}
      end
      # make sure there's work to do
      return if columns.empty? || values.empty?
      
      slice_size = opts && (opts[:commit_every] || opts[:slice])
      
      if slice_size
        values.each_slice(slice_size) do |slice|
          statements = multi_insert_sql(columns, slice)
          @db.transaction{statements.each{|st| execute_dui(st)}}
        end
      else
        statements = multi_insert_sql(columns, values)
        @db.transaction{statements.each{|st| execute_dui(st)}}
      end
    end
    alias_method :import, :multi_insert
    
    # Pretty prints the records in the dataset as plain-text table.
    def print(*cols)
      Sequel::PrettyTable.print(naked.all, cols.empty? ? columns : cols)
    end
    
    # Returns a Range object made from the minimum and maximum values for the
    # given column.
    def range(column)
      if r = select(:min[column].as(:v1), :max[column].as(:v2)).first
        (r[:v1]..r[:v2])
      end
    end
    
    # Returns the first record in the dataset.
    def single_record(opts = nil)
      each((opts||{}).merge(:limit=>1)){|r| return r}
      nil
    end

    # Returns the first value of the first record in the dataset.
    # Returns nil if dataset is empty.
    def single_value(opts = nil)
      if r = naked.single_record(opts)
        r.values.first
      end
    end
    
    # Returns the sum for the given column.
    def sum(column)
      get(:sum[column])
    end

    # Returns true if the table exists.  Will raise an error
    # if the dataset has fixed SQL or selects from another dataset
    # or more than one table.
    def table_exists?
      if @opts[:sql]
        raise Sequel::Error, "this dataset has fixed SQL"
      end
      
      if @opts[:from].size != 1
        raise Sequel::Error, "this dataset selects from multiple sources"
      end
      
      t = @opts[:from].first
      if t.is_a?(Dataset)
        raise Sequel::Error, "this dataset selects from a sub query"
      end
      
      @db.table_exists?(t.to_sym)
    end

    # Returns a string in CSV format containing the dataset records. By 
    # default the CSV representation includes the column titles in the
    # first line. You can turn that off by passing false as the 
    # include_column_titles argument.
    #
    # This does not use a CSV library or handle quoting of values in
    # any way.  If any values in any of the rows could include commas or line
    # endings, you probably shouldn't use this.
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
  end
end
