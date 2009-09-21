module Sequel
  class Dataset
    COMMA_SEPARATOR = ', '.freeze
    COUNT_OF_ALL_AS_COUNT = SQL::Function.new(:count, LiteralString.new('*'.freeze)).as(:count)
    ARRAY_ACCESS_ERROR_MSG = 'You cannot call Dataset#[] with an integer or with no arguments.'.freeze
    MAP_ERROR_MSG = 'Using Dataset#map with an argument and a block is not allowed'.freeze
    GET_ERROR_MSG = 'must provide argument or block to Dataset#get, not both'.freeze
    IMPORT_ERROR_MSG = 'Using Sequel::Dataset#import an empty column array is not allowed'.freeze

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

    # Returns the average value for the given column.
    def avg(column)
      aggregate_dataset.get{avg(column)}
    end
    
    # Returns true if no records exist in the dataset, false otherwise
    def empty?
      get(1).nil?
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
        raise(Error, GET_ERROR_MSG) if block
        select(column).single_value
      else
        select(&block).single_value
      end
    end

    # Returns a dataset grouped by the given column with count by group,
    # order by the count of records (in ascending order). Column aliases
    # may be supplied, and will be included in the select clause.
    #
    # Examples:
    #
    #   ds.group_and_count(:name).all => [{:name=>'a', :count=>1}, ...]
    #   ds.group_and_count(:first_name, :last_name).all => [{:first_name=>'a', :last_name=>'b', :count=>1}, ...]
    #   ds.group_and_count(:first_name___name).all => [{:name=>'a', :count=>1}, ...]
    def group_and_count(*columns)
      groups = columns.map do |c|
        c_table, column, _ = split_symbol(c)
        c_table ? column.to_sym.qualify(c_table) : column.to_sym
      end
      group(*groups).select(*(columns + [COUNT_OF_ALL_AS_COUNT])).order(:count)
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
        raise(Error, MAP_ERROR_MSG) if block
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
    
    # Returns the first record in the dataset.
    def single_record
      clone(:limit=>1).each{|r| return r}
      nil
    end

    # Returns the first value of the first record in the dataset.
    # Returns nil if dataset is empty.
    def single_value
      if r = naked.clone(:graph=>false).single_record
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
  end
end
