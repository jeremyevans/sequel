require 'enumerator'

module Sequel
  class Dataset
    module Convenience
      # Returns true if no records exists in the dataset
      def empty?
        db.dataset.where(exists).get(1) == nil
        # count == 0
      end
      
      # Returns the first record in the dataset.
      def single_record(opts = nil)
        each(opts) {|r| return r}
        nil
      end
      
      NAKED_HASH = {:naked => true}.freeze

      # Returns the first value of the first reecord in the dataset.
      # Returns nill if dataset is empty.
      def single_value(opts = nil)
        opts = opts ? NAKED_HASH.merge(opts) : NAKED_HASH
        # reset the columns cache so it won't fuck subsequent calls to columns
        each(opts) {|r| @columns = nil; return r.values.first}
        nil
      end
      
      def get(column)
        select(column).single_value
      end

      # Returns the first record in the dataset. If the num argument is specified,
      # an array is returned with the first <i>num</i> records.
      def first(*args, &block)
        if block
          return filter(&block).single_record(:limit => 1)
        end
        args = args.empty? ? 1 : (args.size == 1) ? args.first : args
        case args
        when 1
          single_record(:limit => 1)
        when Fixnum
          limit(args).all
        else
          filter(args, &block).single_record(:limit => 1)
        end
      end

      # Returns the first record matching the condition.
      def [](*conditions)
        first(*conditions)
      end

      def []=(conditions, values)
        filter(conditions).update(values)
      end

      # Returns the last records in the dataset by inverting the order. If no
      # order is given, an exception is raised. If num is not given, the last
      # record is returned. Otherwise an array is returned with the last 
      # <i>num</i> records.
      def last(*args)
        raise Error, 'No order specified' unless 
          @opts[:order] || (opts && opts[:order])

        args = args.empty? ? 1 : (args.size == 1) ? args.first : args

        case args
        when Fixnum
          l = {:limit => args}
          opts = {:order => invert_order(@opts[:order])}. \
            merge(opts ? opts.merge(l) : l)
          if args == 1
            single_record(opts)
          else
            clone(opts).all
          end
        else
          filter(args).last(1)
        end
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

      # Returns a hash with one column used as key and another used as value.
      def to_hash(key_column, value_column)
        inject({}) do |m, r|
          m[r[key_column]] = r[value_column]
          m
        end
      end

      # Returns a paginated dataset. The resulting dataset also provides the
      # total number of pages (Dataset#page_count) and the current page number
      # (Dataset#current_page), as well as Dataset#prev_page and Dataset#next_page
      # for implementing pagination controls.
      def paginate(page_no, page_size)
        record_count = count
        total_pages = (record_count / page_size.to_f).ceil
        paginated = limit(page_size, (page_no - 1) * page_size)
        paginated.set_pagination_info(page_no, page_size, record_count)
        paginated
      end
      
      # Sets the pagination info
      def set_pagination_info(page_no, page_size, record_count)
        @current_page = page_no
        @page_size = page_size
        @pagination_record_count = record_count
        @page_count = (record_count / page_size.to_f).ceil
      end
      
      def each_page(page_size)
        record_count = count
        total_pages = (record_count / page_size.to_f).ceil
        
        (1..total_pages).each do |page_no|
          paginated = limit(page_size, (page_no - 1) * page_size)
          paginated.set_pagination_info(page_no, page_size, record_count)
          yield paginated
        end
        
        self
      end

      attr_accessor :page_size, :page_count, :current_page, :pagination_record_count

      # Returns the previous page number or nil if the current page is the first
      def prev_page
        current_page > 1 ? (current_page - 1) : nil
      end

      # Returns the next page number or nil if the current page is the last page
      def next_page
        current_page < page_count ? (current_page + 1) : nil
      end
      
      # Returns the page range
      def page_range
        1..page_count
      end
      
      # Returns the record range for the current page
      def current_page_record_range
        return (0..0) if @current_page > @page_count
        
        a = 1 + (@current_page - 1) * @page_size
        b = a + @page_size - 1
        b = @pagination_record_count if b > @pagination_record_count
        a..b
      end

      # Returns the number of records in the current page
      def current_page_record_count
        return 0 if @current_page > @page_count
        
        a = 1 + (@current_page - 1) * @page_size
        b = a + @page_size - 1
        b = @pagination_record_count if b > @pagination_record_count
        b - a + 1
      end

      # Returns the minimum value for the given column.
      def min(column)
        single_value(:select => [column.MIN.AS(:v)])
      end

      # Returns the maximum value for the given column.
      def max(column)
        single_value(:select => [column.MAX.AS(:v)])
      end

      # Returns the sum for the given column.
      def sum(column)
        single_value(:select => [column.SUM.AS(:v)])
      end

      # Returns the average value for the given column.
      def avg(column)
        single_value(:select => [column.AVG.AS(:v)])
      end
      
      COUNT_OF_ALL_AS_COUNT = :count['*'.lit].AS(:count)
      
      # Returns a dataset grouped by the given column with count by group.
      def group_and_count(*columns)
        group(*columns).select(columns + [COUNT_OF_ALL_AS_COUNT]).order(:count)
      end
      
      # Returns a Range object made from the minimum and maximum values for the
      # given column.
      def range(column)
        r = select(column.MIN.AS(:v1), column.MAX.AS(:v2)).first
        r && (r[:v1]..r[:v2])
      end
      
      # Returns the interval between minimum and maximum values for the given 
      # column.
      def interval(column)
        r = select("(max(#{literal(column)}) - min(#{literal(column)})) AS v".lit).first
        r && r[:v]
      end

      # Pretty prints the records in the dataset as plain-text table.
      def print(*cols)
        Sequel::PrettyTable.print(naked.all, cols.empty? ? columns : cols)
      end
      
      COMMA_SEPARATOR = ', '.freeze
      
      # Returns a string in CSV format containing the dataset records. By 
      # default the CSV representation includes the column titles in the
      # first line. You can turn that off by passing false as the 
      # include_column_titles argument.
      def to_csv(include_column_titles = true)
        n = naked
        cols = n.columns
        csv = ''
        csv << "#{cols.join(COMMA_SEPARATOR)}\r\n" if include_column_titles
        n.each{|r| csv << "#{cols.collect{|c| r[c]}.join(COMMA_SEPARATOR)}\r\n"}
        csv
      end
      
      # Inserts multiple records into the associated table. This method can be
      # to efficiently insert a large amounts of records into a table. Inserts
      # are automatically wrapped in a transaction.
      # 
      # This method can be called either with an array of hashes:
      # 
      #   dataset.multi_insert({:x => 1}, {:x => 2})
      #
      # Or with a columns array and an array of value arrays:
      #
      #   dataset.multi_insert([:x, :y], [[1, 2], [3, 4]])
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
          sql = "INSERT INTO #{table} (#{literal(columns)}) VALUES (#{dataset.sql})"
          return @db.transaction {@db.execute sql}
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
            @db.transaction {statements.each {|st| @db.execute(st)}}
          end
        else
          statements = multi_insert_sql(columns, values)
          @db.transaction {statements.each {|st| @db.execute(st)}}
        end
      end
      alias_method :import, :multi_insert
      
      module QueryBlockCopy #:nodoc:
        def each(*args); raise Error, "#each cannot be invoked inside a query block."; end
        def insert(*args); raise Error, "#insert cannot be invoked inside a query block."; end
        def update(*args); raise Error, "#update cannot be invoked inside a query block."; end
        def delete(*args); raise Error, "#delete cannot be invoked inside a query block."; end

        def clone(opts = nil)
          @opts.merge!(opts)
          self
        end
      end
      
      # Translates a query block into a dataset. Query blocks can be useful
      # when expressing complex SELECT statements, e.g.:
      #
      #   dataset = DB[:items].query do
      #     select :x, :y, :z
      #     where {:x > 1 && :y > 2}
      #     order_by :z.DESC
      #   end
      #
      def query(&block)
        copy = clone({})
        copy.extend(QueryBlockCopy)
        copy.instance_eval(&block)
        clone(copy.opts)
      end
      
      MUTATION_RE = /^(.+)!$/.freeze

      # Provides support for mutation methods (filter!, order!, etc.) and magic
      # methods.
      def method_missing(m, *args, &block)
        if m.to_s =~ MUTATION_RE
          m = $1.to_sym
          super unless respond_to?(m)
          copy = send(m, *args, &block)
          super if copy.class != self.class
          @opts.merge!(copy.opts)
          self
        elsif magic_method_missing(m)
          send(m, *args)
        else
           super
        end
      end
      
      MAGIC_METHODS = {
        /^order_by_(.+)$/   => proc {|c| proc {order(c)}},
        /^first_by_(.+)$/   => proc {|c| proc {order(c).first}},
        /^last_by_(.+)$/    => proc {|c| proc {order(c).last}},
        /^filter_by_(.+)$/  => proc {|c| proc {|v| filter(c => v)}},
        /^all_by_(.+)$/     => proc {|c| proc {|v| filter(c => v).all}},
        /^find_by_(.+)$/    => proc {|c| proc {|v| filter(c => v).first}},
        /^group_by_(.+)$/   => proc {|c| proc {group(c)}},
        /^count_by_(.+)$/   => proc {|c| proc {group_and_count(c)}}
      }

      # Checks if the given method name represents a magic method and 
      # defines it. Otherwise, nil is returned.
      def magic_method_missing(m)
        method_name = m.to_s
        MAGIC_METHODS.each_pair do |r, p|
          if method_name =~ r
            impl = p[$1.to_sym]
            return Dataset.class_def(m, &impl)
          end
        end
        nil
      end
      
      def create_view(name)
        @db.create_view(name, self)
      end
      
      def create_or_replace_view(name)
        @db.create_or_replace_view(name, self)
      end
      
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
    end
  end
end
