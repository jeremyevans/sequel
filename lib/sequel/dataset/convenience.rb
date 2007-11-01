require 'enumerator'

module Sequel
  class Dataset
    module Convenience
      # Iterates through each record, converting it into a hash.
      def each_hash(&block)
        each {|a| block[a.to_hash]}
      end
      
      # Returns true if the record count is 0
      def empty?
        count == 0
      end
      
      # Returns the first record in the dataset.
      def single_record(opts = nil)
        each(opts) {|r| return r}
        nil
      end
      
      NAKED_HASH = {:naked => true}.freeze

      # Returns the first value of the first reecord in the dataset.
      def single_value(opts = nil)
        opts = opts ? NAKED_HASH.merge(opts) : NAKED_HASH
        # reset the columns cache so it won't fuck subsequent calls to columns
        each(opts) {|r| @columns = nil; return r.values.first}
      end

      # Returns the first record in the dataset. If the num argument is specified,
      # an array is returned with the first <i>num</i> records.
      def first(*args, &block)
        if block
          return filter(&block).single_record(:limit => 1)
        end
        args = args.empty? ? 1 : (args.size == 1) ? args.first : args
        case args
        when 1: single_record(:limit => 1)
        when Fixnum: limit(args).all
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
        raise SequelError, 'No order specified' unless 
          @opts[:order] || (opts && opts[:order])

        args = args.empty? ? 1 : (args.size == 1) ? args.first : args

        case args
        when Fixnum:
          l = {:limit => args}
          opts = {:order => invert_order(@opts[:order])}. \
            merge(opts ? opts.merge(l) : l)
          if args == 1
            single_record(opts)
          else
            clone_merge(opts).all
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
      
      # Returns a dataset grouped by the given column with count by group.
      def group_and_count(column)
        group(column).select(column, :count[column].AS(:count)).order(:count)
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
        records = naked.to_a
        csv = ''
        if include_column_titles
          csv << "#{@columns.join(COMMA_SEPARATOR)}\r\n"
        end
        records.each {|r| csv << "#{r.join(COMMA_SEPARATOR)}\r\n"}
        csv
      end

      # Inserts multiple records into the associated table. This method can be
      # to efficiently insert a large amounts of records into a table. Inserts
      # are automatically wrapped in a transaction. If the :commit_every 
      # option is specified, the method will generate a separate transaction 
      # for each batch of records, e.g.:
      #
      #   dataset.multi_insert(list, :commit_every => 1000)
      def multi_insert(list, opts = {})
        if every = opts[:commit_every]
          list.each_slice(every) do |s|
            @db.transaction do
              s.each {|r| @db.execute(insert_sql(r))}
              # @db.execute(s.map {|r| insert_sql(r)}.join)
            end
          end
        else
          @db.transaction do
            # @db.execute(list.map {|r| insert_sql(r)}.join)
            list.each {|r| @db.execute(insert_sql(r))}
          end
        end
      end
      
      module QueryBlockCopy #:nodoc:
        def each(*args); raise SequelError, "#each cannot be invoked inside a query block."; end
        def insert(*args); raise SequelError, "#insert cannot be invoked inside a query block."; end
        def update(*args); raise SequelError, "#update cannot be invoked inside a query block."; end
        def delete(*args); raise SequelError, "#delete cannot be invoked inside a query block."; end
        
        def clone_merge(opts)
          @opts.merge!(opts)
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
        copy = clone_merge({})
        copy.extend(QueryBlockCopy)
        copy.instance_eval(&block)
        clone_merge(copy.opts)
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
    end
  end
end