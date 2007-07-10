module Sequel
  class Dataset
    module Convenience
      # Returns the first record in the dataset.
      def single_record(opts = nil)
        each(opts) {|r| return r}
        nil
      end
      
      NAKED_HASH = {:naked => true}.freeze

      # Returns the first value of the first reecord in the dataset.
      def single_value(opts = nil)
        opts = opts ? NAKED_HASH.merge(opts) : NAKED_HASH
        each(opts) {|r| return r.values.first}
      end

      # Returns the first record in the dataset. If the num argument is specified,
      # an array is returned with the first <i>num</i> records.
      def first(*args)
        args = args.empty? ? 1 : (args.size == 1) ? args.first : args
        case args
        when 1: single_record(:limit => 1)
        when Fixnum: limit(args).all
        else
          filter(args).single_record(:limit => 1)
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
      
      # Maps field values for each record in the dataset (if a field name is
      # given), or performs the stock mapping functionality of Enumerable.
      def map(field_name = nil, &block)
        if field_name
          super() {|r| r[field_name]}
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

      # returns a paginated dataset. The resulting dataset also provides the
      # total number of pages (Dataset#page_count) and the current page number
      # (Dataset#current_page), as well as Dataset#prev_page and Dataset#next_page
      # for implementing pagination controls.
      def paginate(page_no, page_size)
        total_pages = (count / page_size.to_f).ceil
        paginated = limit(page_size, (page_no - 1) * page_size)
        paginated.current_page = page_no
        paginated.page_count = total_pages
        paginated
      end

      attr_accessor :page_count, :current_page

      # Returns the previous page number or nil if the current page is the first
      def prev_page
        current_page > 1 ? (current_page - 1) : nil
      end

      # Returns the next page number or nil if the current page is the last page
      def next_page
        current_page < page_count ? (current_page + 1) : nil
      end

      # Returns the minimum value for the given field.
      def min(field)
        single_value(:select => [field.MIN])
      end

      # Returns the maximum value for the given field.
      def max(field)
        single_value(:select => [field.MAX])
      end

      # Returns the sum for the given field.
      def sum(field)
        single_value(:select => [field.SUM])
      end

      # Returns the average value for the given field.
      def avg(field)
        single_value(:select => [field.AVG])
      end

      # Pretty prints the records in the dataset as plain-text table.
      def print(*cols)
        Sequel::PrettyTable.print(naked.all, cols.empty? ? columns : cols)
      end
    end
  end
end