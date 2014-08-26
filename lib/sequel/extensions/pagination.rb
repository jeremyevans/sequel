# The pagination extension adds the Sequel::Dataset#paginate and #each_page methods,
# which return paginated (limited and offset) datasets with some helpful methods
# that make creating a paginated display easier.
#
# This extension uses Object#extend at runtime, which can hurt performance.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:pagination)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:pagination)

#
module Sequel
  module DatasetPagination
    # Returns a paginated dataset. The returned dataset is limited to
    # the page size at the correct offset, and extended with the Pagination
    # module.  If a record count is not provided, does a count of total
    # number of records for this dataset.
    def paginate(page_no, page_size, record_count=nil)
      raise(Error, "You cannot paginate a dataset that already has a limit") if @opts[:limit]
      paginated = limit(page_size, (page_no - 1) * page_size)
      paginated.extend(Dataset::Pagination)
      paginated.set_pagination_info(page_no, page_size, record_count || count)
    end
      
    # Yields a paginated dataset for each page and returns the receiver. Does
    # a count to find the total number of records for this dataset. Returns
    # an enumerator if no block is given.
    def each_page(page_size)
      raise(Error, "You cannot paginate a dataset that already has a limit") if @opts[:limit]
      return to_enum(:each_page, page_size) unless block_given?
      record_count = count
      total_pages = (record_count / page_size.to_f).ceil
      (1..total_pages).each{|page_no| yield paginate(page_no, page_size, record_count)}
      self
    end
  end

  class Dataset
    # Holds methods that only relate to paginated datasets. Paginated dataset
    # have pages starting at 1 (page 1 is offset 0, page 1 is offset page_size).
    module Pagination
      # The number of records per page (the final page may have fewer than
      # this number of records).
      attr_accessor :page_size

      # The number of pages in the dataset before pagination, of which
      # this paginated dataset is one.  Empty datasets are considered
      # to have a single page.
      attr_accessor :page_count

      # The current page of the dataset, starting at 1 and not 0.
      attr_accessor :current_page

      # The total number of records in the dataset before pagination.
      attr_accessor :pagination_record_count

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

      # Returns true if the current page is the first page
      def first_page?
        @current_page == 1
      end

      # Returns true if the current page is the last page
      def last_page?
        @current_page == @page_count
      end

      # Returns the next page number or nil if the current page is the last page
      def next_page
        current_page < page_count ? (current_page + 1) : nil
      end
      
      # Returns the page range
      def page_range
        1..page_count
      end
      
      # Returns the previous page number or nil if the current page is the first
      def prev_page
        current_page > 1 ? (current_page - 1) : nil
      end

      # Sets the pagination info for this paginated dataset, and returns self.
      def set_pagination_info(page_no, page_size, record_count)
        @current_page = page_no
        @page_size = page_size
        @pagination_record_count = record_count
        @page_count = (record_count / page_size.to_f).ceil
        @page_count = 1 if @page_count == 0
        self
      end
    end
  end

  Dataset.register_extension(:pagination, DatasetPagination)
end
