module Sequel
  class Dataset
    # Returns a paginated dataset. The resulting dataset also provides the
    # total number of pages (Dataset#page_count) and the current page number
    # (Dataset#current_page), as well as Dataset#prev_page and Dataset#next_page
    # for implementing pagination controls.
    def paginate(page_no, page_size)
      raise(Error, "You cannot paginate a dataset that already has a limit") if @opts[:limit]
      record_count = count
      total_pages = record_count.zero? ? 1 : (record_count / page_size.to_f).ceil
      raise(Error, "page_no must be between 1 and #{total_pages}") unless page_no.between?(1, total_pages)
      paginated = limit(page_size, (page_no - 1) * page_size)
      paginated.extend(Pagination)
      paginated.set_pagination_info(page_no, page_size, record_count)
      paginated
    end
      
    def each_page(page_size)
      raise(Error, "You cannot paginate a dataset that already has a limit") if @opts[:limit]
      record_count = count
      total_pages = (record_count / page_size.to_f).ceil
      
      (1..total_pages).each do |page_no|
        paginated = limit(page_size, (page_no - 1) * page_size)
        paginated.extend(Pagination)
        paginated.set_pagination_info(page_no, page_size, record_count)
        yield paginated
      end
      
      self
    end

    module Pagination
      attr_accessor :page_size, :page_count, :current_page, :pagination_record_count

      # Sets the pagination info
      def set_pagination_info(page_no, page_size, record_count)
        @current_page = page_no
        @page_size = page_size
        @pagination_record_count = record_count
        @page_count = (record_count / page_size.to_f).ceil
      end
      
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

      # Returns true if the current page is the last page
      def last_page?
        @current_page == @page_count
      end

      # Returns true if the current page is the first page
      def first_page?
        @current_page == 1
      end
    end
  end
end
