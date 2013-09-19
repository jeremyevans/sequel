module Sequel
  module EmulateOffsetWithReverseAndCount
    # Make empty? work with an offset with an order.
    # By default it would break since the order would be based on
    # a column that empty does not select.
    def empty?
      if o = @opts[:offset]
        unlimited.count <= o
      else
        super
      end
    end

    # Emulate OFFSET support using reverse order in a subselect, requiring
    # a count of the number of rows.
    #
    # If offset is used, an order must be provided, since it needs to be
    # reversed in the subselect.  Note that the order needs to be unambiguous
    # to work correctly, and you must select all columns that you are ordering on.
    def select_sql
      return super unless o = @opts[:offset]

      order = @opts[:order] || default_offset_order
      if order.nil? || order.empty?
        raise(Error, "#{db.database_type} requires an order be provided if using an offset")
      end

      ds = unlimited
      row_count = @opts[:offset_total_count] || ds.clone(:append_sql=>'').count
      dsa1 = dataset_alias(1)

      if o.is_a?(Symbol) && @opts[:bind_vars] && (match = Sequel::Dataset::PreparedStatementMethods::PLACEHOLDER_RE.match(o.to_s))
        # Handle use of bound variable offsets.  Unfortunately, prepared statement
        # bound variable offsets cannot be handled, since the bound variable value
        # isn't available until later.
        s = match[1].to_sym
        if prepared_arg?(s)
          o = prepared_arg(s)
        end
      end

      reverse_offset = row_count - o
      ds = if reverse_offset > 0
        ds.limit(reverse_offset).
          reverse_order(*order).
          from_self(:alias=>dsa1).
          limit(@opts[:limit]).
          order(*order)
      else
        # Sequel doesn't allow a nonpositive limit.  If the offset
        # is greater than the number of rows, the empty result set
        # shuld be returned, so use a condition that is always false.
        ds.where(1=>0)
      end
      sql = @opts[:append_sql] || ''
      subselect_sql_append(sql, ds)
      sql
    end

    private

    # The default order to use for datasets with offsets, if no order is defined.
    # By default, orders by all of the columns in the dataset.
    def default_offset_order
      clone(:append_sql=>'', :offset=>nil).columns
    end
  end
end
