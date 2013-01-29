module Sequel
  module EmulateOffsetWithRowNumber
    # Emulate OFFSET support with the ROW_NUMBER window function
    # 
    # The implementation is ugly, cloning the current dataset and modifying
    # the clone to add a ROW_NUMBER window function (and some other things),
    # then using the modified clone in a subselect which is selected from.
    #
    # If offset is used, an order must be provided, because the use of ROW_NUMBER
    # requires an order.
    def select_sql
      return super unless o = @opts[:offset]

      order = @opts[:order] || default_offset_order
      if order.nil? || order.empty?
        raise(Error, "#{db.database_type} requires an order be provided if using an offset")
      end

      columns = clone(:append_sql=>'').columns
      dsa1 = dataset_alias(1)
      rn = row_number_column
      sql = @opts[:append_sql] || ''
      subselect_sql_append(sql, unlimited.
        unordered.
        select_append{ROW_NUMBER(:over, :order=>order){}.as(rn)}.
        from_self(:alias=>dsa1).
        select(*columns).
        limit(@opts[:limit]).
        where(SQL::Identifier.new(rn) > o).
        order(rn))
      sql
    end

    private

    # The default order to use for datasets with offsets, if no order is defined.
    # By default, orders by all of the columns in the dataset.
    def default_offset_order
      clone(:append_sql=>'').columns
    end
  end
end
