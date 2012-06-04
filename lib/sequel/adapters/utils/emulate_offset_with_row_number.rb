module Sequel
  module EmulateOffsetWithRowNumber
    # When a subselect that uses :offset is used in IN or NOT IN,
    # use a nested subselect that only includes the first column
    # instead of the ROW_NUMBER column added by the emulated offset support.
    def complex_expression_sql_append(sql, op, args)
      case op
      when :IN, :"NOT IN"
        ds = args.at(1)
        if ds.is_a?(Sequel::Dataset) && ds.opts[:offset]
          c = ds.opts[:select].first
          case c
          when Symbol
            t, cl, a = split_symbol(c)
            if a
              c = SQL::Identifier.new(a)
            elsif t
              c = SQL::Identifier.new(cl)
            end
          when SQL::AliasedExpression
            c = SQL::Identifier.new(c.aliaz)
          when SQL::QualifiedIdentifier
            c = SQL::Identifier.new(c.column)
          end
          super(sql, op, [args.at(0), ds.from_self.select(c)])
        else
          super
        end
      else
        super
      end
    end

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

      dsa1 = dataset_alias(1)
      rn = row_number_column
      sql = @opts[:append_sql] || ''
      subselect_sql_append(sql, unlimited.
        unordered.
        select_append{ROW_NUMBER(:over, :order=>order){}.as(rn)}.
        from_self(:alias=>dsa1).
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

    # This emulation adds an extra row number column that should be
    # eliminated.
    def offset_returns_row_number_column?
      true
    end
  end
end
