module Sequel
  module EmulateOffsetWithRowNumber
    # When a subselect that uses :offset is used in IN or NOT IN,
    # use a nested subselect that only includes the first column
    # instead of the ROW_NUMBER column added by the emulated offset support.
    def complex_expression_sql(op, args)
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
          super(op, [args.at(0), ds.from_self.select(c)])
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
    # If offset is used, an order must be provided on some databases, because the use of ROW_NUMBER
    # requires an order.
    def select_sql
      return super unless o = @opts[:offset]
      rne = row_number_expression(@opts[:order])
      dsa1 = dataset_alias(1)
      rn = row_number_column
      ds = emulate_offset_remove_order ? unordered : self
      subselect_sql(ds.unlimited.
        select_append{rne.as(rn)}.
        from_self(:alias=>dsa1).
        limit(@opts[:limit]).
        where(SQL::Identifier.new(rn) > o))
    end

    private

    # Whether the emulated offset support should remove the ORDER clause in
    # the subselect.  True by default as the window function handles the
    # ordering.
    def emulate_offset_remove_order
      true
    end

    # Use the ROW_NUMBER window function with the given order as the row
    # number expression.
    def row_number_expression(order)
      raise(Error, "#{db.database_type} requires an order be provided if using an offset") unless order
      SQL::WindowFunction.new(SQL::Function.new(:ROW_NUMBER), SQL::Window.new(:order=>order))
    end
  end
end
