# The filter_having extension allows Dataset#filter, #and, #or
# and #exclude to operate on the HAVING clause if the dataset
# already has a HAVING clause, which was the historical behavior
# before Sequel 4.  It is only recommended to use this for
# backwards compatibility.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:filter_having)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:filter_having)

#
module Sequel
  module FilterHaving
    # Operate on HAVING clause if HAVING clause already present.
    def and(*cond, &block)
      if @opts[:having]
        having(*cond, &block)
      else
        super
      end
    end

    # Operate on HAVING clause if HAVING clause already present.
    def exclude(*cond, &block)
      if @opts[:having]
        exclude_having(*cond, &block)
      else
        super
      end
    end

    # Operate on HAVING clause if HAVING clause already present.
    def filter(*cond, &block)
      if @opts[:having]
        having(*cond, &block)
      else
        super
      end
    end

    # Operate on HAVING clause if HAVING clause already present.
    def or(*cond, &block)
      if having = @opts[:having]
        cond = cond.first if cond.size == 1
        clone(:having => SQL::BooleanExpression.new(:OR, having, filter_expr(cond, &block)))
      else
        super
      end
    end
  end

  Dataset.register_extension(:filter_having, FilterHaving)
end
