class Sequel::Dataset
  # This module should be included in the dataset class for all databases that
  # don't support INTERSECT or EXCEPT.
  module UnsupportedIntersectExcept
    # Raise an Error if EXCEPT is used
    def except(ds, all=false)
      raise(Sequel::Error, "EXCEPT not supported")
    end

    # Raise an Error if INTERSECT is used
    def intersect(ds, all=false)
      raise(Sequel::Error, "INTERSECT not supported")
    end
  end

  # This module should be included in the dataset class for all databases that
  # don't support INTERSECT ALL or EXCEPT ALL.
  module UnsupportedIntersectExceptAll
    # Raise an Error if EXCEPT is used
    def except(ds, all=false)
      raise(Sequel::Error, "EXCEPT ALL not supported") if all
      super(ds)
    end

    # Raise an Error if INTERSECT is used
    def intersect(ds, all=false)
      raise(Sequel::Error, "INTERSECT ALL not supported") if all
      super(ds)
    end
  end

  # This module should be included in the dataset class for all databases that
  # don't support IS [NOT] (TRUE|FALSE)
  module UnsupportedIsTrue
    # Use an = construct instead of IS and an != OR IS NULL construct instead of IS NOT.
    def complex_expression_sql(op, args)
      case op
      when :IS, :'IS NOT'
        isnot = op != :IS
        return super if (v1 = args.at(1)).nil?
        v0 = literal(args.at(0))
        s = "(#{v0} #{'!' if isnot}= #{literal(v1)})"
        s = "(#{s} OR (#{v0} IS NULL))" if isnot
        s
      else
        super(op, args)
      end
    end
  end
end
