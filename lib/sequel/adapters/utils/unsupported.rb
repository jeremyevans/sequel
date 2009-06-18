class Sequel::Dataset
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
