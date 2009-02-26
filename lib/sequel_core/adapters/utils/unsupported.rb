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
    
    private
    
    # Since EXCEPT and INTERSECT are not supported, and order shouldn't matter
    # when UNION is used, don't worry about parantheses.  This may potentially
    # give incorrect results if UNION ALL is used.
    def select_compounds_sql(sql, opts)
      return unless opts[:compounds]
      opts[:compounds].each do |type, dataset, all|
        sql << " #{type.to_s.upcase}#{' ALL' if all} #{subselect_sql(dataset)}"
      end
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
end
