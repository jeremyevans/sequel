# frozen-string-literal: true

module Sequel
  class Dataset
    module ColumnsLimit1
      COLUMNS_CLONE_OPTIONS = {:distinct => nil, :limit => 1, :offset=>nil, :where=>nil, :having=>nil, :order=>nil, :row_proc=>nil, :graph=>nil, :eager_graph=>nil}.freeze

      # Use a limit of 1 instead of a limit of 0 when
      # getting the columns.
      def columns!
        ds = clone(COLUMNS_CLONE_OPTIONS)
        ds.each{break}

        if cols = ds.cache[:_columns]
          self.columns = cols
        else
          []
        end
      end
    end
  end
end
