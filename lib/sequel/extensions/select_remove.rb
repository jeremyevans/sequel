# The select_remove extension adds Sequel::Dataset#select_remove for removing existing selected
# columns from a dataset.  It's not part of Sequel core as it is rarely needed and has
# some corner cases where it can't work correctly.
#
# To load the extension:
#
#   Sequel.extension :select_remove
#

module Sequel
  class Dataset
    # Remove columns from the list of selected columns.  If any of the currently selected
    # columns use expressions/aliases, this will remove selected columns with the given
    # aliases.  It will also remove entries from the selection that match exactly:
    #
    #   # Assume columns a, b, and c in items table
    #   DB[:items] # SELECT * FROM items
    #   DB[:items].select_remove(:c) # SELECT a, b FROM items
    #   DB[:items].select(:a, :b___c, :c___b).select_remove(:c) # SELECT a, c AS b FROM items
    #   DB[:items].select(:a, :b___c, :c___b).select_remove(:c___b) # SELECT a, b AS c FROM items
    #
    # Note that there are a few cases where this method may not work correctly:
    #
    # * This dataset joins multiple tables and does not have an existing explicit selection.
    #   In this case, the code will currently use unqualified column names for all columns
    #   the dataset returns, except for the columns given.
    # * This dataset has an existing explicit selection containing an item that returns
    #   multiple database columns (e.g. :table.*, 'column1, column2'.lit).  In this case,
    #   the behavior is undefined and this method should not be used.
    #
    # There may be other cases where this method does not work correctly, use it with caution.
    def select_remove(*cols)
      if (sel = @opts[:select]) && !sel.empty?
        select(*(columns.zip(sel).reject{|c, s| cols.include?(c)}.map{|c, s| s} - cols))
      else
        select(*(columns - cols))
      end
    end
  end
end
