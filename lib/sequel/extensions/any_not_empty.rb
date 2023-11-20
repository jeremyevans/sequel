# frozen-string-literal: true
#
# The any_not_empty extension changes the behavior of Dataset#any?
# if called without a block.  By default, this method uses the
# standard Enumerable behavior of enumerating results and seeing
# if any result is not false or nil.  With this extension, it
# just checks whether the dataset is empty.  This approach can
# be much faster if the dataset is currently large.
#
#   DB[:table].any?
#   # SELECT * FROM table
#
#   DB[:table].extension(:any_not_empty).any?
#   # SELECT 1 as one FROM table LIMIT 1
#   
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:any_not_empty)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:any_not_empty)
#
# Note that this can result in any? returning a different result if
# the dataset has a row_proc that can return false or nil.
#
# Related module: Sequel::AnyNotEmpty

#
module Sequel
  module AnyNotEmpty
    # If a block is not given, return whether the dataset is not empty.
    def any?(*a)
      if !a.empty? || defined?(yield)
        super
      else
        !empty?
      end
    end
  end

  Dataset.register_extension(:any_not_empty, AnyNotEmpty)
end
