# frozen-string-literal: true
#
# The from_block extension changes Database#from so that blocks given
# to it are treated as virtual rows applying to the FROM clause,
# instead of virtual rows applying to the WHERE clause.  This will
# probably be made the default in the next major version of Sequel.
#
# This makes it easier to use table returning functions:
#
#   DB.from{table_function(1)}
#   # SELECT * FROM table_function(1)
#
# To load the extension into the database:
#
#   DB.extension :from_block

#
module Sequel
  module Database::FromBlock
    # If a block is given, make it affect the FROM clause:
    #   DB.from{table_function(1)}
    #   # SELECT * FROM table_function(1)
    def from(*args, &block)
      if block
        @default_dataset.from(*args, &block)
      else
        super
      end
    end
  end

  Database.register_extension(:from_block, Database::FromBlock)
end

