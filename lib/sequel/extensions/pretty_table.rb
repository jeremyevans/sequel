# The pretty_table extension adds Sequel::Dataset#print and the
# Sequel::PrettyTable class for creating nice-looking plain-text
# tables.  Example:
#
#   +--+-------+
#   |id|name   |
#   |--+-------|
#   |1 |fasdfas|
#   |2 |test   |
#   +--+-------+
#
# To load the extension:
#
#   Sequel.extension :pretty_table

module Sequel
  extension :_pretty_table

  class Dataset
    # Pretty prints the records in the dataset as plain-text table.
    def print(*cols)
      ds = naked
      rows = ds.all
      Sequel::PrettyTable.print(rows, cols.empty? ? ds.columns : cols)
    end
  end

  Database.register_extension(:pretty_table){}
  Dataset.register_extension(:pretty_table){}
end
