# The columns_introspection extension attempts to introspect the
# selected columns for a dataset before issuing a query.  If it
# thinks it can guess correctly at the columns the query will use,
# it will return the columns without issuing a database query.
# This method is not fool-proof, it's possible that some databases
# will use column names that Sequel does not expect.
#
# To enable this for a single dataset, extend the dataset with
# Sequel::ColumnIntrospection.  To enable this for all datasets, run:
#
#   Sequel::Dataset.introspect_all_columns

module Sequel
  module ColumnsIntrospection
    # Attempt to guess the columns that will be returned
    # if there are columns selected, in order to skip a database
    # query to retrieve the columns.  This should work with
    # Symbols, SQL::Identifiers, SQL::QualifiedIdentifiers, and
    # SQL::AliasedExpressions.
    def columns
      return @columns if @columns
      return columns_without_introspection unless cols = opts[:select] and !cols.empty?
      probable_columns = cols.map{|c| probable_column_name(c)}
      if probable_columns.all?
        @columns = probable_columns
      else
        columns_without_introspection
      end
    end

    private

    # Return the probable name of the column, or nil if one
    # cannot be determined.
    def probable_column_name(c)
      case c
      when Symbol
        _, c, a = split_symbol(c)
        (a || c).to_sym
      when SQL::Identifier
        c.value.to_sym
      when SQL::QualifiedIdentifier
        col = c.column
        col.is_a?(SQL::Identifier) ? col.value.to_sym : col.to_sym
      when SQL::AliasedExpression
        a = c.aliaz
        a.is_a?(SQL::Identifier) ? a.value.to_sym : a.to_sym
      end
    end
  end

  class Dataset
    alias columns_without_introspection columns

    # Enable column introspection for every dataset.
    def self.introspect_all_columns
      include ColumnsIntrospection
      remove_method(:columns) if instance_methods(false).map{|x| x.to_s}.include?('columns')
    end
  end
end
