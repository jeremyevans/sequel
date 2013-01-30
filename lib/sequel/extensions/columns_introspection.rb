# The columns_introspection extension attempts to introspect the
# selected columns for a dataset before issuing a query.  If it
# thinks it can guess correctly at the columns the query will use,
# it will return the columns without issuing a database query.
# This method is not fool-proof, it's possible that some databases
# will use column names that Sequel does not expect.
#
# To attempt to introspect columns for a single dataset:
#
#   ds.extension(:columns_introspection)
#
# To attempt to introspect columns for all datasets on a single database:
#
#   DB.extension(:columns_introspection)
#
# To attempt to introspect columns for all datasets on all databases:
#
#   Sequel.extension :columns_introspection
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
      if (pcs = probable_columns) && pcs.all?
        @columns = pcs
      else
        columns_without_introspection
      end
    end

    protected

    # Return an array of probable column names for the dataset, or
    # nil if it is not possible to determine that through
    # introspection.
    def probable_columns
      if (cols = opts[:select]) && !cols.empty?
        cols.map{|c| probable_column_name(c)}
      elsif !opts[:join] && (f = opts[:from]) && f.length == 1 && (ds = f.first) &&
            (ds.is_a?(Dataset) || (ds.is_a?(SQL::AliasedExpression) && (ds = ds.expression).is_a?(Dataset)))
        ds.probable_columns
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

  Dataset.register_extension(:columns_introspection, Sequel::ColumnsIntrospection)
end

