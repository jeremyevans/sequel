# The null_dataset extension adds the Dataset#nullify method, which
# returns a cloned dataset that will never issue a query to the
# database.  It implements the null object pattern for datasets.
#
# To load the extension:
#
#   Sequel.extension :null_dataset
#
# The most common usage is probably in a method that must return
# a dataset, where the method knows the dataset shouldn't return
# anything.  With standard Sequel, you'd probably just add a
# WHERE condition that is always false, but that still results
# in a query being sent to the database, and can be overridden
# using #unfiltered, the OR operator, or a UNION.
#
# Usage:
#
#   ds = DB[:items].nullify.where(:a=>:b).select(:c)
#   ds.sql # => "SELECT c FROM items WHERE (a = b)"
#   ds.all # => [] # no query sent to the database
#
# Note that there is one case where a null dataset will sent
# a query to the database.  If you call #columns on a nulled
# dataset and the dataset doesn't have an already cached
# version of the columns, it will create a new dataset with
# the same options to get the columns.
#
# This extension uses Object#extend at runtime, which can hurt performance.

module Sequel
  class Dataset
    module Nullifiable
    end

    module NullDataset
      # Create a new dataset from the dataset (which won't
      # be nulled) to get the columns if they aren't already cached.
      def columns
        @columns ||= db.dataset.clone(@opts).columns
      end

      # Return 0 without sending a database query.
      def delete
        0
      end

      # Return self without sending a database query, never yielding.
      def each
        self
      end

      # Return nil without sending a database query, never yielding.
      def fetch_rows(sql)
        nil
      end

      # Return nil without sending a database query.
      def insert(*)
        nil
      end

      # Return nil without sending a database query.
      def truncate
        nil
      end

      # Return 0 without sending a database query.
      def update(v={})
        0
      end

      protected

      # Return nil without sending a database query.
      def _import(columns, values, opts)
        nil
      end

      private

      # Just in case these are called directly by some internal code,
      # make them noops.  There's nothing we can do if the db
      # is accessed directly to make a change, though.
      (%w'_ddl _dui _insert' << '').each do |m|
        class_eval("private; def execute#{m}(sql, opts={}) end", __FILE__, __LINE__)
      end
    end

    # Return a cloned nullified dataset.
    def nullify
      clone.nullify!
    end

    # Nullify the current dataset
    def nullify!
      Sequel::Deprecation.deprecate('Loading the null_dataset extension globally', "Please use Database/Dataset#extension to load the extension into this dataset") unless is_a?(Nullifiable)
      extend NullDataset
    end
  end

  Dataset.register_extension(:null_dataset, Dataset::Nullifiable)
end
