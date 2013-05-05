# The query extension adds Sequel::Dataset#query which allows
# a different way to construct queries instead of the usual
# method chaining.  See Sequel::Dataset#query for details.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds.extension(:query)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:query)

module Sequel
  module DatabaseQuery
    def self.extended(db)
      db.extend_datasets(DatasetQuery)
    end
  end

  class Database
    # Return a dataset modified by the query block
    def query(&block)
      Sequel::Deprecation.deprecate('Loading the query extension globally', "Please use Database#extension to load the extension into this database") unless is_a?(DatabaseQuery)
      dataset.query(&block)
    end
  end

  module DatasetQuery
  end

  class Dataset
    # Translates a query block into a dataset. Query blocks are an
    # alternative to Sequel's usual method chaining, by using
    # instance_eval with a proxy object:
    #
    #   dataset = DB[:items].query do
    #     select :x, :y, :z
    #     filter{(x > 1) & (y > 2)}
    #     reverse :z
    #   end
    #
    # Which is the same as:
    #
    #  dataset = DB[:items].select(:x, :y, :z).filter{(x > 1) & (y > 2)}.reverse(:z)
    def query(&block)
      Sequel::Deprecation.deprecate('Loading the query extension globally', "Please use Database/Dataset#extension to load the extension into this dataset") unless is_a?(DatasetQuery)
      query = Query.new(self)
      query.instance_eval(&block)
      query.dataset
    end

    # Proxy object used by Dataset#query.
    class Query < Sequel::BasicObject
      # The current dataset in the query.  This changes on each method call.
      attr_reader :dataset
     
      def initialize(dataset)
        @dataset = dataset
      end

      # Replace the query's dataset with dataset returned by the method call.
      def method_missing(method, *args, &block)
        @dataset = @dataset.send(method, *args, &block)
        raise(Sequel::Error, "method #{method.inspect} did not return a dataset") unless @dataset.is_a?(Dataset)
        self
      end
    end
  end

  Dataset.register_extension(:query, DatasetQuery)
  Database.register_extension(:query, DatabaseQuery)
end
