# The query extension adds Sequel::Dataset#query which allows
# a different way to construct queries instead of the usual
# method chaining.  See Sequel::Dataset#query for details.
#
# To load the extension, do:
#
#   Sequel.extension :query

module Sequel
  class Database
    # Return a dataset modified by the query block
    def query(&block)
      dataset.query(&block)
    end
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
end
