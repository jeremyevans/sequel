module Sequel
  class Dataset
    module QueryBlockCopy #:nodoc:
      def each(*args); raise Error, "#each cannot be invoked inside a query block."; end
      def insert(*args); raise Error, "#insert cannot be invoked inside a query block."; end
      def update(*args); raise Error, "#update cannot be invoked inside a query block."; end
      def delete(*args); raise Error, "#delete cannot be invoked inside a query block."; end

      def clone(opts = nil)
        @opts.merge!(opts)
        self
      end
    end

    # Translates a query block into a dataset. Query blocks can be useful
    # when expressing complex SELECT statements, e.g.:
    #
    #   dataset = DB[:items].query do
    #     select :x, :y, :z
    #     where {:x > 1 && :y > 2}
    #     order_by :z.DESC
    #   end
    #
    def query(&block)
      copy = clone({})
      copy.extend(QueryBlockCopy)
      copy.instance_eval(&block)
      clone(copy.opts)
    end
  end
end
