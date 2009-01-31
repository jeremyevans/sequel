module Sequel
  class Dataset
    # Translates a query block into a dataset. Query blocks can be useful
    # when expressing complex SELECT statements, e.g.:
    #
    #   dataset = DB[:items].query do
    #     select :x, :y, :z
    #     filter{|o| (o.x > 1) & (o.y > 2)}
    #     order :z.desc
    #   end
    #
    # Which is the same as:
    #
    #  dataset = DB[:items].select(:x, :y, :z).filter{|o| (o.x > 1) & (o.y > 2)}.order(:z.desc)
    #
    # Note that inside a call to query, you cannot call each, insert, update,
    # or delete (or any method that calls those), or Sequel will raise an
    # error.
    def query(&block)
      copy = clone({})
      copy.extend(QueryBlockCopy)
      copy.instance_eval(&block)
      clone(copy.opts)
    end

    # Module used by Dataset#query that has the effect of making all
    # dataset methods into !-style methods that modify the receiver.
    module QueryBlockCopy
      %w'each insert update delete'.each do |meth|
        define_method(meth){|*args| raise Error, "##{meth} cannot be invoked inside a query block."}
      end

      # Merge the given options into the receiver's options and return the receiver
      # instead of cloning the receiver.
      def clone(opts = nil)
        @opts.merge!(opts)
        self
      end
    end
  end
end
