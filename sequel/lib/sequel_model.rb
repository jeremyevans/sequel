require 'sequel_core'

module Sequel
  class Model
    alias_method :model, :class
  end
end

files = %w[
  inflector inflections base hooks record schema associations 
  caching plugins validations eager_loading deprecated
]
dir = File.join(File.dirname(__FILE__), "sequel_model")
files.each {|f| require(File.join(dir, f))}

module Sequel
  class Model
    extend Enumerable
    extend Associations
    # Returns a string representation of the model instance including
    # the class name and values.
    def inspect
      "#<%s @values=%s>" % [self.class.name, @values.inspect]
    end
    
    # Defines a method that returns a filtered dataset.
    def self.subset(name, *args, &block)
      def_dataset_method(name){filter(*args, &block)}
    end
    
    # Finds a single record according to the supplied filter, e.g.:
    #
    #   Ticket.find :author => 'Sharon' # => record
    #   Ticket.find {:price == 17} # => Dataset
    #
    def self.find(*args, &block)
      dataset.filter(*args, &block).first
    end
    
    # TODO: doc
    def self.[](*args)
      args = args.first if (args.size == 1)
      if args === true || args === false
        raise Error::InvalidFilter, "Did you mean to supply a hash?"
      end
      dataset[(Hash === args) ? args : primary_key_hash(args)]
    end
    
    # TODO: doc
    def self.fetch(*args)
      db.fetch(*args).set_model(self)
    end

    # Like find but invokes create with given conditions when record does not
    # exists.
    def self.find_or_create(cond)
      find(cond) || create(cond)
    end

    # Deletes all records in the model's table.
    def self.delete_all
      dataset.delete
    end

    # Like delete_all, but invokes before_destroy and after_destroy hooks if used.
    def self.destroy_all
      dataset.destroy
    end

    # Add dataset methods via metaprogramming
    DATASET_METHODS = %w'all avg count delete distinct eager eager_graph each each_page 
       empty? except exclude filter first from_self full_outer_join graph 
       group group_and_count group_by having import inner_join insert 
       insert_multiple intersect interval invert_order join join_table last 
       left_outer_join limit multi_insert naked order order_by order_more 
       paginate print query range reverse_order right_outer_join select 
       select_all select_more set set_graph_aliases single_value size to_csv 
       transform union uniq unordered update where'

    def_dataset_method *DATASET_METHODS
  end
end
