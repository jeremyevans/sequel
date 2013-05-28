module Sequel
  class Database
    # ---------------------
    # :section: 3 - Methods that create datasets
    # These methods all return instances of this database's dataset class.
    # ---------------------

    # Returns a dataset for the database. If the first argument is a string,
    # the method acts as an alias for Database#fetch, returning a dataset for
    # arbitrary SQL, with or without placeholders:
    #
    #   DB['SELECT * FROM items'].all
    #   DB['SELECT * FROM items WHERE name = ?', my_name].all
    #
    # Otherwise, acts as an alias for Database#from, setting the primary
    # table for the dataset:
    #
    #   DB[:items].sql #=> "SELECT * FROM items"
    def [](*args)
      args.first.is_a?(String) ? fetch(*args) : from(*args)
    end
    
    # Returns a blank dataset for this database.
    #
    #   DB.dataset # SELECT *
    #   DB.dataset.from(:items) # SELECT * FROM items
    def dataset(opts=(no_arg_given=true; nil))
      # REMOVE40
      if no_arg_given
        @dataset_class.new(self)
      else
        @dataset_class.new(self, opts)
      end
    end

    # Fetches records for an arbitrary SQL statement. If a block is given,
    # it is used to iterate over the records:
    #
    #   DB.fetch('SELECT * FROM items'){|r| p r}
    #
    # The +fetch+ method returns a dataset instance:
    #
    #   DB.fetch('SELECT * FROM items').all
    #
    # +fetch+ can also perform parameterized queries for protection against SQL
    # injection:
    #
    #   DB.fetch('SELECT * FROM items WHERE name = ?', my_name).all
    def fetch(sql, *args, &block)
      ds = @default_dataset.with_sql(sql, *args)
      ds.each(&block) if block
      ds
    end
    
    # Returns a new dataset with the +from+ method invoked. If a block is given,
    # it is used as a filter on the dataset.
    #
    #   DB.from(:items) # SELECT * FROM items
    #   DB.from(:items){id > 2} # SELECT * FROM items WHERE (id > 2)
    def from(*args, &block)
      ds = @default_dataset.from(*args)
      block ? ds.filter(&block) : ds
    end
    
    # Returns a new dataset with the select method invoked.
    #
    #   DB.select(1) # SELECT 1
    #   DB.select{server_version{}} # SELECT server_version()
    #   DB.select(:id).from(:items) # SELECT id FROM items
    def select(*args, &block)
      @default_dataset.select(*args, &block)
    end
  end
end
