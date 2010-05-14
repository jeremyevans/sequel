module Sequel
  class Database
    # ---------------------
    # :section: Methods that create datasets
    # These methods all return instances of this database's dataset class.
    # ---------------------

    # Returns a dataset from the database. If the first argument is a string,
    # the method acts as an alias for Database#fetch, returning a dataset for
    # arbitrary SQL:
    #
    #   DB['SELECT * FROM items WHERE name = ?', my_name].all
    #
    # Otherwise, acts as an alias for Database#from, setting the primary
    # table for the dataset:
    #
    #   DB[:items].sql #=> "SELECT * FROM items"
    def [](*args)
      (String === args.first) ? fetch(*args) : from(*args)
    end
    
    # Returns a blank dataset for this database
    def dataset
      ds = Sequel::Dataset.new(self)
    end
    
    # Fetches records for an arbitrary SQL statement. If a block is given,
    # it is used to iterate over the records:
    #
    #   DB.fetch('SELECT * FROM items'){|r| p r}
    #
    # The method returns a dataset instance:
    #
    #   DB.fetch('SELECT * FROM items').all
    #
    # Fetch can also perform parameterized queries for protection against SQL
    # injection:
    #
    #   DB.fetch('SELECT * FROM items WHERE name = ?', my_name).all
    def fetch(sql, *args, &block)
      ds = dataset.with_sql(sql, *args)
      ds.each(&block) if block
      ds
    end
    
    # Returns a new dataset with the from method invoked. If a block is given,
    # it is used as a filter on the dataset.
    def from(*args, &block)
      ds = dataset.from(*args)
      block ? ds.filter(&block) : ds
    end
    
    # Returns a new dataset with the select method invoked.
    def select(*args, &block)
      dataset.select(*args, &block)
    end
  end
end
