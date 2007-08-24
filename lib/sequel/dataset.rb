require 'time'
require 'date'

require File.join(File.dirname(__FILE__), 'dataset/dataset_sql')
require File.join(File.dirname(__FILE__), 'dataset/dataset_convenience')

module Sequel
  # A Dataset represents a view of a the data in a database, constrained by
  # specific parameters such as filtering conditions, order, etc. Datasets
  # can be used to create, retrieve, update and delete records.
  # 
  # Query results are always retrieved on demand, so a dataset can be kept
  # around and reused indefinitely:
  #   my_posts = DB[:posts].filter(:author => 'david') # no records are retrieved
  #   p my_posts.all # records are now retrieved
  #   ...
  #   p my_posts.all # records are retrieved again
  #
  # In order to provide this functionality, dataset methods such as where, 
  # select, order, etc. return modified copies of the dataset, so you can
  # use different datasets to access data:
  #   posts = DB[:posts]
  #   davids_posts = posts.filter(:author => 'david')
  #   old_posts = posts.filter('stamp < ?', 1.week.ago)
  #
  # Datasets are Enumerable objects, so they can be manipulated using any
  # of the Enumerable methods, such as map, inject, etc.
  #
  # === The Dataset Adapter Interface
  #
  # Each adapter should define its own dataset class as a descendant of
  # Sequel::Dataset. The following methods should be overriden by the adapter
  # Dataset class (each method with the stock implementation):
  #
  #   # Iterate over the results of the SQL query and call the supplied
  #   # block with each record (as a hash).
  #   def fetch_rows(sql, &block)
  #     @db.synchronize do
  #       r = @db.execute(sql)
  #       r.each(&block)
  #     end
  #   end
  #
  #   # Insert records.
  #   def insert(*values)
  #     @db.synchronize do
  #       @db.execute(insert_sql(*values)).last_insert_id
  #     end
  #   end
  #
  #   # Update records.
  #   def update(values, opts = nil)
  #     @db.synchronize do
  #       @db.execute(update_sql(values, opts)).affected_rows
  #     end
  #   end
  #
  #   # Delete records.
  #   def delete(opts = nil)
  #     @db.synchronize do
  #       @db.execute(delete_sql(opts)).affected_rows
  #     end
  #   end
  class Dataset
    include Enumerable
    include SQL # in dataset/dataset_sql.rb
    include Convenience # in dataset/dataset_convenience.rb
    
    attr_reader :db
    attr_accessor :opts
    
    alias all to_a
    alias size count
  
    # Constructs a new instance of a dataset with a database instance, initial
    # options and an optional record class. Datasets are usually constructed by
    # invoking Database methods:
    #   DB[:posts]
    # Or:
    #   DB.dataset # the returned dataset is blank
    #
    # Sequel::Dataset is an abstract class that is not useful by itself. Each
    # database adaptor should provide a descendant class of Sequel::Dataset.
    def initialize(db, opts = nil)
      @db = db
      @opts = opts || {}
    end
    
    # Returns a new instance of the dataset with with the give options merged.
    def clone_merge(opts)
      new_dataset = clone
      new_dataset.set_options(@opts.merge(opts))
      new_dataset
    end
    
    def set_options(opts)
      @opts = opts
      @columns = nil
    end
    
    NOTIMPL_MSG = "This method must be overriden in Sequel adapters".freeze
    
    # Executes a select query and fetches records, passing each record to the
    # supplied block. Adapters should override this method.
    def fetch_rows(sql, &block)
      # @db.synchronize do
      #   r = @db.execute(sql)
      #   r.each(&block)
      # end
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    # Inserts values into the associated table. Adapters should override this
    # method.
    def insert(*values)
      # @db.synchronize do
      #   @db.execute(insert_sql(*values)).last_insert_id
      # end
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    # Updates values for the dataset. Adapters should override this method.
    def update(values, opts = nil)
      # @db.synchronize do
      #   @db.execute(update_sql(values, opts)).affected_rows
      # end
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    # Deletes the records in the dataset. Adapters should override this method.
    def delete(opts = nil)
      # @db.synchronize do
      #   @db.execute(delete_sql(opts)).affected_rows
      # end
      raise NotImplementedError, NOTIMPL_MSG
    end
    
    # Returns the columns in the result set in their true order. The stock 
    # implementation returns the content of @columns. If @columns is nil,
    # a query is performed. Adapters are expected to fill @columns with the
    # column information when a query is performed.
    def columns
      first unless @columns
      @columns || []
    end
    
    def <<(*args)
      insert(*args)
    end
    
    # Iterates over the records in the dataset
    def each(opts = nil, &block)
      fetch_rows(select_sql(opts), &block)
    end
    
    # Returns the the model classes associated with the dataset as a hash.
    def model_classes
      @opts[:models]
    end
    
    # Returns the column name for the polymorphic key.
    def polymorphic_key
      @opts[:polymorphic_key]
    end
    
    # Returns a naked dataset clone - i.e. a dataset that returns records as
    # hashes rather than model objects.
    def naked
      d = clone_merge(:naked => true, :models => nil, :polymorphic_key => nil)
      d.set_model(nil)
      d
    end
    
    # Associates or disassociates the dataset with a model. If no argument or
    # nil is specified, the dataset is turned into a naked dataset and returns
    # records as hashes. If a model class specified, the dataset is modified
    # to return records as instances of the model class, e.g:
    #
    #   class MyModel
    #     def initialize(values)
    #       @values = values
    #       ...
    #     end
    #   end
    # 
    #   dataset.set_model(MyModel)
    #
    # You can also provide additional arguments to be passed to the model's
    # initialize method:
    #
    #   class MyModel
    #     def initialize(values, options)
    #       @values = values
    #       ...
    #     end
    #   end
    # 
    #   dataset.set_model(MyModel, :allow_delete => false)
    #  
    # The dataset can be made polymorphic by specifying a column name as the
    # polymorphic key and a hash mapping column values to model classes.
    #
    #   dataset.set_model(:kind, {1 => Person, 2 => Business})
    #
    # You can also set a default model class to fall back on by specifying a
    # class corresponding to nil:
    #
    #   dataset.set_model(:kind, {nil => DefaultClass, 1 => Person, 2 => Business})
    # 
    # To disassociate a model from the dataset, you can call the #set_model 
    # and specify nil as the class:
    # 
    #   dataset.set_model(nil)
    #
    def set_model(key, *args)
      # pattern matching
      case key
      when nil: # set_model(nil) => no
        # no argument provided, so the dataset is denuded
        @opts.merge!(:naked => true, :models => nil, :polymorphic_key => nil)
        extend_with_stock_each
      when Class:
        # isomorphic model
        @opts.merge!(:naked => nil, :models => {nil => key}, :polymorphic_key => nil)
        extend_with_model(key, *args)
        extend_with_destroy
      when Symbol:
        # polymorphic model
        hash = args.shift || raise(SequelError, "No class hash supplied for polymorphic model")
        @opts.merge!(:naked => true, :models => hash, :polymorphic_key => key)
        extend_with_polymorphic_model(key, hash, *args)
        extend_with_destroy
      else
        raise SequelError, "Invalid parameters specified"
      end
      self
    end
    
    private
    # Overrides the each method to convert records to model instances.
    def extend_with_model(c, *args)
      meta_def(:make_model_instance) {|r| c.new(r, *args)}
      m = Module.new do
        def each(opts = nil, &block)
          if opts && opts[:naked]
            fetch_rows(select_sql(opts), &block)
          else
            fetch_rows(select_sql(opts)) {|r| block.call(make_model_instance(r))}
          end
        end
      end
      extend(m)
    end
    
    # Overrides the each method to convert records to polymorphic model
    # instances. The model class is determined according to the value in the
    # key column.
    def extend_with_polymorphic_model(key, hash, *args)
      meta_def(:make_model_instance) do |r|
        c = hash[r[key]] || hash[nil] || \
          raise(SequelError, "No matching model class for record (#{polymorphic_key} => #{r[polymorphic_key].inspect})")
        c.new(r, *args)
      end
      m = Module.new do
        def each(opts = nil, &block)
          if opts && opts[:naked]
            fetch_rows(select_sql(opts), &block)
          else
            fetch_rows(select_sql(opts)) {|r| block.call(make_model_instance(r))}
          end
        end
      end
      extend(m)
    end
    
    # Extends the dataset with a destroy method, that calls destroy for each
    # record in the dataset.
    def extend_with_destroy
      unless respond_to?(:destroy)
        meta_def(:destroy) do
          raise SequelError, 'Dataset not associated with model' unless @opts[:models]
          count = 0
          @db.transaction {each {|r| count += 1; r.destroy}}
          count
        end
      end
    end
    
    # Restores the stock #each implementation.
    def extend_with_stock_each
      m = Module.new do
        def each(opts = nil, &block)
          fetch_rows(select_sql(opts), &block)
        end
      end
      extend(m)
    end
  end
end

