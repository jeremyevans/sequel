require 'time'
require 'date'
require 'yaml'

require File.join(File.dirname(__FILE__), 'dataset/sql')
require File.join(File.dirname(__FILE__), 'dataset/sequelizer')
require File.join(File.dirname(__FILE__), 'dataset/convenience')

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
  #   def update(*args, &block)
  #     @db.synchronize do
  #       @db.execute(update_sql(*args, &block)).affected_rows
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
    include Sequelizer
    include SQL
    include Convenience
    
    attr_reader :db
    attr_accessor :opts
    
    alias_method :all, :to_a
    alias_method :size, :count
  
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
      @row_proc = nil
      @transform = nil
    end
    
    # Returns a new clone of the dataset with with the given options merged.
    def clone(opts = {})
      c = super()
      c.set_options @opts.merge(opts)
      c
    end
    
    def set_options(opts) #:nodoc:
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
    
    # Inserts the supplied values into the associated table.
    def <<(*args)
      insert(*args)
    end
  
    # Updates the dataset with the given values.
    def set(*args, &block)
      update(*args, &block)
    end
    
    # Iterates over the records in the dataset
    def each(opts = nil, &block)
      fetch_rows(select_sql(opts), &block)
      self
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
      d = clone(:naked => true, :models => nil, :polymorphic_key => nil)
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
      when nil # set_model(nil) => no
        # no argument provided, so the dataset is denuded
        @opts.merge!(:naked => true, :models => nil, :polymorphic_key => nil)
        remove_row_proc
        # extend_with_stock_each
      when Class
        # isomorphic model
        @opts.merge!(:naked => nil, :models => {nil => key}, :polymorphic_key => nil)
        if key.respond_to?(:load)
          # the class has a values setter method, so we use it
          set_row_proc {|h| key.load(h, *args)}
        else
          # otherwise we just pass the hash to the constructor
          set_row_proc {|h| key.new(h, *args)}
        end
        extend_with_destroy
      when Symbol
        # polymorphic model
        hash = args.shift || raise(ArgumentError, "No class hash supplied for polymorphic model")
        @opts.merge!(:naked => true, :models => hash, :polymorphic_key => key)
        if hash.values.first.respond_to?(:load)
          # the class has a values setter method, so we use it
          set_row_proc do |h|
            c = hash[h[key]] || hash[nil] || \
              raise(Error, "No matching model class for record (#{polymorphic_key} => #{h[polymorphic_key].inspect})")
            c.load(h, *args)
          end
        else
          # otherwise we just pass the hash to the constructor
          set_row_proc do |h|
            c = hash[h[key]] || hash[nil] || \
              raise(Error, "No matching model class for record (#{polymorphic_key} => #{h[polymorphic_key].inspect})")
            c.new(h, *args)
          end
        end
        extend_with_destroy
      else
        raise ArgumentError, "Invalid model specified"
      end
      self
    end
    
    # Overrides the each method to pass the values through a filter. The filter
    # receives as argument a hash containing the column values for the current
    # record. The filter should return a value which is then passed to the 
    # iterating block. In order to elucidate, here's a contrived example:
    #
    #   dataset.set_row_proc {|h| h.merge(:xxx => 'yyy')}
    #   dataset.first[:xxx] #=> "yyy" # always!
    #
    def set_row_proc(&filter)
      @row_proc = filter
      update_each_method
    end
    
    # Removes the row making proc.
    def remove_row_proc
      @row_proc = nil
      update_each_method
    end
    
    STOCK_TRANSFORMS = {
      :marshal => [proc {|v| Marshal.load(v)}, proc {|v| Marshal.dump(v)}],
      :yaml => [proc {|v| YAML.load v if v}, proc {|v| v.to_yaml}]
    }
    
    # Sets a value transform which is used to convert values loaded and saved
    # to/from the database. The transform should be supplied as a hash. Each
    # value in the hash should be an array containing two proc objects - one
    # for transforming loaded values, and one for transforming saved values.
    # The following example demonstrates how to store Ruby objects in a dataset
    # using Marshal serialization:
    #
    #   dataset.transform(:obj => [
    #     proc {|v| Marshal.load(v)},
    #     proc {|v| Marshal.dump(v)}
    #   ])
    #
    #   dataset.insert_sql(:obj => 1234) #=>
    #   "INSERT INTO items (obj) VALUES ('\004\bi\002\322\004')"
    #
    # Another form of using transform is by specifying stock transforms:
    # 
    #   dataset.transform(:obj => :marshal)
    #
    # The currently supported stock transforms are :marshal and :yaml.
    def transform(t)
      @transform = t
      t.each do |k, v|
        case v
        when Array
          if (v.size != 2) || !v.first.is_a?(Proc) && !v.last.is_a?(Proc)
            raise Error::InvalidTransform, "Invalid transform specified"
          end
        else
          unless v = STOCK_TRANSFORMS[v]
            raise Error::InvalidTransform, "Invalid transform specified"
          else
            t[k] = v
          end
        end
      end
      update_each_method
      self
    end
    
    # Applies the value transform for data loaded from the database.
    def transform_load(r)
      @transform.each do |k, tt|
        if r.has_key?(k)
          r[k] = tt[0][r[k]]
        end
      end
      r
    end
    
    # Applies the value transform for data saved to the database.
    def transform_save(r)
      @transform.each do |k, tt|
        if r.has_key?(k)
          r[k] = tt[1][r[k]]
        end
      end
      r
    end
    
    # Updates the each method according to whether @row_proc and @transform are
    # set or not.
    def update_each_method
      # warning: ugly code generation ahead
      if @row_proc && @transform
        class << self
          def each(opts = nil, &block)
            if opts && opts[:naked]
              fetch_rows(select_sql(opts)) {|r| block[transform_load(r)]}
            else
              fetch_rows(select_sql(opts)) {|r| block[@row_proc[transform_load(r)]]}
            end
            self
          end
        end
      elsif @row_proc
        class << self
          def each(opts = nil, &block)
            if opts && opts[:naked]
              fetch_rows(select_sql(opts), &block)
            else
              fetch_rows(select_sql(opts)) {|r| block[@row_proc[r]]}
            end
            self
          end
        end
      elsif @transform
        class << self
          def each(opts = nil, &block)
            fetch_rows(select_sql(opts)) {|r| block[transform_load(r)]}
            self
          end
        end
      else
        class << self
          def each(opts = nil, &block)
            fetch_rows(select_sql(opts), &block)
            self
          end
        end
      end
    end
    
    # Extends the dataset with a destroy method, that calls destroy for each
    # record in the dataset.
    def extend_with_destroy
      unless respond_to?(:destroy)
        meta_def(:destroy) do
          unless @opts[:models]
            raise Error, "No model associated with this dataset"
          end
          count = 0
          @db.transaction {each {|r| count += 1; r.destroy}}
          count
        end
      end
    end

    @@dataset_classes = []

    def self.dataset_classes #:nodoc:
      @@dataset_classes
    end

    def self.inherited(c) #:nodoc:
      @@dataset_classes << c
    end
  end
end

