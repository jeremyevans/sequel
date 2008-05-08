%w'sql sequelizer convenience callback pagination'.each do |f|
 require "sequel_core/dataset/#{f}"
end

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
  #   old_posts = posts.filter('stamp < ?', Date.today - 7)
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
    include Callback
    
    MUTATION_METHODS = %w'and distinct exclude exists filter from from_self full_outer_join graph
    group group_and_count group_by having inner_join intersect invert_order join
    left_outer_join limit naked or order order_by order_more paginate query reject
    reverse reverse_order right_outer_join select select_all select_more
    set_graph_aliases set_model sort sort_by union unordered where'.collect{|x| x.to_sym}
    NOTIMPL_MSG = "This method must be overriden in Sequel adapters".freeze
    STOCK_TRANSFORMS = {
      :marshal => [
        # for backwards-compatibility we support also non-base64-encoded values.
        proc {|v| Marshal.load(v.unpack('m')[0]) rescue Marshal.load(v)}, 
        proc {|v| [Marshal.dump(v)].pack('m')}
      ],
      :yaml => [
        proc {|v| YAML.load v if v}, 
        proc {|v| v.to_yaml}
      ]
    }
    
    @@dataset_classes = []

    attr_accessor :db, :opts, :row_proc
    
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
    
    ### Class Methods ###

    def self.dataset_classes #:nodoc:
      @@dataset_classes
    end

    # Setup mutation (e.g. filter!) methods
    def self.def_mutation_method(*meths)
      meths.each do |meth|
        class_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end")
      end
    end

    def self.inherited(c) #:nodoc:
      @@dataset_classes << c
    end
    
    ### Instance Methods ###

    # Inserts the supplied values into the associated table.
    def <<(*args)
      insert(*args)
    end
  
    # Returns an array with all records in the dataset. If a block is given,
    # the array is iterated over.
    def all(opts = nil, &block)
      a = []
      each(opts) {|r| a << r}
      post_load(a)
      a.each(&block) if block
      a
    end
  
    # Returns a new clone of the dataset with with the given options merged.
    def clone(opts = {})
      c = super()
      c.opts = @opts.merge(opts)
      c.instance_variable_set(:@columns, nil)
      c
    end
    
    # Returns the columns in the result set in their true order. The stock 
    # implementation returns the content of @columns. If @columns is nil,
    # a query is performed. Adapters are expected to fill @columns with the
    # column information when a query is performed.
    def columns
      first unless @columns
      @columns || []
    end
    
    def columns!
      first
      @columns || []
    end
    
    def def_mutation_method(*meths)
      meths.each do |meth|
        instance_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end")
      end
    end

    # Deletes the records in the dataset. Adapters should override this method.
    def delete(opts = nil)
      raise NotImplementedError, NOTIMPL_MSG
    end
    
    # Iterates over the records in the dataset
    def each(opts = nil, &block)
      if graph = @opts[:graph]
        graph_each(opts, &block)
      else
        row_proc = @row_proc unless opts && opts[:naked]
        transform = @transform
        fetch_rows(select_sql(opts)) do |r|
          r = transform_load(r) if transform
          r = row_proc[r] if row_proc
          yield r
        end
      end
      self
    end

    # Executes a select query and fetches records, passing each record to the
    # supplied block. Adapters should override this method.
    def fetch_rows(sql, &block)
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    # Inserts values into the associated table. Adapters should override this
    # method.
    def insert(*values)
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    # Returns a string representation of the dataset including the class name 
    # and the corresponding SQL select statement.
    def inspect
      "#<#{self.class}: #{sql.inspect}>"
    end

    # Returns the the model classes associated with the dataset as a hash.
    def model_classes
      @opts[:models]
    end
    
    # Returns a naked dataset clone - i.e. a dataset that returns records as
    # hashes rather than model objects.
    def naked
      clone.set_model(nil)
    end
    
    # Returns the column name for the polymorphic key.
    def polymorphic_key
      @opts[:polymorphic_key]
    end
    
    # Updates the dataset with the given values.
    def set(*args, &block)
      update(*args, &block)
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
      # This code is more verbose then necessary for performance reasons
      case key
      when nil # set_model(nil) => no
        # no argument provided, so the dataset is denuded
        @opts.merge!(:naked => true, :models => nil, :polymorphic_key => nil)
        self.row_proc = nil
      when Class
        # isomorphic model
        @opts.merge!(:naked => nil, :models => {nil => key}, :polymorphic_key => nil)
        if key.respond_to?(:load)
          # the class has a values setter method, so we use it
          self.row_proc = proc{|h| key.load(h, *args)}
        else
          # otherwise we just pass the hash to the constructor
          self.row_proc = proc{|h| key.new(h, *args)}
        end
      when Symbol
        # polymorphic model
        hash = args.shift || raise(ArgumentError, "No class hash supplied for polymorphic model")
        @opts.merge!(:naked => true, :models => hash, :polymorphic_key => key)
        if hash.values.first.respond_to?(:load)
          # the class has a values setter method, so we use it
          self.row_proc = proc do |h|
            c = hash[h[key]] || hash[nil] || \
              raise(Error, "No matching model class for record (#{polymorphic_key} => #{h[polymorphic_key].inspect})")
            c.load(h, *args)
          end
        else
          # otherwise we just pass the hash to the constructor
          self.row_proc = proc do |h|
            c = hash[h[key]] || hash[nil] || \
              raise(Error, "No matching model class for record (#{polymorphic_key} => #{h[polymorphic_key].inspect})")
            c.new(h, *args)
          end
        end
      else
        raise ArgumentError, "Invalid model specified"
      end
      self
    end
    
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
      self
    end
    
    # Applies the value transform for data loaded from the database.
    def transform_load(r)
      r.inject({}) do |m, kv|
        k, v = *kv
        m[k] = (tt = @transform[k]) ? tt[0][v] : v
        m
      end
    end
    
    # Applies the value transform for data saved to the database.
    def transform_save(r)
      r.inject({}) do |m, kv|
        k, v = *kv
        m[k] = (tt = @transform[k]) ? tt[1][v] : v
        m
      end
    end
    
    # Updates values for the dataset. Adapters should override this method.
    def update(values, opts = nil)
      raise NotImplementedError, NOTIMPL_MSG
    end
  
    def_mutation_method(*MUTATION_METHODS)

    private
      def mutation_method(meth, *args, &block)
        copy = send(meth, *args, &block)
        @opts.merge!(copy.opts)
        self
      end
  end
end

