%w'callback convenience pagination query schema sequelizer sql'.each do |f|
  require "sequel_core/dataset/#{f}"
end

module Sequel
  # A Dataset represents a view of a the data in a database, constrained by
  # specific parameters such as filtering conditions, order, etc. Datasets
  # can be used to create, retrieve, update and delete records.
  # 
  # Query results are always retrieved on demand, so a dataset can be kept
  # around and reused indefinitely:
  #
  #   my_posts = DB[:posts].filter(:author => 'david') # no records are retrieved
  #   p my_posts.all # records are now retrieved
  #   ...
  #   p my_posts.all # records are retrieved again
  #
  # In order to provide this functionality, dataset methods such as where, 
  # select, order, etc. return modified copies of the dataset, so you can
  # use different datasets to access data:
  #
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
  # Sequel::Dataset. The following methods should be overridden by the adapter
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
  #
  # === Methods added via metaprogramming
  #
  # Some methods are added via metaprogramming:
  #
  # * ! methods - These methods are the same as their non-! counterparts,
  #   but they modify the receiver instead of returning a modified copy
  #   of the dataset.
  # * inner_join, full_outer_join, right_outer_join, left_outer_join - 
  #   This methods are shortcuts to join_table with the join type
  #   already specified.
  class Dataset
    include Enumerable
    
    # The dataset options that require the removal of cached columns
    # if changed.
    COLUMN_CHANGE_OPTS = [:select, :sql, :from, :join].freeze

    # Array of all subclasses of Dataset
    DATASET_CLASSES = []

    # All methods that should have a ! method added that modifies
    # the receiver.
    MUTATION_METHODS = %w'and distinct exclude exists filter from from_self full_outer_join graph
    group group_and_count group_by having inner_join intersect invert join
    left_outer_join limit naked or order order_by order_more paginate query reject
    reverse reverse_order right_outer_join select select_all select_more
    set_graph_aliases set_model sort sort_by unfiltered union unordered where'.collect{|x| x.to_sym}

    NOTIMPL_MSG = "This method must be overridden in Sequel adapters".freeze
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

    # The database that corresponds to this dataset
    attr_accessor :db

    # The hash of options for this dataset, keys are symbols.
    attr_accessor :opts

    # The row_proc for this database, should be a Proc that takes
    # a single hash argument and returns the object you want to
    # fetch_rows to return.
    attr_accessor :row_proc

    # Whether to quote identifiers for this dataset
    attr_writer :quote_identifiers
    
    # Constructs a new instance of a dataset with an associated database and 
    # options. Datasets are usually constructed by invoking Database methods:
    #
    #   DB[:posts]
    #
    # Or:
    #
    #   DB.dataset # the returned dataset is blank
    #
    # Sequel::Dataset is an abstract class that is not useful by itself. Each
    # database adaptor should provide a descendant class of Sequel::Dataset.
    def initialize(db, opts = nil)
      @db = db
      @quote_identifiers = db.quote_identifiers? if db.respond_to?(:quote_identifiers?)
      @opts = opts || {}
      @row_proc = nil
      @transform = nil
    end
    
    ### Class Methods ###

    # The array of dataset subclasses.
    def self.dataset_classes
      DATASET_CLASSES
    end

    # Setup mutation (e.g. filter!) methods.  These operate the same as the
    # non-! methods, but replace the options of the current dataset with the
    # options of the resulting dataset.
    def self.def_mutation_method(*meths)
      meths.each do |meth|
        class_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end")
      end
    end

    # Add the subclass to the array of subclasses.
    def self.inherited(c)
      DATASET_CLASSES << c
    end
    
    ### Instance Methods ###

    # Alias for insert, but not aliased directly so subclasses
    # don't have to override both methods.
    def <<(*args)
      insert(*args)
    end

    # Return the dataset as a column with the given alias, so it can be used in the
    # SELECT clause. This dataset should result in a single row and a single column.
    def as(aliaz)
      ::Sequel::SQL::AliasedExpression.new(self, aliaz)
    end

    # Returns an array with all records in the dataset. If a block is given,
    # the array is iterated over after all items have been loaded.
    def all(opts = nil, &block)
      a = []
      each(opts) {|r| a << r}
      post_load(a)
      a.each(&block) if block
      a
    end
  
    # Returns a new clone of the dataset with with the given options merged.
    # If the options changed include options in COLUMN_CHANGE_OPTS, the cached
    # columns are deleted.
    def clone(opts = {})
      c = super()
      c.opts = @opts.merge(opts)
      c.instance_variable_set(:@columns, nil) if c.options_overlap(COLUMN_CHANGE_OPTS)
      c
    end
    
    # Returns the columns in the result set in their true order.
    # If the columns are currently cached, returns the cached value. Otherwise,
    # a SELECT query is performed to get a single row. Adapters are expected
    # to fill the columns cache with the column information when a query is performed.
    # If the dataset does not have any rows, this will be an empty array.
    # If you are looking for all columns for a single table, see Schema::SQL#schema.
    def columns
      return @columns if @columns
      ds = unfiltered.unordered.clone(:distinct => nil)
      ds.single_record
      @columns = ds.instance_variable_get(:@columns)
      @columns || []
    end
    
    # Remove the cached list of columns and do a SELECT query to find
    # the columns.
    def columns!
      @columns = nil
      columns
    end
    
    # Add a mutation method to this dataset instance.
    def def_mutation_method(*meths)
      meths.each do |meth|
        instance_eval("def #{meth}!(*args, &block); mutation_method(:#{meth}, *args, &block) end")
      end
    end

    # Deletes the records in the dataset. Adapters should override this method.
    def delete(opts = nil)
      raise NotImplementedError, NOTIMPL_MSG
    end
    
    # Iterates over the records in the dataset.
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
    # If the dataset is associated with a single model class, a key of nil
    # is used.  For datasets with polymorphic models, the keys are
    # values of the polymorphic column and the values are the corresponding
    # model classes to which they map.
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
    
    # Whether this dataset quotes identifiers.
    def quote_identifiers?
      @quote_identifiers
    end

    # Alias for set, but not aliased directly so subclasses
    # don't have to override both methods.
    def set(*args, &block)
      update(*args, &block)
    end

    # Associates or disassociates the dataset with a model(s). If
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
    # To make sure that there is always a default model class, the hash provided
    # should have a default value.  To make the dataset map string values to
    # model classes, and keep a good default, try:
    #
    #   dataset.set_model(:kind, Hash.new{|h,k| h[k] = (k.constantize rescue DefaultClass)})
    def set_model(key, *args)
      # This code is more verbose then necessary for performance reasons
      case key
      when nil # set_model(nil) => no argument provided, so the dataset is denuded
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
        if (hash.empty? ? (hash[nil] rescue nil) : hash.values.first).respond_to?(:load)
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
  
    # Add the mutation methods via metaprogramming
    def_mutation_method(*MUTATION_METHODS)

    protected

    # Return true if the dataset has a non-nil value for any key in opts.
    def options_overlap(opts)
      !(@opts.collect{|k,v| k unless v.nil?}.compact & opts).empty?
    end

    private

    # Modify the receiver with the results of sending the meth, args, and block
    # to the receiver and merging the options of the resulting dataset into
    # the receiver's options.
    def mutation_method(meth, *args, &block)
      copy = send(meth, *args, &block)
      @opts.merge!(copy.opts)
      self
    end
  end
end
