module Sequel
  class Model
    # Whether to lazily load the schema for future subclasses.  Unless turned
    # off, checks the database for the table schema whenever a subclass is
    # created
    @@lazy_load_schema = false

    @allowed_columns = nil
    @dataset_methods = {}
    @primary_key = :id
    @raise_on_save_failure = true
    @restrict_primary_key = true
    @restricted_columns = nil
    @sti_dataset = nil
    @sti_key = nil
    @strict_param_setting = true
    @typecast_empty_string_to_nil = true
    @typecast_on_assignment = true

    # Which columns should be the only columns allowed in a call to set
    # (default: all columns).
    metaattr_reader :allowed_columns

    # Hash of dataset methods to add to this class and subclasses when
    # set_dataset is called.
    metaattr_reader :dataset_methods

    # The default primary key for classes (default: :id)
    metaattr_reader :primary_key

    # Whether to raise an error instead of returning nil on a failure
    # to save/create/save_changes/etc.
    metaattr_accessor :raise_on_save_failure

    # Which columns should not be update in a call to set
    # (default: no columns).
    metaattr_reader :restricted_columns

    # The base dataset for STI, to which filters are added to get
    # only the models for the specific STI subclass.
    metaattr_reader :sti_dataset

    # The column name holding the STI key for this model
    metaattr_reader :sti_key

    # Whether new/set/update and their variants should raise an error
    # if an invalid key is used (either that doesn't exist or that
    # access is restricted to it).
    metaattr_accessor :strict_param_setting

    # Whether to typecast the empty string ('') to nil for columns that
    # are not string or blob.
    metaattr_accessor :typecast_empty_string_to_nil

    # Whether to typecast attribute values on assignment (default: true)
    metaattr_accessor :typecast_on_assignment

    # Dataset methods to proxy via metaprogramming
    DATASET_METHODS = %w'<< all avg count delete distinct eager eager_graph each each_page 
       empty? except exclude filter first from from_self full_outer_join get graph 
       group group_and_count group_by having import inner_join insert 
       insert_multiple intersect interval invert_order join join_table last 
       left_outer_join limit map multi_insert naked order order_by order_more 
       paginate print query range reverse_order right_outer_join select 
       select_all select_more server set set_graph_aliases single_value size to_csv to_hash
       transform union uniq unfiltered unordered update where'.map{|x| x.to_sym}

    # Instance variables that are inherited in subclasses
    INHERITED_INSTANCE_VARIABLES = {:@allowed_columns=>:dup, :@cache_store=>nil,
      :@cache_ttl=>nil, :@dataset_methods=>:dup, :@primary_key=>nil, 
      :@raise_on_save_failure=>nil, :@restricted_columns=>:dup, :@restrict_primary_key=>nil,
      :@sti_dataset=>nil, :@sti_key=>nil, :@strict_param_setting=>nil,
      :@typecast_empty_string_to_nil=>nil, :@typecast_on_assignment=>nil}

    # Returns the first record from the database matching the conditions.
    # If a hash is given, it is used as the conditions.  If another
    # object is given, it finds the first record whose primary key(s) match
    # the given argument(s).  If caching is used, the cache is checked
    # first before a dataset lookup is attempted unless a hash is supplied.
    def self.[](*args)
      args = args.first if (args.size == 1)

      if Hash === args
        dataset[args]
      else
        @cache_store ? cache_lookup(args) : dataset[primary_key_hash(args)]
      end
    end
    
    # Returns the columns in the result set in their original order.
    # Generally, this will used the columns determined via the database
    # schema, but in certain cases (e.g. models that are based on a joined
    # dataset) it will use Dataset#columns to find the columns, which
    # may be empty if the Dataset has no records.
    def self.columns
      @columns || set_columns(dataset.naked.columns)
    end
  
    # Creates new instance with values set to passed-in Hash, saves it
    # (running any callbacks), and returns the instance if the object
    # was saved correctly.  If there was an error saving the object,
    # returns false.
    def self.create(values = {}, &block)
      obj = new(values, &block)
      return unless obj.save
      obj
    end

    # Returns the dataset associated with the Model class.
    def self.dataset
      @dataset || raise(Error, "No dataset associated with #{self}")
    end
  
    # Returns the database associated with the Model class.
    def self.db
      return @db if @db
      @db = self == Model ? DATABASES.first : superclass.db
      raise(Error, "No database associated with #{self}") unless @db
      @db
    end
    
    # Sets the database associated with the Model class.
    def self.db=(db)
      @db = db
      if @dataset
        set_dataset(db[table_name])
      end
    end
    
    # Returns the cached schema information if available or gets it
    # from the database.
    def self.db_schema
      @db_schema ||= get_db_schema
    end

    # If a block is given, define a method on the dataset with the given argument name using
    # the given block as well as a method on the model that calls the
    # dataset method.
    #
    # If a block is not given, define a method on the model for each argument
    # that calls the dataset method of the same argument name.
    def self.def_dataset_method(*args, &block)
      raise(Error, "No arguments given") if args.empty?
      if block_given?
        raise(Error, "Defining a dataset method using a block requires only one argument") if args.length > 1
        meth = args.first
        @dataset_methods[meth] = block
        dataset.meta_def(meth, &block)
      end
      args.each{|arg| instance_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__)}
    end
    
    # Deletes all records in the model's table.
    def self.delete_all
      dataset.delete
    end
  
    # Like delete_all, but invokes before_destroy and after_destroy hooks if used.
    def self.destroy_all
      dataset.destroy
    end
  
    # Returns a dataset with custom SQL that yields model objects.
    def self.fetch(*args)
      db.fetch(*args).set_model(self)
    end
  
    # Modify and return eager loading dataset based on association options
    def self.eager_loading_dataset(opts, ds, select, associations)
      ds = ds.select(*select) if select
      ds = ds.order(*opts[:order]) if opts[:order]
      ds = ds.eager(opts[:eager]) if opts[:eager]
      ds = ds.eager_graph(opts[:eager_graph]) if opts[:eager_graph]
      ds = ds.eager(associations) unless associations.blank?
      ds = opts[:eager_block].call(ds) if opts[:eager_block]
      ds
    end

    # Finds a single record according to the supplied filter, e.g.:
    #
    #   Ticket.find :author => 'Sharon' # => record
    def self.find(*args, &block)
      dataset.filter(*args, &block).first
    end
    
    # Like find but invokes create with given conditions when record does not
    # exists.
    def self.find_or_create(cond)
      find(cond) || create(cond)
    end
  
    # If possible, set the dataset for the model subclass as soon as it
    # is created.  Also, inherit the INHERITED_INSTANCE_VARIABLES
    # from the parent class.
    def self.inherited(subclass)
      sup_class = subclass.superclass
      ivs = subclass.instance_variables.collect{|x| x.to_s}
      INHERITED_INSTANCE_VARIABLES.each do |iv, dup|
        next if ivs.include?(iv.to_s)
        sup_class_value = sup_class.instance_variable_get(iv)
        sup_class_value = sup_class_value.dup if dup == :dup && sup_class_value
        subclass.instance_variable_set(iv, sup_class_value)
      end
      unless ivs.include?("@dataset")
        begin
          if sup_class == Model
            subclass.set_dataset(Model.db[subclass.implicit_table_name]) unless subclass.name.blank?
          elsif ds = sup_class.instance_variable_get(:@dataset)
            subclass.set_dataset(sup_class.sti_key ? sup_class.sti_dataset.filter(sup_class.sti_key=>subclass.name.to_s) : ds.clone)
          end
        rescue
          nil
        end
      end
    end
  
    # Returns the implicit table name for the model class.
    def self.implicit_table_name
      name.demodulize.underscore.pluralize.to_sym
    end

    # Set whether to lazily load the schema for future model classes.
    # When the schema is lazy loaded, the schema information is grabbed
    # during the first instantiation of the class instead of
    # when the class is created.
    def self.lazy_load_schema=(value)
      @@lazy_load_schema = value
    end
  
    # Initializes a model instance as an existing record. This constructor is
    # used by Sequel to initialize model instances when fetching records.
    # #load requires that values be a hash where all keys are symbols. It
    # probably should not be used by external code.
    def self.load(values)
      new(values, true)
    end

    # Mark the model as not having a primary key. Not having a primary key
    # can cause issues, among which is that you won't be able to update records.
    def self.no_primary_key
      @primary_key = nil
    end

    # Returns primary key attribute hash.  If using a composite primary key
    # value such be an array with values for each primary key in the correct
    # order.  For a standard primary key, value should be an object with a
    # compatible type for the key.  If the model does not have a primary key,
    # raises an Error.
    def self.primary_key_hash(value)
      raise(Error, "#{self} does not have a primary key") unless key = @primary_key
      case key
      when Array
        hash = {}
        key.each_with_index{|k,i| hash[k] = value[i]}
        hash
      else
        {key => value}
      end
    end

    # Restrict the setting of the primary key(s) inside new/set/update.  Because
    # this is the default, this only make sense to use in a subclass where the
    # parent class has used unrestrict_primary_key.
    def self.restrict_primary_key
      @restrict_primary_key = true
    end

    # Whether or not setting the primary key inside new/set/update is
    # restricted, true by default.
    def self.restrict_primary_key?
      @restrict_primary_key
    end

    # Serializes column with YAML or through marshalling.  Arguments should be
    # column symbols, with an optional trailing hash with a :format key
    # set to :yaml or :marshal (:yaml is the default).  Setting this adds
    # a transform to the model and dataset so that columns values will be serialized
    # when saved and deserialized when returned from the database.
    def self.serialize(*columns)
      format = columns.extract_options![:format] || :yaml
      @transform = columns.inject({}) do |m, c|
        m[c] = format
        m
      end
      @dataset.transform(@transform) if @dataset
    end
  
    # Set the columns to allow in new/set/update.  Using this means that
    # any columns not listed here will not be modified.  If you have any virtual
    # setter methods (methods that end in =) that you want to be used in
    # new/set/update, they need to be listed here as well (without the =).
    #
    # It may be better to use (set|update)_only instead of this in places where
    # only certain columns may be allowed.
    def self.set_allowed_columns(*cols)
      @allowed_columns = cols
    end

    # Sets the dataset associated with the Model class. ds can be a Symbol
    # (specifying a table name in the current database), or a Dataset.
    # If a dataset is used, the model's database is changed to the given
    # dataset.  If a symbol is used, a dataset is created from the current
    # database with the table name given. Other arguments raise an Error.
    #
    # This sets the model of the the given/created dataset to the current model
    # and adds a destroy method to it.  It also extends the dataset with
    # the Associations::EagerLoading methods, and assigns a transform to it
    # if there is one associated with the model. Finally, it attempts to 
    # determine the database schema based on the given/created dataset unless
    # lazy_load_schema is set.
    def self.set_dataset(ds)
      @dataset = case ds
      when Symbol
        db[ds]
      when Dataset
        @db = ds.db
        ds
      else
        raise(Error, "Model.set_dataset takes a Symbol or a Sequel::Dataset")
      end
      @dataset.set_model(self)
      @dataset.extend(DatasetMethods)
      @dataset.extend(Associations::EagerLoading)
      @dataset.transform(@transform) if @transform
      @dataset_methods.each{|meth, block| @dataset.meta_def(meth, &block)} if @dataset_methods
      unless @@lazy_load_schema
        (@db_schema = get_db_schema) rescue nil
      end
      self
    end
    metaalias :dataset=, :set_dataset
  
    # Sets primary key, regular and composite are possible.
    #
    # Example:
    #   class Tagging < Sequel::Model
    #     # composite key
    #     set_primary_key :taggable_id, :tag_id
    #   end
    #
    #   class Person < Sequel::Model
    #     # regular key
    #     set_primary_key :person_id
    #   end
    #
    # You can set it to nil to not have a primary key, but that
    # cause certain things not to work, see #no_primary_key.
    def self.set_primary_key(*key)
      @primary_key = (key.length == 1) ? key[0] : key.flatten
    end

    # Set the columns to restrict in new/set/update.  Using this means that
    # any columns listed here will not be modified.  If you have any virtual
    # setter methods (methods that end in =) that you want not to be used in
    # new/set/update, they need to be listed here as well (without the =).
    #
    # It may be better to use (set|update)_except instead of this in places where
    # only certain columns may be allowed.
    def self.set_restricted_columns(*cols)
      @restricted_columns = cols
    end

    # Makes this model a polymorphic model with the given key being a string
    # field in the database holding the name of the class to use.  If the
    # key given has a NULL value or there are any problems looking up the
    # class, uses the current class.
    #
    # This should be used to set up single table inheritance for the model,
    # and it only makes sense to use this in the parent class.
    #
    # You should call sti_key after any calls to set_dataset in the model,
    # otherwise subclasses might not have the filters set up correctly.
    #
    # The filters that sti_key sets up in subclasses will not work if
    # those subclasses have further subclasses.  For those middle subclasses,
    # you will need to call set_dataset manually with the correct filter set.
    def self.set_sti_key(key)
      m = self
      @sti_key = key
      @sti_dataset = dataset
      dataset.set_model(key, Hash.new{|h,k| h[k] = (k.constantize rescue m)})
      before_create(:set_sti_key){send("#{key}=", model.name.to_s)}
    end

    # Returns the columns as a list of frozen strings instead
    # of a list of symbols.  This makes it possible to check
    # whether a column exists without creating a symbol, which
    # would be a memory leak if called with user input.
    def self.str_columns
      @str_columns ||= columns.map{|c| c.to_s.freeze}
    end
  
    # Defines a method that returns a filtered dataset.  Subsets
    # create dataset methods, so they can be chained for scoping.
    # For example:
    #
    #   Topic.subset(:popular, :num_posts > 100)
    #   Topic.subset(:recent, :created_on > Date.today - 7)
    #
    # Allows you to do:
    #
    #   Topic.filter(:username.like('%joe%')).popular.recent
    #
    # to get topics with a username that includes joe that
    # have more than 100 posts and were created less than
    # 7 days ago.
    def self.subset(name, *args, &block)
      def_dataset_method(name){filter(*args, &block)}
    end
    
    # Returns name of primary table for the dataset.
    def self.table_name
      dataset.opts[:from].first
    end

    # Allow the setting of the primary key(s) inside new/set/update.
    def self.unrestrict_primary_key
      @restrict_primary_key = false
    end

    # Add model methods that call dataset methods
    def_dataset_method(*DATASET_METHODS)

    ### Private Class Methods ###
    
    # Create the column accessors
    def self.def_column_accessor(*columns) # :nodoc:
      columns.each do |column|
        im = instance_methods.collect{|x| x.to_s}
        meth = "#{column}="
         define_method(column){self[column]} unless im.include?(column.to_s)
        unless im.include?(meth)
          define_method(meth) do |*v|
            len = v.length
            raise(ArgumentError, "wrong number of arguments (#{len} for 1)") unless len == 1
            self[column] = v.first 
          end
        end
      end
    end

    # Get the schema from the database, fall back on checking the columns
    # via the database if that will return inaccurate results or if
    # it raises an error.
    def self.get_db_schema(reload = false) # :nodoc:
      set_columns(nil)
      return nil unless @dataset
      schema_hash = {}
      ds_opts = dataset.opts
      single_table = ds_opts[:from] && (ds_opts[:from].length == 1) \
        && !ds_opts.include?(:join) && !ds_opts.include?(:sql)
      get_columns = proc{columns rescue []}
      if single_table && (schema_array = (db.schema(table_name, :reload=>reload) rescue nil))
        schema_array.each{|k,v| schema_hash[k] = v}
        if ds_opts.include?(:select)
          # Dataset only selects certain columns, delete the other
          # columns from the schema
          cols = get_columns.call
          schema_hash.delete_if{|k,v| !cols.include?(k)}
          cols.each{|c| schema_hash[c] ||= {}}
        else
          # Dataset is for a single table with all columns,
          # so set the columns based on the order they were
          # returned by the schema.
          cols = schema_array.collect{|k,v| k}
          set_columns(cols)
          # Also set the columns for the dataset, so the dataset
          # doesn't have to do a query to get them.
          dataset.instance_variable_set(:@columns, cols)
        end
      else
        # If the dataset uses multiple tables or custom sql or getting
        # the schema raised an error, just get the columns and
        # create an empty schema hash for it.
        get_columns.call.each{|c| schema_hash[c] = {}}
      end
      schema_hash
    end

    # Set the columns for this model, reset the str_columns,
    # and create accessor methods for each column.
    def self.set_columns(new_columns) # :nodoc:
      @columns = new_columns
      def_column_accessor(*new_columns) if new_columns
      @str_columns = nil
      @columns
    end

    private_class_method :def_column_accessor, :get_db_schema, :set_columns
  end
end
