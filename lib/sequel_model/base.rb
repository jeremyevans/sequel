module Sequel
  class Model
    module ClassMethods
      # Which columns should be the only columns allowed in a call to set
      # (default: all columns).
      attr_reader :allowed_columns
  
      # All association reflections defined for this model (default: none).
      attr_reader :association_reflections
  
      # Hash of dataset methods to add to this class and subclasses when
      # set_dataset is called.
      attr_reader :dataset_methods
  
      # The default primary key for classes (default: :id)
      attr_reader :primary_key
  
      # Whether to raise an error instead of returning nil on a failure
      # to save/create/save_changes/etc.
      attr_accessor :raise_on_save_failure
  
      # Whether to raise an error when unable to typecast data for a column
      # (default: true)
      attr_accessor :raise_on_typecast_failure
  
      # Which columns should not be update in a call to set
      # (default: no columns).
      attr_reader :restricted_columns
  
      # Should be the literal primary key column name if this Model's table has a simple primary key, or
      # nil if the model has a compound primary key or no primary key.
      attr_reader :simple_pk
  
      # Should be the literal table name if this Model's dataset is a simple table (no select, order, join, etc.),
      # or nil otherwise.
      attr_reader :simple_table
  
      # Whether new/set/update and their variants should raise an error
      # if an invalid key is used (either that doesn't exist or that
      # access is restricted to it).
      attr_accessor :strict_param_setting
  
      # Whether to typecast the empty string ('') to nil for columns that
      # are not string or blob.
      attr_accessor :typecast_empty_string_to_nil
  
      # Whether to typecast attribute values on assignment (default: true)
      attr_accessor :typecast_on_assignment
  
      # Returns the first record from the database matching the conditions.
      # If a hash is given, it is used as the conditions.  If another
      # object is given, it finds the first record whose primary key(s) match
      # the given argument(s).  If caching is used, the cache is checked
      # first before a dataset lookup is attempted unless a hash is supplied.
      def [](*args)
        args = args.first if (args.size == 1)
        return dataset[args] if args.is_a?(Hash)
        return cache_lookup(args) if @cache_store
        if t = simple_table and p = simple_pk
          with_sql("SELECT * FROM #{t} WHERE #{p} = #{dataset.literal(args)} LIMIT 1").first
        else
          dataset[primary_key_hash(args)]
        end
      end
      
      # Returns the columns in the result set in their original order.
      # Generally, this will used the columns determined via the database
      # schema, but in certain cases (e.g. models that are based on a joined
      # dataset) it will use Dataset#columns to find the columns, which
      # may be empty if the Dataset has no records.
      def columns
        @columns || set_columns(dataset.naked.columns)
      end
    
      # Creates new instance with values set to passed-in Hash, saves it
      # (running any callbacks), and returns the instance if the object
      # was saved correctly.  If there was an error saving the object,
      # returns false.
      def create(values = {}, &block)
        obj = new(values, &block)
        return unless obj.save
        obj
      end
  
      # Returns the dataset associated with the Model class.
      def dataset
        @dataset || raise(Error, "No dataset associated with #{self}")
      end
    
      # Returns the database associated with the Model class.
      def db
        return @db if @db
        @db = self == Model ? DATABASES.first : superclass.db
        raise(Error, "No database associated with #{self}") unless @db
        @db
      end
      
      # Sets the database associated with the Model class.
      def db=(db)
        @db = db
        set_dataset(db.dataset(@dataset.opts)) if @dataset
      end
      
      # Returns the cached schema information if available or gets it
      # from the database.
      def db_schema
        @db_schema ||= get_db_schema
      end
  
      # If a block is given, define a method on the dataset with the given argument name using
      # the given block as well as a method on the model that calls the
      # dataset method.
      #
      # If a block is not given, define a method on the model for each argument
      # that calls the dataset method of the same argument name.
      def def_dataset_method(*args, &block)
        raise(Error, "No arguments given") if args.empty?
        if block_given?
          raise(Error, "Defining a dataset method using a block requires only one argument") if args.length > 1
          meth = args.first
          @dataset_methods[meth] = block
          dataset.meta_def(meth, &block)
        end
        args.each{|arg| instance_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__)}
      end
      
      
      # Modify and return eager loading dataset based on association options
      def eager_loading_dataset(opts, ds, select, associations)
        ds = ds.select(*select) if select
        ds = ds.filter(opts[:conditions]) if opts[:conditions]
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
      def find(*args, &block)
        filter(*args, &block).first
      end
      
      # Like find but invokes create with given conditions when record does not
      # exists.
      def find_or_create(cond)
        find(cond) || create(cond)
      end
    
      # If possible, set the dataset for the model subclass as soon as it
      # is created.  Also, inherit the INHERITED_INSTANCE_VARIABLES
      # from the parent class.
      def inherited(subclass)
        ivs = subclass.instance_variables.collect{|x| x.to_s}
        EMPTY_INSTANCE_VARIABLES.each{|iv| subclass.instance_variable_set(iv, nil) unless ivs.include?(iv.to_s)}
        INHERITED_INSTANCE_VARIABLES.each do |iv, dup|
          next if ivs.include?(iv.to_s)
          sup_class_value = instance_variable_get(iv)
          sup_class_value = sup_class_value.dup if dup == :dup && sup_class_value
          subclass.instance_variable_set(iv, sup_class_value)
        end
        unless ivs.include?("@dataset")
          db
          begin
            if self == Model
              subclass.set_dataset(subclass.implicit_table_name) unless subclass.name.blank?
            elsif ds = instance_variable_get(:@dataset)
              subclass.set_dataset(ds.clone, :inherited=>true)
            end
          rescue
            nil
          end
        end
      end
    
      # Returns the implicit table name for the model class.
      def implicit_table_name
        pluralize(underscore(demodulize(name))).to_sym
      end
  
      # Initializes a model instance as an existing record. This constructor is
      # used by Sequel to initialize model instances when fetching records.
      # #load requires that values be a hash where all keys are symbols. It
      # probably should not be used by external code.
      def load(values)
        new(values, true)
      end
  
      # Mark the model as not having a primary key. Not having a primary key
      # can cause issues, among which is that you won't be able to update records.
      def no_primary_key
        @simple_pk = @primary_key = nil
      end
  
      # Returns primary key attribute hash.  If using a composite primary key
      # value such be an array with values for each primary key in the correct
      # order.  For a standard primary key, value should be an object with a
      # compatible type for the key.  If the model does not have a primary key,
      # raises an Error.
      def primary_key_hash(value)
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
      def restrict_primary_key
        @restrict_primary_key = true
      end
  
      # Whether or not setting the primary key inside new/set/update is
      # restricted, true by default.
      def restrict_primary_key?
        @restrict_primary_key
      end
  
      # Serializes column with YAML or through marshalling.  Arguments should be
      # column symbols, with an optional trailing hash with a :format key
      # set to :yaml or :marshal (:yaml is the default).  Setting this adds
      # a transform to the model and dataset so that columns values will be serialized
      # when saved and deserialized when returned from the database.
      def serialize(*columns)
        format = extract_options!(columns)[:format] || :yaml
        @transform = columns.inject({}) do |m, c|
          m[c] = format
          m
        end
        @dataset.transform(@transform) if @dataset
      end
  
      # Whether or not the given column is serialized for this model.
      def serialized?(column)
        @transform ? @transform.include?(column) : false
      end
    
      # Set the columns to allow in new/set/update.  Using this means that
      # any columns not listed here will not be modified.  If you have any virtual
      # setter methods (methods that end in =) that you want to be used in
      # new/set/update, they need to be listed here as well (without the =).
      #
      # It may be better to use (set|update)_only instead of this in places where
      # only certain columns may be allowed.
      def set_allowed_columns(*cols)
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
      # determine the database schema based on the given/created dataset.
      def set_dataset(ds, opts={})
        inherited = opts[:inherited]
        @dataset = case ds
        when Symbol
          @simple_table = db.literal(ds)
          db[ds]
        when Dataset
          @simple_table = nil
          @db = ds.db
          ds
        else
          raise(Error, "Model.set_dataset takes a Symbol or a Sequel::Dataset")
        end
        @dataset.row_proc = Proc.new{|r| load(r)}
        @dataset.transform(@transform) if @transform
        if inherited
          @simple_table = superclass.simple_table
          @columns = @dataset.columns rescue nil
        else
          @dataset.extend(DatasetMethods)
          @dataset.extend(Associations::EagerLoading)
          @dataset_methods.each{|meth, block| @dataset.meta_def(meth, &block)} if @dataset_methods
        end
        @dataset.model = self
        @db_schema = (inherited ? superclass.db_schema : get_db_schema) rescue nil
        self
      end
      alias dataset= set_dataset
    
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
      def set_primary_key(*key)
        @simple_pk = key.length == 1 ? db.literal(key.first) : nil 
        @primary_key = (key.length == 1) ? key[0] : key.flatten
      end
  
      # Set the columns to restrict in new/set/update.  Using this means that
      # any columns listed here will not be modified.  If you have any virtual
      # setter methods (methods that end in =) that you want not to be used in
      # new/set/update, they need to be listed here as well (without the =).
      #
      # It may be better to use (set|update)_except instead of this in places where
      # only certain columns may be allowed.
      def set_restricted_columns(*cols)
        @restricted_columns = cols
      end
  
      # Defines a method that returns a filtered dataset.  Subsets
      # create dataset methods, so they can be chained for scoping.
      # For example:
      #
      #   Topic.subset(:popular){|o| o.num_posts > 100}
      #   Topic.subset(:recent){|o| o.created_on > Date.today - 7}
      #
      # Allows you to do:
      #
      #   Topic.filter(:username.like('%joe%')).popular.recent
      #
      # to get topics with a username that includes joe that
      # have more than 100 posts and were created less than
      # 7 days ago.
      def subset(name, *args, &block)
        def_dataset_method(name){filter(*args, &block)}
      end
      
      # Returns name of primary table for the dataset.
      def table_name
        dataset.opts[:from].first
      end
  
      # Allow the setting of the primary key(s) inside new/set/update.
      def unrestrict_primary_key
        @restrict_primary_key = false
      end
  
      private
      
      # Create the column accessors.  For columns that can be used as method names directly in ruby code,
      # use a string to define the method for speed.  For other columns names, use a block.
      def def_column_accessor(*columns)
        columns, bad_columns = columns.partition{|x| %r{\A[_A-Za-z][0-9A-Za-z_]*\z}io.match(x.to_s)}
        bad_columns.each{|x| def_bad_column_accessor(x)}
        im = instance_methods.collect{|x| x.to_s}
        columns.each do |column|
          meth = "#{column}="
          overridable_methods_module.module_eval("def #{column}; self[:#{column}] end") unless im.include?(column.to_s)
          overridable_methods_module.module_eval("def #{meth}(v); self[:#{column}] = v end") unless im.include?(meth)
        end
      end
  
      # Create a column accessor for a column with a method name that is hard to use in ruby code.
      def def_bad_column_accessor(column)
        overridable_methods_module.module_eval do
          define_method(column){self[column]}
          define_method("#{column}="){|v| self[column] = v}
        end
      end
  
      # Removes and returns the last member of the array if it is a hash. Otherwise,
      # an empty hash is returned This method is useful when writing methods that
      # take an options hash as the last parameter.
      def extract_options!(array)
        array.last.is_a?(Hash) ? array.pop : {}
      end
  
      # Get the schema from the database, fall back on checking the columns
      # via the database if that will return inaccurate results or if
      # it raises an error.
      def get_db_schema(reload = false)
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
            # Set the primary key(s) based on the schema information
            pks = schema_array.collect{|k,v| k if v[:primary_key]}.compact
            pks.length > 0 ? set_primary_key(*pks) : no_primary_key
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
  
      # Module that the class includes that holds methods the class adds for column accessors and
      # associations so that the methods can be overridden with super
      def overridable_methods_module
        include(@overridable_methods_module = Module.new) unless @overridable_methods_module
        @overridable_methods_module
      end
  
      # Set the columns for this model, reset the str_columns,
      # and create accessor methods for each column.
      def set_columns(new_columns)
        @columns = new_columns
        def_column_accessor(*new_columns) if new_columns
        @str_columns = nil
        @columns
      end

      # Add model methods that call dataset methods
      DATASET_METHODS.each{|arg| class_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__)}
  
      # Returns a copy of the model's dataset with custom SQL
      alias fetch with_sql
    end

    extend Enumerable
    extend Inflections
    extend Metaprogramming
    extend ClassMethods
    include Metaprogramming
  end
end
