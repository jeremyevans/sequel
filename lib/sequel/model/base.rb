module Sequel
  class Model
    extend Enumerable
    extend Inflections
    extend Metaprogramming
    include Metaprogramming

    # Class methods for Sequel::Model that implement basic model functionality.
    #
    # * All of the method names in Model::DATASET_METHODS have class methods created that call
    #   the Model's dataset with the method of the same name with the given arguments.
    module ClassMethods
      # Which columns should be the only columns allowed in a call to set
      # (default: not set, so all columns not otherwise restricted).
      attr_reader :allowed_columns
  
      # Array of modules that extend this model's dataset.  Stored
      # so that if the model's dataset is changed, it will be extended
      # with all of these modules.
      attr_reader :dataset_method_modules

      # Hash of dataset methods with method name keys and proc values that are
      # stored so when the dataset changes, methods defined with def_dataset_method
      # will be applied to the new dataset.
      attr_reader :dataset_methods
  
      # The primary key for the class.  Sequel can determine this automatically for
      # many databases, but not all, so you may need to set it manually.  If not
      # determined automatically, the default is :id.
      attr_reader :primary_key
  
      # Whether to raise an error instead of returning nil on a failure
      # to save/create/save_changes/etc due to a validation failure or
      # a before_* hook returning false.
      attr_accessor :raise_on_save_failure
  
      # Whether to raise an error when unable to typecast data for a column
      # (default: true).  This should be set to false if you want to use
      # validations to display nice error messages to the user (e.g. most
      # web applications).  You can use the validates_not_string validations
      # (from either the validation_helpers or validation_class_methods standard
      # plugins) in connection with option to check for typecast failures for
      # columns that aren't blobs or strings.
      attr_accessor :raise_on_typecast_failure
      
      # Whether to raise an error if an UPDATE or DELETE query related to
      # a model instance does not modify exactly 1 row.  If set to false,
      # Sequel will not check the number of rows modified (default: true).
      attr_accessor :require_modification
  
      # Which columns are specifically restricted in a call to set/update/new/etc.
      # (default: not set).  Some columns are restricted regardless of
      # this setting, such as the primary key column and columns in Model::RESTRICTED_SETTER_METHODS.
      attr_reader :restricted_columns
  
      # Should be the literal primary key column name if this Model's table has a simple primary key, or
      # nil if the model has a compound primary key or no primary key.
      attr_reader :simple_pk
  
      # Should be the literal table name if this Model's dataset is a simple table (no select, order, join, etc.),
      # or nil otherwise.  This and simple_pk are used for an optimization in Model.[].
      attr_reader :simple_table
  
      # Whether new/set/update and their variants should raise an error
      # if an invalid key is used.  A key is invalid if no setter method exists
      # for that key or the access to the setter method is restricted (e.g. due to it
      # being a primary key field).  If set to false, silently skip
      # any key where the setter method doesn't exist or access to it is restricted.
      attr_accessor :strict_param_setting
  
      # Whether to typecast the empty string ('') to nil for columns that
      # are not string or blob.  In most cases the empty string would be the
      # way to specify a NULL SQL value in string form (nil.to_s == ''),
      # and an empty string would not usually be typecast correctly for other
      # types, so the default is true.
      attr_accessor :typecast_empty_string_to_nil
  
      # Whether to typecast attribute values on assignment (default: true).
      # If set to false, no typecasting is done, so it will be left up to the
      # database to typecast the value correctly.
      attr_accessor :typecast_on_assignment
  
      # Whether to use a transaction by default when saving/deleting records (default: true).
      # If you are sending database queries in before_* or after_* hooks, you shouldn't change
      # the default setting without a good reason.
      attr_accessor :use_transactions
  
      # Returns the first record from the database matching the conditions.
      # If a hash is given, it is used as the conditions.  If another
      # object is given, it finds the first record whose primary key(s) match
      # the given argument(s).  
      def [](*args)
        args = args.first if (args.size == 1)
        args.is_a?(Hash) ? dataset[args] : primary_key_lookup(args)
      end
      
      # Returns the columns in the result set in their original order.
      # Generally, this will use the columns determined via the database
      # schema, but in certain cases (e.g. models that are based on a joined
      # dataset) it will use Dataset#columns to find the columns, which
      # may be empty if the Dataset has no records.
      def columns
        @columns || set_columns(dataset.naked.columns)
      end
    
      # Creates instance using new with the given values and block, and saves it.
      def create(values = {}, &block)
        new(values, &block).save
      end
  
      # Returns the dataset associated with the Model class.  Raises
      # an error if there is no associated dataset for this class.
      def dataset
        @dataset || raise(Error, "No dataset associated with #{self}")
      end
    
      # Returns the database associated with the Model class.
      # If this model doesn't have a database associated with it,
      # assumes the superclass's database, or the first object in
      # Sequel::DATABASES.  If no Sequel::Database object has
      # been created, raises an error.
      def db
        return @db if @db
        @db = self == Model ? DATABASES.first : superclass.db
        raise(Error, "No database associated with #{self}") unless @db
        @db
      end
      
      # Sets the database associated with the Model class. If the
      # model has an associated dataset, sets the model's dataset
      # to a dataset on the new database with the same options
      # used by the current dataset.
      def db=(db)
        @db = db
        set_dataset(db.dataset(@dataset.opts)) if @dataset
      end
      
      # Returns the cached schema information if available or gets it
      # from the database.
      def db_schema
        @db_schema ||= get_db_schema
      end
  
      # If a block is given, define a method on the dataset (if the model has an associated dataset)  with the given argument name using
      # the given block as well as a method on the model that calls the
      # dataset method.  Stores the method name and block so that it can be reapplied if the model's
      # dataset changes.
      #
      # If a block is not given, define a method on the model for each argument
      # that calls the dataset method of the same argument name.
      def def_dataset_method(*args, &block)
        raise(Error, "No arguments given") if args.empty?
        if block_given?
          raise(Error, "Defining a dataset method using a block requires only one argument") if args.length > 1
          meth = args.first
          @dataset_methods[meth] = block
          dataset.meta_def(meth, &block) if @dataset
        end
        args.each{|arg| instance_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__) unless respond_to?(arg)}
      end
      
      # Finds a single record according to the supplied filter, e.g.:
      #
      #   Ticket.find :author => 'Sharon' # => record
      #
      # You are encouraged to use Model.[] or Model.first instead of this method.
      def find(*args, &block)
        filter(*args, &block).first
      end
      
      # Like find but invokes create with given conditions when record does not
      # exist.
      def find_or_create(cond)
        find(cond) || create(cond)
      end
    
      # If possible, set the dataset for the model subclass as soon as it
      # is created.  Also, make sure the inherited class instance variables
      # are copied into the subclass.
      def inherited(subclass)
        super
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
            if self == Model || !@dataset
              subclass.set_dataset(subclass.implicit_table_name) unless subclass.name.empty?
            elsif @dataset
              subclass.set_dataset(@dataset.clone, :inherited=>true)
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
      # load requires that values be a hash where all keys are symbols. It
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
      # Returns self.
      #
      # This changes the row_proc of the given dataset to return
      # model objects, extends the dataset with the dataset_method_modules,
      # and defines methods on the dataset using the dataset_methods.
      # It also attempts to determine the database schema for the model,
      # based on the given dataset.
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
        if inherited
          @simple_table = superclass.simple_table
          @columns = @dataset.columns rescue nil
        else
          @dataset_method_modules.each{|m| @dataset.extend(m)} if @dataset_method_modules
          @dataset_methods.each{|meth, block| @dataset.meta_def(meth, &block)} if @dataset_methods
        end
        @dataset.model = self if @dataset.respond_to?(:model=)
        check_non_connection_error{@db_schema = (inherited ? superclass.db_schema : get_db_schema)}
        self
      end
      alias dataset= set_dataset
    
      # Sets the primary key for this model. You can use either a regular 
      # or a composite primary key.
      #
      # Example:
      #   class Tagging < Sequel::Model
      #     # composite key
      #     set_primary_key [:taggable_id, :tag_id]
      #   end
      #
      #   class Person < Sequel::Model
      #     # regular key
      #     set_primary_key :person_id
      #   end
      #
      # You can set it to nil to not have a primary key, but that
      # cause certain things not to work, see no_primary_key.
      def set_primary_key(*key)
        key = key.flatten
        @simple_pk = key.length == 1 ? db.literal(key.first) : nil 
        @primary_key = (key.length == 1) ? key[0] : key
      end
  
      # Set the columns to restrict in new/set/update.  Using this means that
      # attempts to call setter methods for the columns listed here will cause an
      # exception or be silently skipped (based on the strict_param_setting setting.
      # If you have any virtual # setter methods (methods that end in =) that you
      # want not to be used in new/set/update, they need to be listed here as well (without the =).
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
      #   Topic.subset(:joes, :username.like('%joe%'))
      #   Topic.subset(:popular){|o| o.num_posts > 100}
      #   Topic.subset(:recent){|o| o.created_on > Date.today - 7}
      #
      # Allows you to do:
      #
      #   Topic.joes.recent.popular
      #
      # to get topics with a username that includes joe that
      # have more than 100 posts and were created less than
      # 7 days ago.
      #
      # Both the args given and the block are passed to Dataset#filter.
      def subset(name, *args, &block)
        def_dataset_method(name){filter(*args, &block)}
      end
      
      # Returns name of primary table for the dataset.
      def table_name
        dataset.first_source_alias
      end
  
      # Allow the setting of the primary key(s) inside new/set/update.
      def unrestrict_primary_key
        @restrict_primary_key = false
      end
  
      private
      
      # Yield to the passed block and swallow all errors other than DatabaseConnectionErrors.
      def check_non_connection_error
        begin
          yield
        rescue Sequel::DatabaseConnectionError
          raise
        rescue
          nil
        end
      end

      # Create a column accessor for a column with a method name that is hard to use in ruby code.
      def def_bad_column_accessor(column)
        overridable_methods_module.module_eval do
          define_method(column){self[column]}
          define_method("#{column}="){|v| self[column] = v}
        end
      end
  
      # Create the column accessors.  For columns that can be used as method names directly in ruby code,
      # use a string to define the method for speed.  For other columns names, use a block.
      def def_column_accessor(*columns)
        columns, bad_columns = columns.partition{|x| NORMAL_METHOD_NAME_REGEXP.match(x.to_s)}
        bad_columns.each{|x| def_bad_column_accessor(x)}
        im = instance_methods.collect{|x| x.to_s}
        columns.each do |column|
          meth = "#{column}="
          overridable_methods_module.module_eval("def #{column}; self[:#{column}] end", __FILE__, __LINE__) unless im.include?(column.to_s)
          overridable_methods_module.module_eval("def #{meth}(v); self[:#{column}] = v end", __FILE__, __LINE__) unless im.include?(meth)
        end
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
        get_columns = proc{check_non_connection_error{columns} || []}
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
            # Set the primary key(s) based on the schema information,
            # if the schema information includes primary key information
            if schema_array.all?{|k,v| v.has_key?(:primary_key)}
              pks = schema_array.collect{|k,v| k if v[:primary_key]}.compact
              pks.length > 0 ? set_primary_key(*pks) : no_primary_key
            end
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
      
      # For the given opts hash and default name or :class option, add a
      # :class_name option unless already present which contains the name
      # of the class to use as a string.  The purpose is to allow late
      # binding to the class later using constantize.
      def late_binding_class_option(opts, default)
        case opts[:class]
          when String, Symbol
            # Delete :class to allow late binding
            opts[:class_name] ||= opts.delete(:class).to_s
          when Class
            opts[:class_name] ||= opts[:class].name
        end
        opts[:class_name] ||= ((name || '').split("::")[0..-2] + [camelize(default)]).join('::')
      end
  
      # Module that the class includes that holds methods the class adds for column accessors and
      # associations so that the methods can be overridden with super
      def overridable_methods_module
        include(@overridable_methods_module = Module.new) unless @overridable_methods_module
        @overridable_methods_module
      end
      
      # Find the row in the dataset that matches the primary key.  Uses
      # an static SQL optimization if the table and primary key are simple.
      def primary_key_lookup(pk)
        if t = simple_table and p = simple_pk
          with_sql("SELECT * FROM #{t} WHERE #{p} = #{dataset.literal(pk)}").first
        else
          dataset[primary_key_hash(pk)]
        end
      end
  
      # Set the columns for this model and create accessor methods for each column.
      def set_columns(new_columns)
        @columns = new_columns
        def_column_accessor(*new_columns) if new_columns
        @columns
      end

      # Add model methods that call dataset methods
      DATASET_METHODS.each{|arg| class_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__)}
  
      # Returns a copy of the model's dataset with custom SQL
      alias fetch with_sql
    end

    # Sequel::Model instance methods that implement basic model functionality.
    #
    # * All of the methods in HOOKS create instance methods that are called
    #   by Sequel when the appropriate action occurs.  For example, when destroying
    #   a model object, Sequel will call before_destroy, do the destroy,
    #   and then call after_destroy.
    # * The following instance_methods all call the class method of the same
    #   name: columns, dataset, db, primary_key, db_schema.
    # * The following instance methods allow boolean flags to be set on a per-object
    #   basis: raise_on_save_failure, raise_on_typecast_failure, require_modification, strict_param_setting, 
    #   typecast_empty_string_to_nil, typecast_on_assignment, use_transactions.
    #   If they are not used, the object will default to whatever the model setting is.
    module InstanceMethods
      HOOKS.each{|h| class_eval("def #{h}; end", __FILE__, __LINE__)}

      # Define instance method(s) that calls class method(s) of the
      # same name, caching the result in an instance variable.  Define
      # standard attr_writer method for modifying that instance variable
      def self.class_attr_overridable(*meths) # :nodoc:
        meths.each{|meth| class_eval("def #{meth}; !defined?(@#{meth}) ? (@#{meth} = self.class.#{meth}) : @#{meth} end", __FILE__, __LINE__)}
        attr_writer(*meths) 
      end 
    
      # Define instance method(s) that calls class method(s) of the
      # same name. Replaces the construct:
      #   
      #   define_method(meth){self.class.send(meth)}
      def self.class_attr_reader(*meths) # :nodoc:
        meths.each{|meth| class_eval("def #{meth}; model.#{meth} end", __FILE__, __LINE__)}
      end

      private_class_method :class_attr_overridable, :class_attr_reader

      class_attr_reader :columns, :db, :primary_key, :db_schema
      class_attr_overridable :raise_on_save_failure, :raise_on_typecast_failure, :require_modification, :strict_param_setting, :typecast_empty_string_to_nil, :typecast_on_assignment, :use_transactions

      # The hash of attribute values.  Keys are symbols with the names of the
      # underlying database columns.
      attr_reader :values

      # Creates new instance and passes the given values to set.
      # If a block is given, yield the instance to the block unless
      # from_db is true.
      # This method runs the after_initialize hook after
      # it has optionally yielded itself to the block.
      #
      # Arguments:
      # * values - should be a hash to pass to set. 
      # * from_db - should only be set by Model.load, forget it
      #   exists.
      def initialize(values = {}, from_db = false)
        if from_db
          @new = false
          set_values(values)
        else
          @values = {}
          @new = true
          @modified = true
          set(values)
          changed_columns.clear 
          yield self if block_given?
        end
        after_initialize
      end
      
      # Returns value of the column's attribute.
      def [](column)
        @values[column]
      end
  
      # Sets the value for the given column.  If typecasting is enabled for
      # this object, typecast the value based on the column's type.
      # If this a a new record or the typecasted value isn't the same
      # as the current value for the column, mark the column as changed.
      def []=(column, value)
        # If it is new, it doesn't have a value yet, so we should
        # definitely set the new value.
        # If the column isn't in @values, we can't assume it is
        # NULL in the database, so assume it has changed.
        v = typecast_value(column, value)
        if new? || !@values.include?(column) || v != @values[column]
          changed_columns << column unless changed_columns.include?(column)
          @values[column] = v
        end
      end
  
      # Compares model instances by values.
      def ==(obj)
        (obj.class == model) && (obj.values == @values)
      end
      alias eql? ==
  
      # If pk is not nil, true only if the objects have the same class and pk.
      # If pk is nil, false.
      def ===(obj)
        pk.nil? ? false : (obj.class == model) && (obj.pk == pk)
      end
  
      # class is defined in Object, but it is also a keyword,
      # and since a lot of instance methods call class methods,
      # this alias makes it so you can use model instead of
      # self.class.
      alias_method :model, :class

      # The autoincrementing primary key for this model object. Should be
      # overridden if you have a composite primary key with one part of it
      # being autoincrementing.
      def autoincrementing_primary_key
        primary_key
      end
  
      # The columns that have been updated.  This isn't completely accurate,
      # see Model#[]=.
      def changed_columns
        @changed_columns ||= []
      end
  
      # Deletes and returns self.  Does not run destroy hooks.
      # Look into using destroy instead.
      def delete
        _delete
        self
      end
      
      # Like delete but runs hooks before and after delete.
      # If before_destroy returns false, returns false without
      # deleting the object the the database. Otherwise, deletes
      # the item from the database and returns self.  Uses a transaction
      # if use_transactions is true or if the :transaction option is given and
      # true.
      def destroy(opts = {})
        checked_save_failure{checked_transaction(opts){_destroy(opts)}}
      end

      # Iterates through all of the current values using each.
      #
      # Example:
      #   Ticket.find(7).each { |k, v| puts "#{k} => #{v}" }
      def each(&block)
        @values.each(&block)
      end
  
      # Returns the validation errors associated with this object.
      def errors
        @errors ||= Errors.new
      end 

      # Returns true when current instance exists, false otherwise.
      # Generally an object that isn't new will exist unless it has
      # been deleted.
      def exists?
        this.count > 0
      end
      
      # Value that should be unique for objects with the same class and pk (if pk is not nil), or
      # the same class and values (if pk is nil).
      def hash
        [model, pk.nil? ? @values.sort_by{|k,v| k.to_s} : pk].hash
      end
  
      # Returns value for the :id attribute, even if the primary key is
      # not id. To get the primary key value, use #pk.
      def id
        @values[:id]
      end
  
      # Returns a string representation of the model instance including
      # the class name and values.
      def inspect
        "#<#{model.name} @values=#{inspect_values}>"
      end
  
      # Returns the keys in values.  May not include all column names.
      def keys
        @values.keys
      end
      
      # Refresh this record using for_update unless this is a new record.  Returns self.
      def lock!
        new? ? self : _refresh(this.for_update)
      end
      
      # Remove elements of the model object that make marshalling fail. Returns self.
      def marshallable!
        @this = nil
        self
      end

      # Explicitly mark the object as modified, so save_changes/update will
      # run callbacks even if no columns have changed.
      def modified!
        @modified = true
      end

      # Whether this object has been modified since last saved, used by
      # save_changes to determine whether changes should be saved.  New
      # values are always considered modified.
      def modified?
        @modified || !changed_columns.empty?
      end
  
      # Returns true if the current instance represents a new record.
      def new?
        @new
      end
      
      # Returns the primary key value identifying the model instance.
      # Raises an error if this model does not have a primary key.
      # If the model has a composite primary key, returns an array of values.
      def pk
        raise(Error, "No primary key is associated with this model") unless key = primary_key
        key.is_a?(Array) ? key.map{|k| @values[k]} : @values[key]
      end
      
      # Returns a hash identifying the model instance.  It should be true that:
      # 
      #  Model[model_instance.pk_hash] === model_instance
      def pk_hash
        model.primary_key_hash(pk)
      end
      
      # Reloads attributes from database and returns self. Also clears all
      # cached association and changed_columns information.  Raises an Error if the record no longer
      # exists in the database.
      def refresh
        _refresh(this)
      end

      # Alias of refresh, but not aliased directly to make overriding in a plugin easier.
      def reload
        refresh
      end
  
      # Creates or updates the record, after making sure the record
      # is valid.  If the record is not valid, or before_save,
      # before_create (if new?), or before_update (if !new?) return
      # false, returns nil unless raise_on_save_failure is true (if it
      # is true, it raises an error).
      # Otherwise, returns self. You can provide an optional list of
      # columns to update, in which case it only updates those columns.
      #
      # Takes the following options:
      #
      # * :changed - save all changed columns, instead of all columns or the columns given
      # * :transaction - set to false not to use a transaction
      # * :validate - set to false not to validate the model before saving
      def save(*columns)
        opts = columns.last.is_a?(Hash) ? columns.pop : {}
        if opts[:validate] != false and !valid?
          raise(ValidationFailed.new(errors)) if raise_on_save_failure
          return
        end
        checked_save_failure{checked_transaction(opts){_save(columns, opts)}}
      end

      # Saves only changed columns if the object has been modified.
      # If the object has not been modified, returns nil.  If unable to
      # save, returns false unless raise_on_save_failure is true.
      def save_changes(opts={})
        save(opts.merge(:changed=>true)) || false if modified? 
      end
  
      # Updates the instance with the supplied values with support for virtual
      # attributes, raising an exception if a value is used that doesn't have
      # a setter method (or ignoring it if strict_param_setting = false).
      # Does not save the record.
      def set(hash)
        set_restricted(hash, nil, nil)
      end
  
      # Set all values using the entries in the hash, ignoring any setting of
      # allowed_columns or restricted columns in the model.
      def set_all(hash)
        set_restricted(hash, false, false)
      end
  
      # Set all values using the entries in the hash, except for the keys
      # given in except.
      def set_except(hash, *except)
        set_restricted(hash, false, except.flatten)
      end
  
      # Set the values using the entries in the hash, only if the key
      # is included in only.
      def set_only(hash, *only)
        set_restricted(hash, only.flatten, false)
      end
  
      # Returns (naked) dataset that should return only this instance.
      def this
        @this ||= model.dataset.filter(pk_hash).limit(1).naked
      end
      
      # Runs set with the passed hash and then runs save_changes.
      def update(hash)
        update_restricted(hash, nil, nil)
      end
  
      # Update all values using the entries in the hash, ignoring any setting of
      # allowed_columns or restricted columns in the model.
      def update_all(hash)
        update_restricted(hash, false, false)
      end
  
      # Update all values using the entries in the hash, except for the keys
      # given in except.
      def update_except(hash, *except)
        update_restricted(hash, false, except.flatten)
      end
  
      # Update the values using the entries in the hash, only if the key
      # is included in only.
      def update_only(hash, *only)
        update_restricted(hash, only.flatten, false)
      end
      
      # Validates the object.  If the object is invalid, errors should be added
      # to the errors attribute.  By default, does nothing, as all models
      # are valid by default.
      def validate
      end

      # Validates the object and returns true if no errors are reported.
      def valid?
        errors.clear
        if before_validation == false
          save_failure(:validation) if raise_on_save_failure
          return false
        end
        validate
        after_validation
        errors.empty?
      end

      private
      
      # Actually do the deletion of the object's dataset.
      def _delete
        n = _delete_dataset.delete
        raise(NoExistingObject, "Attempt to delete object did not result in a single row modification (Rows Deleted: #{n}, SQL: #{_delete_dataset.delete_sql})") if require_modification && n != 1
        n
      end
      
      # The dataset to use when deleting the object.   The same as the object's
      # dataset by default.
      def _delete_dataset
        this
      end
  
      # Internal destroy method, separted from destroy to
      # allow running inside a transaction
      def _destroy(opts)
        return save_failure(:destroy) if before_destroy == false
        _destroy_delete
        after_destroy
        self
      end
      
      # Internal delete method to call when destroying an object,
      # separated from delete to allow you to override destroy's version
      # without affecting delete.
      def _destroy_delete
        delete
      end

      def _insert
        ds = model.dataset
        if ds.respond_to?(:insert_select) and h = ds.insert_select(@values)
          @values = h
          nil
        else
          iid = ds.insert(@values)
          # if we have a regular primary key and it's not set in @values,
          # we assume it's the last inserted id
          if (pk = autoincrementing_primary_key) && pk.is_a?(Symbol) && !@values[pk]
            @values[pk] = iid
          end
          pk
        end
      end
      
      # Refresh using a particular dataset, used inside save to make sure the same server
      # is used for reading newly inserted values from the database
      def _refresh(dataset)
        set_values(dataset.first || raise(Error, "Record not found"))
        changed_columns.clear
        self
      end
      
      # Internal version of save, split from save to allow running inside
      # it's own transaction.
      def _save(columns, opts)
        return save_failure(:save) if before_save == false
        if new?
          return save_failure(:create) if before_create == false
          pk = _insert
          @this = nil if pk
          @new = false
          @was_new = true
          after_create
          after_save
          @was_new = nil
          if pk
            ds = this
            ds = ds.server(:default) unless ds.opts[:server]
            _refresh(ds)
          else
            changed_columns.clear
          end
        else
          return save_failure(:update) if before_update == false
          if columns.empty?
            @columns_updated = opts[:changed] ? @values.reject{|k,v| !changed_columns.include?(k)} : @values.dup
            changed_columns.clear
          else # update only the specified columns
            @columns_updated = @values.reject{|k, v| !columns.include?(k)}
            changed_columns.reject!{|c| columns.include?(c)}
          end
          Array(primary_key).each{|x| @columns_updated.delete(x)}
          _update(@columns_updated) unless @columns_updated.empty?
          after_update
          after_save
          @columns_updated = nil
        end
        @modified = false
        self
      end
      
      # Update this instance's dataset with the supplied column hash.
      def _update(columns)
        n = _update_dataset.update(columns)
        raise(NoExistingObject, "Attempt to update object did not result in a single row modification (SQL: #{_update_dataset.update_sql(columns)})") if require_modification && n != 1
        n
      end
      
      # The dataset to use when updating an object.  The same as the object's
      # dataset by default.
      def _update_dataset
        this
      end

      # If raise_on_save_failure is false, check for BeforeHookFailed
      # beind raised by yielding and swallow it.
      def checked_save_failure
        if raise_on_save_failure
          yield
        else
          begin
            yield
          rescue BeforeHookFailed 
            nil
          end
        end
      end
      
      # If transactions should be used, wrap the yield in a transaction block.
      def checked_transaction(opts)
        use_transaction?(opts) ? db.transaction(opts){yield} : yield
      end

      # Default inspection output for the values hash, overwrite to change what #inspect displays.
      def inspect_values
        @values.inspect
      end
  
      # Raise an error if raise_on_save_failure is true, return nil otherwise.
      def save_failure(type)
        raise BeforeHookFailed, "one of the before_#{type} hooks returned false"
      end
  
      # Set the columns, filtered by the only and except arrays.
      def set_restricted(hash, only, except)
        meths = setter_methods(only, except)
        strict = strict_param_setting
        hash.each do |k,v|
          m = "#{k}="
          if meths.include?(m)
            send(m, v)
          elsif strict
            raise Error, "method #{m} doesn't exist or access is restricted to it"
          end
        end
        self
      end
      
      # Replace the current values with hash.
      def set_values(hash)
        @values = hash
      end
      
      # Returns all methods that can be used for attribute
      # assignment (those that end with =), modified by the only
      # and except arguments:
      #
      # * only
      #   * false - Don't modify the results
      #   * nil - if the model has allowed_columns, use only these, otherwise, don't modify
      #   * Array - allow only the given methods to be used
      # * except
      #   * false - Don't modify the results
      #   * nil - if the model has restricted_columns, remove these, otherwise, don't modify
      #   * Array - remove the given methods
      #
      # only takes precedence over except, and if only is not used, certain methods are always
      # restricted (RESTRICTED_SETTER_METHODS).  The primary key is restricted by default as
      # well, see Model.unrestrict_primary_key to change this.
      def setter_methods(only, except)
        only = only.nil? ? model.allowed_columns : only
        except = except.nil? ? model.restricted_columns : except
        if only
          only.map{|x| "#{x}="}
        else
          meths = methods.collect{|x| x.to_s}.grep(SETTER_METHOD_REGEXP) - RESTRICTED_SETTER_METHODS
          meths -= Array(primary_key).map{|x| "#{x}="} if primary_key && model.restrict_primary_key?
          meths -= except.map{|x| "#{x}="} if except
          meths
        end
      end
  
      # Typecast the value to the column's type if typecasting.  Calls the database's
      # typecast_value method, so database adapters can override/augment the handling
      # for database specific column types.
      def typecast_value(column, value)
        return value unless typecast_on_assignment && db_schema && (col_schema = db_schema[column])
        value = nil if value == '' and typecast_empty_string_to_nil and col_schema[:type] and ![:string, :blob].include?(col_schema[:type])
        raise(InvalidValue, "nil/NULL is not allowed for the #{column} column") if raise_on_typecast_failure && value.nil? && (col_schema[:allow_null] == false)
        begin
          model.db.typecast_value(col_schema[:type], value)
        rescue InvalidValue
          raise_on_typecast_failure ? raise : value
        end
      end
  
      # Set the columns, filtered by the only and except arrays.
      def update_restricted(hash, only, except)
        set_restricted(hash, only, except)
        save_changes
      end
      
      # Whether to use a transaction for this action.  If the :transaction
      # option is present in the hash, use that, otherwise, fallback to the
      # object's default (if set), or class's default (if not).
      def use_transaction?(opts = {})
        opts.fetch(:transaction, use_transactions)
      end
    end

    # Dataset methods are methods that the model class extends its dataset with in
    # the call to set_dataset.
    module DatasetMethods
      # The model class associated with this dataset
      attr_accessor :model

      # Destroy each row in the dataset by instantiating it and then calling
      # destroy on the resulting model object.  This isn't as fast as deleting
      # the dataset, which does a single SQL call, but this runs any destroy
      # hooks on each object in the dataset.
      def destroy
        @db.transaction{all{|r| r.destroy}.length}
      end

      # This allows you to call to_hash without any arguments, which will
      # result in a hash with the primary key value being the key and the
      # model object being the value.
      def to_hash(key_column=nil, value_column=nil)
        if key_column
          super
        else
          raise(Sequel::Error, "No primary key for model") unless model and pk = model.primary_key
          super(pk, value_column) 
        end
      end
    end

    plugin self
  end
end
