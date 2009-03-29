module Sequel
  class Model
    extend Enumerable
    extend Inflections
    extend Metaprogramming
    include Metaprogramming

    module ClassMethods
      # Which columns should be the only columns allowed in a call to set
      # (default: all columns).
      attr_reader :allowed_columns
  
      # Array of modules that extend this model's dataset.
      attr_reader :dataset_method_modules

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
  
      # Whether to use a transaction by default when saving/deleting records
      attr_accessor :use_transactions
  
      # Returns the first record from the database matching the conditions.
      # If a hash is given, it is used as the conditions.  If another
      # object is given, it finds the first record whose primary key(s) match
      # the given argument(s).  
      def [](*args)
        args = args.first if (args.size == 1)
        return dataset[args] if args.is_a?(Hash)
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
        args.each{|arg| instance_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__) unless method_defined?(arg)}
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
              subclass.set_dataset(subclass.implicit_table_name) unless subclass.name.empty?
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
          @dataset_method_modules.each{|m| @dataset.extend(m)} if @dataset_method_modules
          @dataset_methods.each{|meth, block| @dataset.meta_def(meth, &block)} if @dataset_methods
        end
        @dataset.model = self if @dataset.respond_to?(:model=)
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
        columns, bad_columns = columns.partition{|x| DATASET_METHOD_RE.match(x.to_s)}
        bad_columns.each{|x| def_bad_column_accessor(x)}
        im = instance_methods.collect{|x| x.to_s}
        columns.each do |column|
          meth = "#{column}="
          overridable_methods_module.module_eval("def #{column}; self[:#{column}] end") unless im.include?(column.to_s)
          overridable_methods_module.module_eval("def #{meth}(v); self[:#{column}] = v end") unless im.include?(meth)
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

    module InstanceMethods
      HOOKS.each{|h| class_eval("def #{h}; end", __FILE__, __LINE__)}

      # Define instance method(s) that calls class method(s) of the
      # same name, caching the result in an instance variable.  Define
      # standard attr_writer method for modifying that instance variable
      def self.class_attr_overridable(*meths) # :nodoc:
        meths.each{|meth| class_eval("def #{meth}; !defined?(@#{meth}) ? (@#{meth} = self.class.#{meth}) : @#{meth} end")}
        attr_writer(*meths) 
      end 
    
      # Define instance method(s) that calls class method(s) of the
      # same name. Replaces the construct:
      #   
      #   define_method(meth){self.class.send(meth)}
      def self.class_attr_reader(*meths) # :nodoc:
        meths.each{|meth| class_eval("def #{meth}; model.#{meth} end")}
      end

      private_class_method :class_attr_overridable, :class_attr_reader

      class_attr_reader :columns, :db, :primary_key, :db_schema
      class_attr_overridable :raise_on_save_failure, :raise_on_typecast_failure, :strict_param_setting, :typecast_empty_string_to_nil, :typecast_on_assignment, :use_transactions
      
      # The hash of attribute values.  Keys are symbols with the names of the
      # underlying database columns.
      attr_reader :values

      # Creates new instance with values set to passed-in Hash.
      # If a block is given, yield the instance to the block unless
      # from_db is true.
      # This method runs the after_initialize hook after
      # it has optionally yielded itself to the block.
      #
      # Arguments:
      # * values - should be a hash with symbol keys, though
      #   string keys will work if from_db is false.
      # * from_db - should only be set by Model.load, forget it
      #   exists.
      def initialize(values = {}, from_db = false)
        if from_db
          @new = false
          @values = values
        else
          @values = {}
          @new = true
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
  
      # Sets value of the column's attribute and marks the column as changed.
      # If the column already has the same value, this is a no-op.
      def []=(column, value)
        # If it is new, it doesn't have a value yet, so we should
        # definitely set the new value.
        # If the column isn't in @values, we can't assume it is
        # NULL in the database, so assume it has changed.
        if new? || !@values.include?(column) || value != @values[column]
          changed_columns << column unless changed_columns.include?(column)
          @values[column] = typecast_value(column, value)
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
      # the model makes it so you can use model instead of
      # self.class.
      alias_method :model, :class
  
      # The current cached associations.  A hash with the keys being the
      # association name symbols and the values being the associated object
      # or nil (many_to_one), or the array of associated objects (*_to_many).
      def associations
        @associations ||= {}
      end
  
      # The columns that have been updated.  This isn't completely accurate,
      # see Model#[]=.
      def changed_columns
        @changed_columns ||= []
      end
  
      # Deletes and returns self.  Does not run destroy hooks.
      # Look into using destroy instead.
      def delete
        this.delete
        self
      end
      
      # Like delete but runs hooks before and after delete.
      # If before_destroy returns false, returns false without
      # deleting the object the the database. Otherwise, deletes
      # the item from the database and returns self.
      def destroy
        use_transactions ? db.transaction{_destroy} : _destroy
      end

      # Enumerates through all attributes.
      #
      # Example:
      #   Ticket.find(7).each { |k, v| puts "#{k} => #{v}" }
      def each(&block)
        @values.each(&block)
      end
  
      # Returns the validation errors associated with the object.
      def errors
        @errors ||= Errors.new
      end 

      # Returns true when current instance exists, false otherwise.
      def exists?
        this.count > 0
      end
      
      # Unique for objects with the same class and pk (if pk is not nil), or
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
  
      # Returns attribute names as an array of symbols.
      def keys
        @values.keys
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
        case key
        when Array
          key.collect{|k| @values[k]}
        else
          @values[key]
        end
      end
      
      # Returns a hash identifying the model instance.  It should be true that:
      # 
      #  Model[model_instance.pk_hash] === model_instance
      def pk_hash
        model.primary_key_hash(pk)
      end
      
      # Reloads attributes from database and returns self. Also clears all
      # cached association information.  Raises an Error if the record no longer
      # exists in the database.
      def refresh
        @values = this.first || raise(Error, "Record not found")
        changed_columns.clear
        associations.clear
        self
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
      # * :changed - save all changed columns, instead of all columns or the columns
      # * :transaction - set to false not to use a transaction
      # * :validate - set to false not to validate the model before saving
      def save(*columns)
        opts = columns.last.is_a?(Hash) ? columns.pop : {}
        return save_failure(:invalid) if opts[:validate] != false and !valid?
        use_transaction = if opts.include?(:transaction)
          opts[:transaction]
        else
          use_transactions
        end
        use_transaction ? db.transaction(opts){_save(columns, opts)} : _save(columns, opts)
      end

      # Saves only changed columns or does nothing if no columns are marked as 
      # chanaged.  If no columns have been changed, returns nil.  If unable to
      # save, returns false unless raise_on_save_failure is true.
      def save_changes
        save(:changed=>true) || false unless changed_columns.empty?
      end
  
      # Updates the instance with the supplied values with support for virtual
      # attributes, raising an exception if a value is used that doesn't have
      # a setter method (or ignoring it if strict_param_setting = false).
      # Does not save the record.
      #
      # If no columns have been set for this model (very unlikely), assume symbol
      # keys are valid column names, and assign the column value based on that.
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
      
      # Runs set with the passed hash and runs save_changes (which runs any callback methods).
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
          save_failure(:validation)
          return false
        end
        validate
        after_validation
        errors.empty?
      end

      private
  
      # Internal destroy method, separted from destroy to
      # allow running inside a transaction
      def _destroy
        return save_failure(:destroy) if before_destroy == false
        delete
        after_destroy
        self
      end
      
      # Internal version of save, split from save to allow running inside
      # it's own transaction.
      def _save(columns, opts)
        return save_failure(:save) if before_save == false
        if new?
          return save_failure(:create) if before_create == false
          ds = model.dataset
          if ds.respond_to?(:insert_select) and h = ds.insert_select(@values)
            @values = h
            @this = nil
          else
            iid = ds.insert(@values)
            # if we have a regular primary key and it's not set in @values,
            # we assume it's the last inserted id
            if (pk = primary_key) && !(Array === pk) && !@values[pk]
              @values[pk] = iid
            end
            @this = nil if pk
          end
          @new = false
          @was_new = true
          after_create
          after_save
          @was_new = nil
          refresh if pk
        else
          return save_failure(:update) if before_update == false
          if columns.empty?
            @columns_updated = opts[:changed] ? @values.reject{|k,v| !changed_columns.include?(k)} : @values
            changed_columns.clear
          else # update only the specified columns
            @columns_updated = @values.reject{|k, v| !columns.include?(k)}
            changed_columns.reject!{|c| columns.include?(c)}
          end
          this.update(@columns_updated)
          after_update
          after_save
          @columns_updated = nil
        end
        self
      end
      
      # Default inspection output for a record, overwrite to change the way #inspect prints the @values hash
      def inspect_values
        @values.inspect
      end
  
      # Raise an error if raise_on_save_failure is true
      def save_failure(type)
        if raise_on_save_failure
          if type == :invalid
            raise ValidationFailed, errors.full_messages.join(', ')
          else
            raise BeforeHookFailed, "one of the before_#{type} hooks returned false"
          end
        end
      end
  
      # Set the columns, filtered by the only and except arrays.
      def set_restricted(hash, only, except)
        columns_not_set = [nil, false, "", [], {}].include?(model.instance_variable_get(:@columns))
        meths = setter_methods(only, except)
        strict = strict_param_setting
        hash.each do |k,v|
          m = "#{k}="
          if meths.include?(m)
            send(m, v)
          elsif columns_not_set && (Symbol === k)
            Deprecation.deprecate('Calling Model#set_restricted for a column without a setter method when the model class does not have any columns', 'Use Model#[] for these columns')
            self[k] = v
          elsif strict
            raise Error, "method #{m} doesn't exist or access is restricted to it"
          end
        end
        self
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
          meths = methods.collect{|x| x.to_s}.grep(/=\z/) - RESTRICTED_SETTER_METHODS
          meths -= Array(primary_key).map{|x| "#{x}="} if primary_key && model.restrict_primary_key?
          meths -= except.map{|x| "#{x}="} if except
          meths
        end
      end
  
      # Typecast the value to the column's type if typecasting.  Calls the database's
      # typecast_value method, so database adapters can override/augment the handling
      # for database specific column types.
      def typecast_value(column, value)
        # Deprecation.deprecate : Remove model.serialized call
        return value unless typecast_on_assignment && db_schema && (col_schema = db_schema[column]) && !model.serialized?(column)
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
    end

    # Dataset methods are methods that the model class extends its dataset with in
    # the call to set_dataset.
    module DatasetMethods
      # The model class associated with this dataset
      attr_accessor :model

      # Destroy each row in the dataset by instantiating it and then calling
      # destroy on the resulting model object.  This isn't as fast as deleting
      # the object, which does a single SQL call, but this runs any destroy
      # hooks.
      def destroy
        count = 0
        @db.transaction{all{|r| count += 1; r.destroy}}
        count
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
