# frozen-string-literal: true

module Sequel
  class Model
    extend Enumerable
    extend Inflections

    # Class methods for Sequel::Model that implement basic model functionality.
    #
    # * All of the method names in Model::DATASET_METHODS have class methods created that call
    #   the Model's dataset with the method of the same name with the given arguments.
    module ClassMethods
      # Which columns should be the only columns allowed in a call to a mass assignment method (e.g. set)
      # (default: not set, so all columns not otherwise restricted are allowed).
      attr_reader :allowed_columns

      # Array of modules that extend this model's dataset.  Stored
      # so that if the model's dataset is changed, it will be extended
      # with all of these modules.
      attr_reader :dataset_method_modules

      # The default options to use for Model#set_fields.  These are merged with
      # the options given to set_fields.
      attr_accessor :default_set_fields_options

      # SQL string fragment used for faster DELETE statement creation when deleting/destroying
      # model instances, or nil if the optimization should not be used. For internal use only.
      attr_reader :fast_instance_delete_sql

      # The dataset that instance datasets (#this) are based on.  Generally a naked version of
      # the model's dataset limited to one row.  For internal use only.
      attr_reader :instance_dataset

      # Array of plugin modules loaded by this class
      #
      #   Sequel::Model.plugins
      #   # => [Sequel::Model, Sequel::Model::Associations]
      attr_reader :plugins
  
      # The primary key for the class.  Sequel can determine this automatically for
      # many databases, but not all, so you may need to set it manually.  If not
      # determined automatically, the default is :id.
      attr_reader :primary_key
  
      # Whether to raise an error instead of returning nil on a failure
      # to save/create/save_changes/update/destroy due to a validation failure or
      # a before_* hook returning false (default: true). 
      attr_accessor :raise_on_save_failure
  
      # Whether to raise an error when unable to typecast data for a column
      # (default: true).  This should be set to false if you want to use
      # validations to display nice error messages to the user (e.g. most
      # web applications).  You can use the validates_schema_types validation
      # (from the validation_helpers plugin) in connection with this setting to
      # check for typecast failures during validation.
      attr_accessor :raise_on_typecast_failure
      
      # Whether to raise an error if an UPDATE or DELETE query related to
      # a model instance does not modify exactly 1 row.  If set to false,
      # Sequel will not check the number of rows modified (default: true).
      attr_accessor :require_modification
  
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
  
      # Whether to enable the after_commit and after_rollback hooks when saving/destroying
      # instances.  On by default, can be turned off for performance reasons or when using
      # prepared transactions (which aren't compatible with after commit/rollback).
      attr_accessor :use_after_commit_rollback
  
      # Whether to use a transaction by default when saving/deleting records (default: true).
      # If you are sending database queries in before_* or after_* hooks, you shouldn't change
      # the default setting without a good reason.
      attr_accessor :use_transactions
  
      # Returns the first record from the database matching the conditions.
      # If a hash is given, it is used as the conditions.  If another
      # object is given, it finds the first record whose primary key(s) match
      # the given argument(s).  If no object is returned by the dataset, returns nil.
      # 
      #   Artist[1] # SELECT * FROM artists WHERE id = 1
      #   # => #<Artist {:id=>1, ...}>
      #
      #   Artist[:name=>'Bob'] # SELECT * FROM artists WHERE (name = 'Bob') LIMIT 1
      #   # => #<Artist {:name=>'Bob', ...}>
      def [](*args)
        args = args.first if args.size <= 1
        args.is_a?(Hash) ? first_where(args) : (primary_key_lookup(args) unless args.nil?)
      end

      # Initializes a model instance as an existing record. This constructor is
      # used by Sequel to initialize model instances when fetching records.
      # Requires that values be a hash where all keys are symbols. It
      # probably should not be used by external code.
      def call(values)
        o = allocate
        o.instance_variable_set(:@values, values)
        o
      end
      
      # Clear the setter_methods cache
      def clear_setter_methods_cache
        @setter_methods = nil
      end
  
      # Returns the columns in the result set in their original order.
      # Generally, this will use the columns determined via the database
      # schema, but in certain cases (e.g. models that are based on a joined
      # dataset) it will use <tt>Dataset#columns</tt> to find the columns.
      #
      #   Artist.columns
      #   # => [:id, :name]
      def columns
        @columns || set_columns(dataset.naked.columns)
      end
    
      # Creates instance using new with the given values and block, and saves it.
      # 
      #   Artist.create(:name=>'Bob')
      #   # INSERT INTO artists (name) VALUES ('Bob')
      #
      #   Artist.create do |a|
      #     a.name = 'Jim'
      #   end # INSERT INTO artists (name) VALUES ('Jim')
      def create(values = {}, &block)
        new(values, &block).save
      end
  
      # Returns the dataset associated with the Model class.  Raises
      # an +Error+ if there is no associated dataset for this class.
      # In most cases, you don't need to call this directly, as Model
      # proxies many dataset methods to the underlying dataset.
      #
      #   Artist.dataset.all # SELECT * FROM artists
      def dataset
        @dataset || raise(Error, "No dataset associated with #{self}")
      end

      # Alias of set_dataset
      def dataset=(ds)
        set_dataset(ds)
      end

      # Extend the dataset with a module, similar to adding
      # a plugin with the methods defined in DatasetMethods.
      # This is the recommended way to add methods to model datasets.
      #
      # If an argument, it should be a module, and is used to extend
      # the underlying dataset.  Otherwise an anonymous module is created, and
      # if a block is given, it is module_evaled, allowing you do define
      # dataset methods directly using the standard ruby def syntax.
      # Returns the module given or the anonymous module created.
      #
      #   # Usage with existing module
      #   Artist.dataset_module Sequel::ColumnsIntrospection
      #
      #   # Usage with anonymous module
      #   Artist.dataset_module do
      #     def foo
      #       :bar
      #     end
      #   end
      #   Artist.dataset.foo
      #   # => :bar
      #   Artist.foo
      #   # => :bar
      #
      # Any anonymous modules created are actually instances of Sequel::Model::DatasetModule
      # (a Module subclass), which allows you to call the subset method on them:
      #
      #   Artist.dataset_module do
      #     subset :released, Sequel.identifier(release_date) > Sequel::CURRENT_DATE
      #   end
      #
      # Any public methods in the dataset module will have class methods created that
      # call the method on the dataset, assuming that the class method is not already
      # defined.
      def dataset_module(mod = nil)
        if mod
          raise Error, "can't provide both argument and block to Model.dataset_module" if block_given?
          dataset_extend(mod)
          mod
        else
          @dataset_module ||= DatasetModule.new(self)
          @dataset_module.module_eval(&Proc.new) if block_given?
          dataset_extend(@dataset_module)
          @dataset_module
        end
      end
    
      # Returns the database associated with the Model class.
      # If this model doesn't have a database associated with it,
      # assumes the superclass's database, or the first object in
      # Sequel::DATABASES.  If no Sequel::Database object has
      # been created, raises an error.
      #
      #   Artist.db.transaction do # BEGIN
      #     Artist.create(:name=>'Bob')
      #     # INSERT INTO artists (name) VALUES ('Bob')
      #   end # COMMIT
      def db
        return @db if @db
        @db = self == Model ? Sequel.synchronize{DATABASES.first} : superclass.db
        raise(Error, "No database associated with #{self}: have you called Sequel.connect or #{self}.db= ?") unless @db
        @db
      end
      
      # Sets the database associated with the Model class. If the
      # model has an associated dataset, sets the model's dataset
      # to a dataset on the new database with the same options
      # used by the current dataset.  This can be used directly on
      # Sequel::Model to set the default database to be used
      # by subclasses, or to override the database used for specific
      # models:
      #
      #   Sequel::Model.db = DB1
      #   Artist.db = DB2
      #
      # Note that you should not use this to change the model's database
      # at runtime.  If you have that need, you should look into Sequel's
      # sharding support.
      def db=(db)
        @db = db
        set_dataset(db.dataset.clone(@dataset.opts)) if @dataset
      end
      
      # Returns the cached schema information if available or gets it
      # from the database.  This is a hash where keys are column symbols
      # and values are hashes of information related to the column.  See
      # <tt>Database#schema</tt>.
      #
      #   Artist.db_schema
      #   # {:id=>{:type=>:integer, :primary_key=>true, ...},
      #   #  :name=>{:type=>:string, :primary_key=>false, ...}} 
      def db_schema
        @db_schema ||= get_db_schema
      end
  
      # Create a column alias, where the column methods have one name, but the underlying storage uses a
      # different name.
      def def_column_alias(meth, column)
        clear_setter_methods_cache
        overridable_methods_module.module_eval do
          define_method(meth){self[column]}
          define_method("#{meth}="){|v| self[column] = v}
        end
      end
  
      # If a block is given, define a method on the dataset (if the model currently has an dataset)  with the given argument name using
      # the given block.  Also define a class method on the model that calls the
      # dataset method.  Stores the method name and block so that it can be reapplied if the model's
      # dataset changes.
      #
      # If a block is not given, just define a class method on the model for each argument
      # that calls the dataset method of the same argument name.
      #
      # It is recommended that you define methods inside a block passed to #dataset_module
      # instead of using this method, as #dataset_module allows you to use normal
      # ruby def syntax.
      #
      #   # Add new dataset method and class method that calls it
      #   Artist.def_dataset_method(:by_name){order(:name)}
      #   Artist.filter(:name.like('A%')).by_name
      #   Artist.by_name.filter(:name.like('A%'))
      #
      #   # Just add a class method that calls an existing dataset method
      #   Artist.def_dataset_method(:server!)
      #   Artist.server!(:server1)
      def def_dataset_method(*args, &block)
        raise(Error, "No arguments given") if args.empty?

        if block
          raise(Error, "Defining a dataset method using a block requires only one argument") if args.length > 1
          dataset_module{define_method(args.first, &block)}
        else
          args.each{|arg| def_model_dataset_method(arg)}
        end
      end

      # Finds a single record according to the supplied filter.
      # You are encouraged to use Model.[] or Model.first instead of this method.
      #
      #   Artist.find(:name=>'Bob')
      #   # SELECT * FROM artists WHERE (name = 'Bob') LIMIT 1
      #
      #   Artist.find{name > 'M'}
      #   # SELECT * FROM artists WHERE (name > 'M') LIMIT 1
      def find(*args, &block)
        if args.length == 1 && !block
          # Use optimized finder
          first_where(args.first)
        else
          filter(*args, &block).first
        end
      end
      
      # Like +find+ but invokes create with given conditions when record does not
      # exist.  Unlike +find+ in that the block used in this method is not passed
      # to +find+, but instead is passed to +create+ only if +find+ does not
      # return an object.
      #
      #   Artist.find_or_create(:name=>'Bob')
      #   # SELECT * FROM artists WHERE (name = 'Bob') LIMIT 1
      #   # INSERT INTO artists (name) VALUES ('Bob')
      #
      #   Artist.find_or_create(:name=>'Jim'){|a| a.hometown = 'Sactown'}
      #   # SELECT * FROM artists WHERE (name = 'Jim') LIMIT 1
      #   # INSERT INTO artists (name, hometown) VALUES ('Jim', 'Sactown')
      def find_or_create(cond, &block)
        find(cond) || create(cond, &block)
      end
    

      FINDER_TYPES = [:first, :all, :each, :get].freeze

      # Create an optimized finder method using a dataset placeholder literalizer.
      # This pre-computes the SQL to use for the query, except for given arguments.
      #
      # There are two ways to use this.  The recommended way is to pass a symbol
      # that represents a model class method that returns a dataset:
      #
      #   def Artist.by_name(name)
      #     where(:name=>name)
      #   end
      #
      #   Artist.finder :by_name
      #
      # This creates an optimized first_by_name method, which you can call normally:
      #
      #   Artist.first_by_name("Joe")
      #
      # The alternative way to use this to pass your own block:
      #
      #   Artist.finder(:name=>:first_by_name){|pl, ds| ds.where(:name=>pl.arg).limit(1)}
      #
      # Note that if you pass your own block, you are responsible for manually setting
      # limits if necessary (as shown above).
      #
      # Options:
      # :arity :: When using a symbol method name, this specifies the arity of the method.
      #           This should be used if if the method accepts an arbitrary number of arguments,
      #           or the method has default argument values.  Note that if the method is defined
      #           as a dataset method, the class method Sequel creates accepts an arbitrary number
      #           of arguments, so you should use this option in that case.  If you want to handle
      #           multiple possible arities, you need to call the finder method multiple times with
      #           unique :arity and :name methods each time.
      # :name :: The name of the method to create.  This must be given if you pass a block.
      #          If you use a symbol, this defaults to the symbol prefixed by the type.
      # :mod :: The module in which to create the finder method.  Defaults to the singleton
      #         class of the model.
      # :type :: The type of query to run.  Can be :first, :each, :all, or :get, defaults to
      #          :first.
      #
      # Caveats:
      #
      # This doesn't handle all possible cases.  For example, if you have a method such as:
      #
      #   def Artist.by_name(name)
      #     name ? where(:name=>name) : exclude(:name=>nil)
      #   end
      #
      # Then calling a finder without an argument will not work as you expect.
      #
      #   Artist.finder :by_name
      #   Artist.by_name(nil).first
      #   # WHERE (name IS NOT NULL)
      #   Artist.first_by_name(nil)
      #   # WHERE (name IS NULL)
      #
      # See Dataset::PlaceholderLiteralizer for additional caveats.
      def finder(meth=OPTS, opts=OPTS, &block)
        if block
          raise Error, "cannot pass both a method name argument and a block of Model.finder" unless meth.is_a?(Hash)
          raise Error, "cannot pass two option hashes to Model.finder" unless opts.equal?(OPTS)
          opts = meth
          raise Error, "must provide method name via :name option when passing block to Model.finder" unless meth_name = opts[:name]
        end

        type = opts.fetch(:type, :first)
        unless prepare = opts[:prepare]
          raise Error, ":type option to Model.finder must be :first, :all, :each, or :get" unless FINDER_TYPES.include?(type)
        end
        limit1 = type == :first || type == :get
        meth_name ||= opts[:name] || :"#{type}_#{meth}"

        argn = lambda do |model|
          if arity = opts[:arity]
            arity
          else
            method = block || model.method(meth)
            (method.arity < 0 ? method.arity.abs - 1 : method.arity)
          end
        end

        loader_proc = if prepare
          proc do |model|
            args = prepare_method_args('$a', argn.call(model))
            ds = if block
              model.instance_exec(*args, &block)
            else
              model.send(meth, *args)
            end
            ds = ds.limit(1) if limit1
            model_name = model.name
            if model_name.to_s.empty?
              model_name = model.object_id
            else
              model_name = model_name.gsub(/\W/, '_')
            end
            ds.prepare(type, :"#{model_name}_#{meth_name}")
          end
        else
          proc do |model|
            n = argn.call(model)
            block ||= lambda do |pl, model2|
              args = (0...n).map{pl.arg}
              ds = model2.send(meth, *args)
              ds = ds.limit(1) if limit1
              ds
            end

            Sequel::Dataset::PlaceholderLiteralizer.loader(model, &block) 
          end
        end

        Sequel.synchronize{@finder_loaders[meth_name] = loader_proc}
        mod = opts[:mod] || (class << self; self; end)
        if prepare
          def_prepare_method(mod, meth_name)
        else
          def_finder_method(mod, meth_name, type)
        end
      end

      # An alias for calling first on the model's dataset, but with
      # optimized handling of the single argument case.
      def first(*args, &block)
        if args.length == 1 && !block && !args.first.is_a?(Integer)
          # Use optimized finder
          first_where(args.first)
        else
          dataset.first(*args, &block)
        end
      end

      # An alias for calling first! on the model's dataset, but with
      # optimized handling of the single argument case.
      def first!(*args, &block)
        first(*args, &block) || raise(Sequel::NoMatchingRow.new(dataset))
      end

      # Clear the setter_methods cache when a module is included, as it
      # may contain setter methods.
      def include(*mods)
        clear_setter_methods_cache
        super
      end

      # If possible, set the dataset for the model subclass as soon as it
      # is created.  Also, make sure the inherited class instance variables
      # are copied into the subclass.
      #
      # Sequel queries the database to get schema information as soon as
      # a model class is created:
      #
      #   class Artist < Sequel::Model # Causes schema query
      #   end
      def inherited(subclass)
        super
        ivs = subclass.instance_variables.collect(&:to_s)
        inherited_instance_variables.each do |iv, dup|
          next if ivs.include?(iv.to_s)
          if (sup_class_value = instance_variable_get(iv)) && dup
            sup_class_value = case dup
            when :dup
              sup_class_value.dup
            when :hash_dup
              h = {}
              sup_class_value.each{|k,v| h[k] = v.dup}
              h
            when Proc
              dup.call(sup_class_value)
            else
              raise Error, "bad inherited instance variable type: #{dup.inspect}"
            end
          end
          subclass.instance_variable_set(iv, sup_class_value)
        end

        unless ivs.include?("@dataset")
          if @dataset && self != Model
            subclass.set_dataset(@dataset.clone, :inherited=>true) rescue nil
          elsif (n = subclass.name) && !n.to_s.empty?
            db
            subclass.set_dataset(subclass.implicit_table_name) rescue nil
          end
        end
      end

      # Returns the implicit table name for the model class, which is the demodulized,
      # underscored, pluralized name of the class.
      #
      #   Artist.implicit_table_name # => :artists
      #   Foo::ArtistAlias.implicit_table_name # => :artist_aliases
      def implicit_table_name
        pluralize(underscore(demodulize(name))).to_sym
      end
  
      # Calls #call with the values hash.  Only for backwards compatibility.
      def load(values)
        call(values)
      end

      # Clear the setter_methods cache when a setter method is added
      def method_added(meth)
        clear_setter_methods_cache if meth.to_s =~ SETTER_METHOD_REGEXP
        super
      end
  
      # Mark the model as not having a primary key. Not having a primary key
      # can cause issues, among which is that you won't be able to update records.
      #
      #   Artist.primary_key # => :id
      #   Artist.no_primary_key
      #   Artist.primary_key # => nil
      def no_primary_key
        clear_setter_methods_cache
        self.simple_pk = @primary_key = nil
      end
      
      # Loads a plugin for use with the model class, passing optional arguments
      # to the plugin.  If the plugin is a module, load it directly.  Otherwise,
      # require the plugin from either sequel/plugins/#{plugin} or
      # sequel_#{plugin}, and then attempt to load the module using a
      # the camelized plugin name under Sequel::Plugins.
      def plugin(plugin, *args, &block)
        m = plugin.is_a?(Module) ? plugin : plugin_module(plugin)
        unless @plugins.include?(m)
          @plugins << m
          m.apply(self, *args, &block) if m.respond_to?(:apply)
          extend(m::ClassMethods) if plugin_module_defined?(m, :ClassMethods)
          include(m::InstanceMethods) if plugin_module_defined?(m, :InstanceMethods)
          if plugin_module_defined?(m, :DatasetMethods)
            dataset_extend(m::DatasetMethods, :create_class_methods=>false)
          end
        end
        m.configure(self, *args, &block) if m.respond_to?(:configure)
      end

      # Returns primary key attribute hash.  If using a composite primary key
      # value such be an array with values for each primary key in the correct
      # order.  For a standard primary key, value should be an object with a
      # compatible type for the key.  If the model does not have a primary key,
      # raises an +Error+.
      #
      #   Artist.primary_key_hash(1) # => {:id=>1}
      #   Artist.primary_key_hash([1, 2]) # => {:id1=>1, :id2=>2}
      def primary_key_hash(value)
        case key = @primary_key
        when Symbol
          {key => value}
        when Array
          hash = {}
          key.zip(Array(value)){|k,v| hash[k] = v}
          hash
        else
          raise(Error, "#{self} does not have a primary key")
        end
      end

      # Return a hash where the keys are qualified column references.  Uses the given
      # qualifier if provided, or the table_name otherwise. This is useful if you
      # plan to join other tables to this table and you want the column references
      # to be qualified.
      #
      #   Artist.filter(Artist.qualified_primary_key_hash(1))
      #   # SELECT * FROM artists WHERE (artists.id = 1)
      def qualified_primary_key_hash(value, qualifier=table_name)
        case key = @primary_key
        when Symbol
          {SQL::QualifiedIdentifier.new(qualifier, key) => value}
        when Array
          hash = {}
          key.zip(Array(value)){|k,v| hash[SQL::QualifiedIdentifier.new(qualifier, k)] = v}
          hash
        else
          raise(Error, "#{self} does not have a primary key")
        end
      end
  
      # Similar to finder, but uses a prepared statement instead of a placeholder
      # literalizer. This makes the SQL used static (cannot vary per call), but
      # allows binding argument values instead of literalizing them into the SQL
      # query string.
      #
      # If a block is used with this method, it is instance_execed by the model,
      # and should accept the desired number of placeholder arguments.
      #
      # The options are the same as the options for finder, with the following
      # exception:
      # :type :: Specifies the type of prepared statement to create
      def prepared_finder(meth=OPTS, opts=OPTS, &block)
        if block
          raise Error, "cannot pass both a method name argument and a block of Model.finder" unless meth.is_a?(Hash)
          meth = meth.merge(:prepare=>true)
        else
          opts = opts.merge(:prepare=>true)
        end
        finder(meth, opts, &block)
      end

      # Restrict the setting of the primary key(s) when using mass assignment (e.g. +set+).  Because
      # this is the default, this only make sense to use in a subclass where the
      # parent class has used +unrestrict_primary_key+.
      def restrict_primary_key
        clear_setter_methods_cache
        @restrict_primary_key = true
      end
  
      # Whether or not setting the primary key(s) when using mass assignment (e.g. +set+) is
      # restricted, true by default.
      def restrict_primary_key?
        @restrict_primary_key
      end
  
      # Set the columns to allow when using mass assignment (e.g. +set+).  Using this means that
      # any columns not listed here will not be modified.  If you have any virtual
      # setter methods (methods that end in =) that you want to be used during
      # mass assignment, they need to be listed here as well (without the =).
      #
      # It may be better to use a method such as +set_only+ or +set_fields+ that lets you specify
      # the allowed fields per call.
      #
      #   Artist.set_allowed_columns(:name, :hometown)
      #   Artist.set(:name=>'Bob', :hometown=>'Sactown') # No Error
      #   Artist.set(:name=>'Bob', :records_sold=>30000) # Error
      def set_allowed_columns(*cols)
        clear_setter_methods_cache
        @allowed_columns = cols
      end
  
      # Sets the dataset associated with the Model class. +ds+ can be a +Symbol+,
      # +LiteralString+, <tt>SQL::Identifier</tt>, <tt>SQL::QualifiedIdentifier</tt>,
      # <tt>SQL::AliasedExpression</tt>
      # (all specifying a table name in the current database), or a +Dataset+.
      # If a dataset is used, the model's database is changed to the database of the given
      # dataset.  If a dataset is not used, a dataset is created from the current
      # database with the table name given. Other arguments raise an +Error+.
      # Returns self.
      #
      # This changes the row_proc of the dataset to return
      # model objects and extends the dataset with the dataset_method_modules.
      # It also attempts to determine the database schema for the model,
      # based on the given dataset.
      #
      #   Artist.set_dataset(:tbl_artists)
      #   Artist.set_dataset(DB[:artists])
      #
      # Note that you should not use this to change the model's dataset
      # at runtime.  If you have that need, you should look into Sequel's
      # sharding support.
      def set_dataset(ds, opts=OPTS)
        inherited = opts[:inherited]
        @dataset = convert_input_dataset(ds)
        @require_modification = Sequel::Model.require_modification.nil? ? @dataset.provides_accurate_rows_matched? : Sequel::Model.require_modification
        if inherited
          self.simple_table = superclass.simple_table
          @columns = @dataset.columns rescue nil
        else
          @dataset_method_modules.each{|m| @dataset.extend(m)} if @dataset_method_modules
        end
        @dataset.model = self if @dataset.respond_to?(:model=)
        check_non_connection_error{@db_schema = (inherited ? superclass.db_schema : get_db_schema)}
        reset_instance_dataset
        self
      end

      # Sets the primary key for this model. You can use either a regular 
      # or a composite primary key.  To not use a primary key, set to nil
      # or use +no_primary_key+.  On most adapters, Sequel can automatically
      # determine the primary key to use, so this method is not needed often.
      #
      #   class Person < Sequel::Model
      #     # regular key
      #     set_primary_key :person_id
      #   end
      #
      #   class Tagging < Sequel::Model
      #     # composite key
      #     set_primary_key [:taggable_id, :tag_id]
      #   end
      def set_primary_key(key)
        clear_setter_methods_cache
        if key.is_a?(Array)
          if key.length < 2
            key = key.first
          else
            key = key.dup.freeze
          end
        end
        self.simple_pk = if key && !key.is_a?(Array)
          (@dataset || db).literal(key)
        end
        @primary_key = key
      end
  
      # Cache of setter methods to allow by default, in order to speed up new/set/update instance methods.
      def setter_methods
        @setter_methods ||= get_setter_methods
      end

      # Sets up a dataset method that returns a filtered dataset.
      # Sometimes thought of as a scope, and like most dataset methods,
      # they can be chained.
      # For example:
      #
      #   Topic.subset(:joes, :username.like('%joe%'))
      #   Topic.subset(:popular){num_posts > 100}
      #   Topic.subset(:recent){created_on > Date.today - 7}
      #
      # Allows you to do:
      #
      #   Topic.joes.recent.popular
      #
      # to get topics with a username that includes joe that
      # have more than 100 posts and were created less than
      # 7 days ago.
      #
      # Both the args given and the block are passed to <tt>Dataset#filter</tt>.
      #
      # This method creates dataset methods that do not accept arguments.  To create
      # dataset methods that accept arguments, you should use define a
      # method directly inside a #dataset_module block.
      def subset(name, *args, &block)
        def_dataset_method(name){filter(*args, &block)}
      end
      
      # Returns name of primary table for the dataset. If the table for the dataset
      # is aliased, returns the aliased name.
      #
      #   Artist.table_name # => :artists
      #   Sequel::Model(:foo).table_name # => :foo
      #   Sequel::Model(:foo___bar).table_name # => :bar
      def table_name
        dataset.first_source_alias
      end
  
      # Allow the setting of the primary key(s) when using the mass assignment methods.
      # Using this method can open up security issues, be very careful before using it.
      #
      #   Artist.set(:id=>1) # Error
      #   Artist.unrestrict_primary_key
      #   Artist.set(:id=>1) # No Error
      def unrestrict_primary_key
        clear_setter_methods_cache
        @restrict_primary_key = false
      end

      # Return the model instance with the primary key, or nil if there is no matching record.
      def with_pk(pk)
        primary_key_lookup(pk)
      end

      # Return the model instance with the primary key, or raise NoMatchingRow if there is no matching record.
      def with_pk!(pk)
        with_pk(pk) || raise(NoMatchingRow.new(dataset))
      end
  
      # Add model methods that call dataset methods
      Plugins.def_dataset_methods(self, DATASET_METHODS)
  
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

      # Convert the given object to a Dataset that should be used as
      # this model's dataset.
      def convert_input_dataset(ds)
        case ds
        when Symbol, SQL::Identifier, SQL::QualifiedIdentifier, SQL::AliasedExpression, LiteralString
          self.simple_table = db.literal(ds)
          ds = db.from(ds)
        when Dataset
          self.simple_table = if ds.send(:simple_select_all?)
            ds.literal(ds.first_source_table)
          end
          @db = ds.db
        else
          raise(Error, "Model.set_dataset takes one of the following classes as an argument: Symbol, LiteralString, SQL::Identifier, SQL::QualifiedIdentifier, SQL::AliasedExpression, Dataset")
        end
        set_dataset_row_proc(ds)
        ds
      end

      # Add the module to the class's dataset_method_modules.  Extend the dataset with the
      # module if the model has a dataset.  Add dataset methods to the class for all
      # public dataset methods.
      def dataset_extend(mod, opts=OPTS)
        @dataset.extend(mod) if @dataset
        reset_instance_dataset
        dataset_method_modules << mod
        unless opts[:create_class_methods] == false
          mod.public_instance_methods.each{|meth| def_model_dataset_method(meth)}
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
        clear_setter_methods_cache
        columns, bad_columns = columns.partition{|x| NORMAL_METHOD_NAME_REGEXP.match(x.to_s)}
        bad_columns.each{|x| def_bad_column_accessor(x)}
        im = instance_methods.collect(&:to_s)
        columns.each do |column|
          meth = "#{column}="
          overridable_methods_module.module_eval("def #{column}; self[:#{column}] end", __FILE__, __LINE__) unless im.include?(column.to_s)
          overridable_methods_module.module_eval("def #{meth}(v); self[:#{column}] = v end", __FILE__, __LINE__) unless im.include?(meth)
        end
      end
  
      # Define a model method that calls the dataset method with the same name,
      # only used for methods with names that can't be presented directly in
      # ruby code.
      def def_model_dataset_method(meth)
        return if respond_to?(meth, true)

        if meth.to_s =~ NORMAL_METHOD_NAME_REGEXP
          instance_eval("def #{meth}(*args, &block); dataset.#{meth}(*args, &block) end", __FILE__, __LINE__)
        else
          (class << self; self; end).send(:define_method, meth){|*args, &block| dataset.send(meth, *args, &block)}
        end
      end

      # Define a finder method in the given module with the given method name that
      # load rows using the finder with the given name.
      def def_finder_method(mod, meth, type)
        mod.send(:define_method, meth){|*args, &block| finder_for(meth).send(type, *args, &block)}
      end

      # Define a prepared_finder method in the given module that will call the associated prepared
      # statement.
      def def_prepare_method(mod, meth)
        mod.send(:define_method, meth){|*args, &block| finder_for(meth).call(prepare_method_arg_hash(args), &block)}
      end

      # Find the finder to use for the give method.  If a finder has not been loaded
      # for the method, load the finder and set correctly in the finders hash, then
      # return the finder.
      def finder_for(meth)
        unless finder = Sequel.synchronize{@finders[meth]}
          finder_loader = @finder_loaders.fetch(meth)
          finder = finder_loader.call(self)
          Sequel.synchronize{@finders[meth] = finder}
        end
        finder
      end

      # Get the schema from the database, fall back on checking the columns
      # via the database if that will return inaccurate results or if
      # it raises an error.
      def get_db_schema(reload = reload_db_schema?)
        set_columns(nil)
        return nil unless @dataset
        schema_hash = {}
        ds_opts = dataset.opts
        get_columns = proc{check_non_connection_error{columns} || []}
        schema_array = check_non_connection_error{db.schema(dataset, :reload=>reload)} if db.supports_schema_parsing?
        if schema_array
          schema_array.each{|k,v| schema_hash[k] = v}

          # Set the primary key(s) based on the schema information,
          # if the schema information includes primary key information
          if schema_array.all?{|k,v| v.has_key?(:primary_key)}
            pks = schema_array.collect{|k,v| k if v[:primary_key]}.compact
            pks.length > 0 ? set_primary_key(pks) : no_primary_key
          end

          if (select = ds_opts[:select]) && !(select.length == 1 && select.first.is_a?(SQL::ColumnAll))
            # We don't remove the columns from the schema_hash,
            # as it's possible they will be used for typecasting
            # even if they are not selected.
            cols = get_columns.call
            cols.each{|c| schema_hash[c] ||= {}}
            def_column_accessor(*schema_hash.keys)
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

      # Uncached version of setter_methods, to be overridden by plugins
      # that want to modify the methods used.
      def get_setter_methods
        if allowed_columns
          allowed_columns.map{|x| "#{x}="}
        else
          meths = instance_methods.collect(&:to_s).grep(SETTER_METHOD_REGEXP) - RESTRICTED_SETTER_METHODS
          meths -= Array(primary_key).map{|x| "#{x}="} if primary_key && restrict_primary_key?
          meths
        end
      end
  
      # A hash of instance variables to automatically set up in subclasses.
      # See Sequel::Model::INHERITED_INSTANCE_VARIABLES.  It is safe to modify
      # the hash returned by this method, though it may not be safe to modify
      # values of the hash.
      def inherited_instance_variables
        INHERITED_INSTANCE_VARIABLES.dup
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
      # associations so that the methods can be overridden with +super+.
      def overridable_methods_module
        include(@overridable_methods_module = Module.new) unless @overridable_methods_module
        @overridable_methods_module
      end
      
      # Returns the module for the specified plugin. If the module is not 
      # defined, the corresponding plugin required.
      def plugin_module(plugin)
        module_name = plugin.to_s.gsub(/(^|_)(.)/){|x| x[-1..-1].upcase}
        if !Sequel::Plugins.const_defined?(module_name) ||
           (Sequel.const_defined?(module_name) &&
            Sequel::Plugins.const_get(module_name) == Sequel.const_get(module_name))
          begin
            require "sequel/plugins/#{plugin}"
          rescue LoadError => e
            begin
              require "sequel_#{plugin}"
            rescue LoadError => e2
              e.message << "; #{e2.message}"
              raise e
            end
          end
        end
        Sequel::Plugins.const_get(module_name)
      end

      # Check if the plugin module +plugin+ defines the constant named by +submod+.
      def plugin_module_defined?(plugin, submod)
        if RUBY_VERSION >= '1.9'
          plugin.const_defined?(submod, false)
        else
        # :nocov:
          plugin.const_defined?(submod)
        # :nocov:
        end
      end
  
      # An hash of prepared argument values for the given arguments, with keys
      # starting at a.  Used by the methods created by prepared_finder.
      def prepare_method_arg_hash(args)
        h = {}
        prepare_method_args('a', args.length).zip(args).each{|k, v| h[k] = v}
        h
      end

      # An array of prepared statement argument names, of length n and starting with base.
      def prepare_method_args(base, n)
        (0...n).map do
          s = base.to_sym
          base = base.next
          s
        end
      end

      # Find the row in the dataset that matches the primary key.  Uses
      # a static SQL optimization if the table and primary key are simple.
      #
      # This method should not be called with a nil primary key, in case
      # it is overridden by plugins which assume that the passed argument
      # is valid.
      def primary_key_lookup(pk)
        if sql = @fast_pk_lookup_sql
          sql = sql.dup
          ds = dataset
          ds.literal_append(sql, pk)
          ds.fetch_rows(sql){|r| return ds.row_proc.call(r)}
          nil
        elsif dataset.joined_dataset?
          first_where(qualified_primary_key_hash(pk))
        else
          first_where(primary_key_hash(pk))
        end
      end

      # Whether to reload the database schema by default, ignoring any cached value.
      def reload_db_schema?
        false
      end
      
      # Reset the cached fast primary lookup SQL if a simple table and primary key
      # are used, or set it to nil if not used.
      def reset_fast_pk_lookup_sql
        @fast_pk_lookup_sql = if @simple_table && @simple_pk
          "SELECT * FROM #@simple_table WHERE #@simple_pk = ".freeze
        end
        @fast_instance_delete_sql = if @simple_table && @simple_pk
          "DELETE FROM #@simple_table WHERE #@simple_pk = ".freeze
        end
      end

      # Reset the instance dataset to a modified copy of the current dataset,
      # should be used whenever the model's dataset is modified.
      def reset_instance_dataset
        @finders.clear if @finders
        @instance_dataset = @dataset.limit(1).naked if @dataset
      end
  
      # Set the columns for this model and create accessor methods for each column.
      def set_columns(new_columns)
        @columns = new_columns
        def_column_accessor(*new_columns) if new_columns
        @columns
      end

      # Set the dataset's row_proc to the current model.
      def set_dataset_row_proc(ds)
        ds.row_proc = self
      end

      # Reset the fast primary key lookup SQL when the simple_pk value changes.
      def simple_pk=(pk)
        @simple_pk = pk
        reset_fast_pk_lookup_sql
      end

      # Reset the fast primary key lookup SQL when the simple_table value changes.
      def simple_table=(t)
        @simple_table = t
        reset_fast_pk_lookup_sql
      end

      # Returns a copy of the model's dataset with custom SQL
      #
      #   Artist.fetch("SELECT * FROM artists WHERE name LIKE 'A%'")
      #   Artist.fetch("SELECT * FROM artists WHERE id = ?", 1)
      alias fetch with_sql
    end

    # Sequel::Model instance methods that implement basic model functionality.
    #
    # * All of the methods in +HOOKS+ and +AROUND_HOOKS+ create instance methods that are called
    #   by Sequel when the appropriate action occurs.  For example, when destroying
    #   a model object, Sequel will call +around_destroy+, which will call +before_destroy+, do
    #   the destroy, and then call +after_destroy+.
    # * The following instance_methods all call the class method of the same
    #   name: columns, db, primary_key, db_schema.
    # * All of the methods in +BOOLEAN_SETTINGS+ create attr_writers allowing you
    #   to set values for the attribute.  It also creates instance getters returning
    #   the value of the setting.  If the value has not yet been set, it
    #   gets the default value from the class by calling the class method of the same name.
    module InstanceMethods
      HOOKS.each{|h| class_eval("def #{h}; end", __FILE__, __LINE__)}
      AROUND_HOOKS.each{|h| class_eval("def #{h}; yield end", __FILE__, __LINE__)}

      # Define instance method(s) that calls class method(s) of the
      # same name. Replaces the construct:
      #   
      #   define_method(meth){self.class.send(meth)}
      [:columns, :db, :primary_key, :db_schema].each{|meth| class_eval("def #{meth}; self.class.#{meth} end", __FILE__, __LINE__)}

      # Define instance method(s) that calls class method(s) of the
      # same name, caching the result in an instance variable.  Define
      # standard attr_writer method for modifying that instance variable.
      BOOLEAN_SETTINGS.each{|meth| class_eval("def #{meth}; !defined?(@#{meth}) ? (frozen? ? self.class.#{meth} : (@#{meth} = self.class.#{meth})) : @#{meth} end", __FILE__, __LINE__)}
      attr_writer(*BOOLEAN_SETTINGS)

      # The hash of attribute values.  Keys are symbols with the names of the
      # underlying database columns. The returned hash is a reference to the
      # receiver's values hash, and modifying it will also modify the receiver's
      # values.
      #
      #   Artist.new(:name=>'Bob').values # => {:name=>'Bob'}
      #   Artist[1].values # => {:id=>1, :name=>'Jim', ...}
      attr_reader :values
      alias to_hash values

      # Get the value of the column.  Takes a single symbol or string argument.
      # By default it calls send with the argument to get the value.  This can
      # be overridden if you have columns that conflict with existing
      # method names.
      alias get_column_value send

      # Set the value of the column.  Takes two argument.  The first is a
      # symbol or string argument for the column name, suffixed with =.  The
      # second is the value to set for the column.  By default it calls send
      # with the argument to set the value.  This can be overridden if you have
      # columns that conflict with existing method names (unlikely for setter
      # methods, but possible).
      alias set_column_value send

      # Creates new instance and passes the given values to set.
      # If a block is given, yield the instance to the block.
      #
      # Arguments:
      # values :: should be a hash to pass to set. 
      #
      #   Artist.new(:name=>'Bob')
      #
      #   Artist.new do |a|
      #     a.name = 'Bob'
      #   end
      def initialize(values = {})
        @values = {}
        @new = true
        @modified = true
        initialize_set(values)
        changed_columns.clear 
        yield self if block_given?
      end

      # Returns value of the column's attribute.
      #
      #   Artist[1][:id] #=> 1
      def [](column)
        @values[column]
      end
  
      # Sets the value for the given column.  If typecasting is enabled for
      # this object, typecast the value based on the column's type.
      # If this is a new record or the typecasted value isn't the same
      # as the current value for the column, mark the column as changed.
      #
      #   a = Artist.new
      #   a[:name] = 'Bob'
      #   a.values #=> {:name=>'Bob'}
      def []=(column, value)
        # If it is new, it doesn't have a value yet, so we should
        # definitely set the new value.
        # If the column isn't in @values, we can't assume it is
        # NULL in the database, so assume it has changed.
        v = typecast_value(column, value)
        vals = @values
        if new? || !vals.include?(column) || v != (c = vals[column]) || v.class != c.class
          change_column_value(column, v)
        end
      end
  
      # Alias of eql?
      def ==(obj)
        eql?(obj)
      end
  
      # If pk is not nil, true only if the objects have the same class and pk.
      # If pk is nil, false.
      #
      #   Artist[1] === Artist[1] # true
      #   Artist.new === Artist.new # false
      #   Artist[1].set(:name=>'Bob') == Artist[1] # => true
      def ===(obj)
        pk.nil? ? false : (obj.class == model) && (obj.pk == pk)
      end
  
      # class is defined in Object, but it is also a keyword,
      # and since a lot of instance methods call class methods,
      # this alias makes it so you can use model instead of
      # self.class.
      #
      #   Artist.new.model # => Artist
      alias_method :model, :class

      # The autoincrementing primary key for this model object. Should be
      # overridden if you have a composite primary key with one part of it
      # being autoincrementing.
      def autoincrementing_primary_key
        primary_key
      end

      # Cancel the current action.  Should be called in before hooks to halt
      # the processing of the action.  If a +msg+ argument is given and
      # the model instance is configured to raise exceptions on failure,
      # sets the message to use for the raised HookFailed exception.
      def cancel_action(msg=nil)
        raise_hook_failure(msg)
      end
  
      # The columns that have been updated.  This isn't completely accurate,
      # as it could contain columns whose values have not changed.
      #
      #   a = Artist[1]
      #   a.changed_columns # => []
      #   a.name = 'Bob'
      #   a.changed_columns # => [:name]
      def changed_columns
        @changed_columns ||= []
      end
  
      # Deletes and returns +self+.  Does not run destroy hooks.
      # Look into using +destroy+ instead.
      #
      #   Artist[1].delete # DELETE FROM artists WHERE (id = 1)
      #   # => #<Artist {:id=>1, ...}>
      def delete
        raise Sequel::Error, "can't delete frozen object" if frozen?
        _delete
        self
      end
      
      # Like delete but runs hooks before and after delete.
      # If before_destroy returns false, returns false without
      # deleting the object from the database. Otherwise, deletes
      # the item from the database and returns self.  Uses a transaction
      # if use_transactions is true or if the :transaction option is given and
      # true.
      #
      #   Artist[1].destroy # BEGIN; DELETE FROM artists WHERE (id = 1); COMMIT;
      #   # => #<Artist {:id=>1, ...}>
      def destroy(opts = OPTS)
        raise Sequel::Error, "can't destroy frozen object" if frozen?
        checked_save_failure(opts){checked_transaction(opts){_destroy(opts)}}
      end

      # Iterates through all of the current values using each.
      #
      #  Album[1].each{|k, v| puts "#{k} => #{v}"}
      #  # id => 1
      #  # name => 'Bob'
      def each(&block)
        @values.each(&block)
      end
  
      # Compares model instances by values.
      #
      #   Artist[1] == Artist[1] # => true
      #   Artist.new == Artist.new # => true
      #   Artist[1].set(:name=>'Bob') == Artist[1] # => false
      def eql?(obj)
        (obj.class == model) && (obj.values == @values)
      end

      # Returns the validation errors associated with this object.
      # See +Errors+.
      def errors
        @errors ||= errors_class.new
      end 

      # Returns true when current instance exists, false otherwise.
      # Generally an object that isn't new will exist unless it has
      # been deleted.  Uses a database query to check for existence,
      # unless the model object is new, in which case this is always
      # false.
      #
      #   Artist[1].exists? # SELECT 1 FROM artists WHERE (id = 1)
      #   # => true
      #   Artist.new.exists?
      #   # => false
      def exists?
        new? ? false : !this.get(SQL::AliasedExpression.new(1, :one)).nil?
      end
      
      # Ignore the model's setter method cache when this instances extends a module, as the
      # module may contain setter methods.
      def extend(mod)
        @singleton_setter_added = true
        super
      end

      # Freeze the object in such a way that it is still usable but not modifiable.
      # Once an object is frozen, you cannot modify it's values, changed_columns,
      # errors, or dataset.
      def freeze
        values.freeze
        changed_columns.freeze
        unless errors.frozen?
          validate
          errors.freeze
        end
        this.freeze if !new? && model.primary_key
        super
      end
  
      # Value that should be unique for objects with the same class and pk (if pk is not nil), or
      # the same class and values (if pk is nil).
      #
      #   Artist[1].hash == Artist[1].hash # true
      #   Artist[1].set(:name=>'Bob').hash == Artist[1].hash # true
      #   Artist.new.hash == Artist.new.hash # true
      #   Artist.new(:name=>'Bob').hash == Artist.new.hash # false
      def hash
        case primary_key
        when Array
          [model, !pk.all? ? @values : pk].hash
        when Symbol
          [model, pk.nil? ? @values : pk].hash
        else
          [model, @values].hash
        end
      end
  
      # Returns value for the :id attribute, even if the primary key is
      # not id. To get the primary key value, use +pk+.
      #
      #   Artist[1].id # => 1
      def id
        @values[:id]
      end
  
      # Returns a string representation of the model instance including
      # the class name and values.
      def inspect
        "#<#{model.name} @values=#{inspect_values}>"
      end
  
      # Returns the keys in +values+.  May not include all column names.
      #
      #   Artist.new.keys # => []
      #   Artist.new(:name=>'Bob').keys # => [:name]
      #   Artist[1].keys # => [:id, :name]
      def keys
        @values.keys
      end
      
      # Refresh this record using +for_update+ unless this is a new record.  Returns self.
      # This can be used to make sure no other process is updating the record at the
      # same time.
      #
      #   a = Artist[1]
      #   Artist.db.transaction do
      #     a.lock!
      #     a.update(:name=>'A')
      #   end
      def lock!
        _refresh(this.for_update) unless new?
        self
      end
      
      # Remove elements of the model object that make marshalling fail. Returns self.
      #
      #   a = Artist[1]
      #   a.marshallable!
      #   Marshal.dump(a)
      def marshallable!
        @this = nil
        self
      end

      # Explicitly mark the object as modified, so +save_changes+/+update+ will
      # run callbacks even if no columns have changed.
      #
      #   a = Artist[1]
      #   a.save_changes # No callbacks run, as no changes
      #   a.modified!
      #   a.save_changes # Callbacks run, even though no changes made
      #
      # If a column is given, specifically marked that column as modified,
      # so that +save_changes+/+update+ will include that column in the
      # update. This should be used if you plan on mutating the column
      # value instead of assigning a new column value:
      #
      #   a.modified!(:name)
      #   a.name.gsub!(/[aeou]/, 'i')
      def modified!(column=nil)
        if column && !changed_columns.include?(column)
          changed_columns << column
        end
        @modified = true
      end

      # Whether this object has been modified since last saved, used by
      # save_changes to determine whether changes should be saved.  New
      # values are always considered modified.
      #
      #   a = Artist[1]
      #   a.modified? # => false
      #   a.set(:name=>'Jim')
      #   a.modified? # => true
      #
      # If a column is given, specifically check if the given column has
      # been modified:
      #
      #   a.modified?(:num_albums) # => false
      #   a.num_albums = 10
      #   a.modified?(:num_albums) # => true
      def modified?(column=nil)
        if column
          changed_columns.include?(column)
        else
          @modified || !changed_columns.empty?
        end
      end
  
      # Returns true if the current instance represents a new record.
      #
      #   Artist.new.new? # => true
      #   Artist[1].new? # => false
      def new?
        defined?(@new) ? @new : (@new = false)
      end
      
      # Returns the primary key value identifying the model instance.
      # Raises an +Error+ if this model does not have a primary key.
      # If the model has a composite primary key, returns an array of values.
      #
      #   Artist[1].pk # => 1
      #   Artist[[1, 2]].pk # => [1, 2]
      def pk
        raise(Error, "No primary key is associated with this model") unless key = primary_key
        if key.is_a?(Array)
          vals = @values
          key.map{|k| vals[k]}
        else
          @values[key]
        end
      end
      
      # Returns a hash mapping the receivers primary key column(s) to their values.
      # 
      #   Artist[1].pk_hash # => {:id=>1}
      #   Artist[[1, 2]].pk_hash # => {:id1=>1, :id2=>2}
      def pk_hash
        model.primary_key_hash(pk)
      end
      
      # Returns a hash mapping the receivers primary key column(s) to their values.
      # 
      #   Artist[1].qualified_pk_hash
      #   # => {Sequel.qualify(:artists, :id)=>1}
      #   Artist[[1, 2]].qualified_pk_hash
      #   # => {Sequel.qualify(:artists, :id1)=>1, Sequel.qualify(:artists, :id2)=>2}
      def qualified_pk_hash(qualifier=model.table_name)
        model.qualified_primary_key_hash(pk, qualifier)
      end
      
      # Reloads attributes from database and returns self. Also clears all
      # changed_columns information.  Raises an +Error+ if the record no longer
      # exists in the database.
      #
      #   a = Artist[1]
      #   a.name = 'Jim'
      #   a.refresh
      #   a.name # => 'Bob'
      def refresh
        raise Sequel::Error, "can't refresh frozen object" if frozen?
        _refresh(this)
        self
      end

      # Alias of refresh, but not aliased directly to make overriding in a plugin easier.
      def reload
        refresh
      end
  
      # Creates or updates the record, after making sure the record
      # is valid and before hooks execute successfully. Fails if:
      #
      # * the record is not valid, or
      # * before_save returns false, or
      # * the record is new and before_create returns false, or
      # * the record is not new and before_update returns false.
      #
      # If +save+ fails and either raise_on_save_failure or the
      # :raise_on_failure option is true, it raises ValidationFailed
      # or HookFailed. Otherwise it returns nil.
      #
      # If it succeeds, it returns self.
      #
      # You can provide an optional list of columns to update, in which
      # case it only updates those columns, or a options hash.
      #
      # Takes the following options:
      #
      # :changed :: save all changed columns, instead of all columns or the columns given
      # :columns :: array of specific columns that should be saved.
      # :raise_on_failure :: set to true or false to override the current
      #                      +raise_on_save_failure+ setting
      # :server :: set the server/shard on the object before saving, and use that
      #            server/shard in any transaction.
      # :transaction :: set to true or false to override the current
      #                 +use_transactions+ setting
      # :validate :: set to false to skip validation
      def save(opts=OPTS)
        raise Sequel::Error, "can't save frozen object" if frozen?
        set_server(opts[:server]) if opts[:server] 
        _before_validation
        if opts[:validate] != false
          unless checked_save_failure(opts){_valid?(true, opts)}
            raise(ValidationFailed.new(self)) if raise_on_failure?(opts)
            return
          end
        end
        checked_save_failure(opts){checked_transaction(opts){_save(opts)}}
      end

      # Saves only changed columns if the object has been modified.
      # If the object has not been modified, returns nil.  If unable to
      # save, returns false unless +raise_on_save_failure+ is true.
      #
      #   a = Artist[1]
      #   a.save_changes # => nil
      #   a.name = 'Jim'
      #   a.save_changes # UPDATE artists SET name = 'Bob' WHERE (id = 1)
      #   # => #<Artist {:id=>1, :name=>'Jim', ...}
      def save_changes(opts=OPTS)
        save(Hash[opts].merge!(:changed=>true)) || false if modified? 
      end
  
      # Updates the instance with the supplied values with support for virtual
      # attributes, raising an exception if a value is used that doesn't have
      # a setter method (or ignoring it if <tt>strict_param_setting = false</tt>).
      # Does not save the record.
      #
      #   artist.set(:name=>'Jim')
      #   artist.name # => 'Jim'
      def set(hash)
        set_restricted(hash, :default)
      end
  
      # Set all values using the entries in the hash, ignoring any setting of
      # allowed_columns in the model.
      #
      #   Artist.set_allowed_columns(:num_albums)
      #   artist.set_all(:name=>'Jim')
      #   artist.name # => 'Jim'
      def set_all(hash)
        set_restricted(hash, :all)
      end
  
      # For each of the fields in the given array +fields+, call the setter
      # method with the value of that +hash+ entry for the field. Returns self.
      #
      # You can provide an options hash, with the following options currently respected:
      # :missing :: Can be set to :skip to skip missing entries or :raise to raise an
      #             Error for missing entries.  The default behavior is not to check for
      #             missing entries, in which case the default value is used.  To be
      #             friendly with most web frameworks, the missing check will also check
      #             for the string version of the argument in the hash if given a symbol.
      #
      # Examples:
      #
      #   artist.set_fields({:name=>'Jim'}, [:name])
      #   artist.name # => 'Jim'
      #
      #   artist.set_fields({:hometown=>'LA'}, [:name])
      #   artist.name # => nil
      #   artist.hometown # => 'Sac'
      #
      #   artist.name # => 'Jim'
      #   artist.set_fields({}, [:name], :missing=>:skip)
      #   artist.name # => 'Jim'
      #
      #   artist.name # => 'Jim'
      #   artist.set_fields({}, [:name], :missing=>:raise)
      #   # Sequel::Error raised
      def set_fields(hash, fields, opts=nil)
        opts = if opts
          Hash[model.default_set_fields_options].merge!(opts)
        else
          model.default_set_fields_options
        end

        case opts[:missing]
        when :skip
          fields.each do |f|
            if hash.has_key?(f) 
              set_column_value("#{f}=", hash[f])
            elsif f.is_a?(Symbol) && hash.has_key?(sf = f.to_s)
              set_column_value("#{sf}=", hash[sf])
            end
          end
        when :raise
          fields.each do |f|
            if hash.has_key?(f)
              set_column_value("#{f}=", hash[f])
            elsif f.is_a?(Symbol) && hash.has_key?(sf = f.to_s)
              set_column_value("#{sf}=", hash[sf])
            else
              raise(Sequel::Error, "missing field in hash: #{f.inspect} not in #{hash.inspect}")
            end
          end
        else
          fields.each{|f| set_column_value("#{f}=", hash[f])}
        end
        self
      end
  
      # Set the values using the entries in the hash, only if the key
      # is included in only.  It may be a better idea to use +set_fields+
      # instead of this method.
      #
      #   artist.set_only({:name=>'Jim'}, :name)
      #   artist.name # => 'Jim'
      #
      #   artist.set_only({:hometown=>'LA'}, :name) # Raise Error
      def set_only(hash, *only)
        set_restricted(hash, only.flatten)
      end
  
      # Set the shard that this object is tied to.  Returns self.
      def set_server(s)
        @server = s
        @this.opts[:server] = s if @this
        self
      end

      # Clear the setter_methods cache when a method is added
      def singleton_method_added(meth)
        @singleton_setter_added = true if meth.to_s =~ SETTER_METHOD_REGEXP
        super
      end
  
      # Returns (naked) dataset that should return only this instance.
      #
      #   Artist[1].this
      #   # SELECT * FROM artists WHERE (id = 1) LIMIT 1
      def this
        return @this if @this
        raise Error, "No dataset for model #{model}" unless ds = model.instance_dataset

        cond = if ds.joined_dataset?
          qualified_pk_hash
        else
          pk_hash
        end

        @this = use_server(ds.where(cond))
      end
      
      # Runs #set with the passed hash and then runs save_changes.
      #
      #   artist.update(:name=>'Jim') # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      def update(hash)
        update_restricted(hash, :default)
      end
  
      # Update all values using the entries in the hash, ignoring any setting of
      # +allowed_columns+ in the model.
      #
      #   Artist.set_allowed_columns(:num_albums)
      #   artist.update_all(:name=>'Jim') # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      def update_all(hash)
        update_restricted(hash, :all)
      end
  
      # Update the instances values by calling +set_fields+ with the arguments, then
      # saves any changes to the record.  Returns self.
      #
      #   artist.update_fields({:name=>'Jim'}, [:name])
      #   # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      #
      #   artist.update_fields({:hometown=>'LA'}, [:name])
      #   # UPDATE artists SET name = NULL WHERE (id = 1)
      def update_fields(hash, fields, opts=nil)
        set_fields(hash, fields, opts)
        save_changes
      end

      # Update the values using the entries in the hash, only if the key
      # is included in only.  It may be a better idea to use +update_fields+
      # instead of this method.
      #
      #   artist.update_only({:name=>'Jim'}, :name)
      #   # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      #
      #   artist.update_only({:hometown=>'LA'}, :name) # Raise Error
      def update_only(hash, *only)
        update_restricted(hash, only.flatten)
      end
      
      # Validates the object.  If the object is invalid, errors should be added
      # to the errors attribute.  By default, does nothing, as all models
      # are valid by default.  See the {"Model Validations" guide}[rdoc-ref:doc/validations.rdoc].
      # for details about validation.  Should not be called directly by
      # user code, call <tt>valid?</tt> instead to check if an object
      # is valid.
      def validate
      end

      # Validates the object and returns true if no errors are reported.
      #
      #   artist(:name=>'Valid').valid? # => true
      #   artist(:name=>'Invalid').valid? # => false
      #   artist.errors.full_messages # => ['name cannot be Invalid']
      def valid?(opts = OPTS)
        _before_validation
        _valid?(false, opts)
      end

      private
      
      # Run code directly after the INSERT query, before after_create.
      # This is only a temporary API, it should not be overridden by external code.
      def _after_create(pk)
        @this = nil
        @new = false
        @was_new = true
      end

      # Run code after around_save returns, before calling after_commit.
      # This is only a temporary API, it should not be overridden by external code.
      def _after_save(pk)
        if @was_new
          @was_new = nil
          pk ? _save_refresh : changed_columns.clear
        else
          @columns_updated = nil
        end
        @modified = false
      end

      # Run code directly after the UPDATE query, before after_update.
      # This is only a temporary API, it should not be overridden by external code.
      def _after_update
        @this = nil
      end

      # Run code before any validation is done, but also run it before saving
      # even if validation is skipped.  This is a private hook.  It exists so that
      # plugins can set values automatically before validation (as the values
      # need to be validated), but should be set even if validation is skipped.
      # Unlike the regular before_validation hook, we do not skip the save/validation
      # if this returns false.
      def _before_validation
      end

      # Do the deletion of the object's dataset, and check that the row
      # was actually deleted.
      def _delete
        n = _delete_without_checking
        raise(NoExistingObject, "Attempt to delete object did not result in a single row modification (Rows Deleted: #{n}, SQL: #{_delete_dataset.delete_sql})") if require_modification && n != 1
        n
      end
      
      # The dataset to use when deleting the object.   The same as the object's
      # dataset by default.
      def _delete_dataset
        this
      end
  
      # Actually do the deletion of the object's dataset.  Return the
      # number of rows modified.
      def _delete_without_checking
        if sql = (m = model).fast_instance_delete_sql
          sql = sql.dup
          ds = use_server(m.dataset)
          ds.literal_append(sql, pk)
          ds.with_sql_delete(sql)
        else
          _delete_dataset.delete 
        end
      end

      # Internal destroy method, separted from destroy to
      # allow running inside a transaction
      def _destroy(opts)
        sh = {:server=>this_server}
        db.after_rollback(sh){after_destroy_rollback} if uacr = use_after_commit_rollback
        called = false
        around_destroy do
          called = true
          raise_hook_failure(:before_destroy) if before_destroy == false
          _destroy_delete
          after_destroy
          true
        end
        raise_hook_failure(:around_destroy) unless called
        db.after_commit(sh){after_destroy_commit} if uacr
        self
      end
      
      # Internal delete method to call when destroying an object,
      # separated from delete to allow you to override destroy's version
      # without affecting delete.
      def _destroy_delete
        delete
      end

      # Insert the record into the database, returning the primary key if
      # the record should be refreshed from the database.
      def _insert
        ds = _insert_dataset
        if _use_insert_select?(ds) && (h = _insert_select_raw(ds))
          _save_set_values(h)
          nil
        else
          iid = _insert_raw(ds)
          # if we have a regular primary key and it's not set in @values,
          # we assume it's the last inserted id
          if (pk = autoincrementing_primary_key) && pk.is_a?(Symbol) && !(vals = @values)[pk]
            vals[pk] = iid
          end
          pk
        end
      end

      # The dataset to use when inserting a new object.   The same as the model's
      # dataset by default.
      def _insert_dataset
        use_server(model.instance_dataset)
      end
  
      # Insert into the given dataset and return the primary key created (if any).
      def _insert_raw(ds)
        ds.insert(_insert_values)
      end

      # Insert into the given dataset and return the hash of column values.
      def _insert_select_raw(ds)
        ds.insert_select(_insert_values)
      end

      # The values hash to use when inserting a new record.
      alias _insert_values values
      
      # Refresh using a particular dataset, used inside save to make sure the same server
      # is used for reading newly inserted values from the database
      def _refresh(dataset)
        _refresh_set_values(_refresh_get(dataset) || raise(NoExistingObject, "Record not found"))
        changed_columns.clear
      end

      # Get the row of column data from the database.
      def _refresh_get(dataset)
        dataset.first
      end
      
      # Set the refreshed values after 
      def _refresh_set_values(h)
        @values = h
      end

      # Internal version of save, split from save to allow running inside
      # it's own transaction.
      def _save(opts)
        sh = {:server=>this_server}
        db.after_rollback(sh){after_rollback} if uacr = use_after_commit_rollback
        pk = nil
        called_save = false
        called_cu = false
        around_save do
          called_save = true
          raise_hook_failure(:before_save) if before_save == false
          if new?
            around_create do
              called_cu = true
              raise_hook_failure(:before_create) if before_create == false
              pk = _insert
              _after_create(pk)
              after_create
              true
            end
            raise_hook_failure(:around_create) unless called_cu
          else
            around_update do
              called_cu = true
              raise_hook_failure(:before_update) if before_update == false
              columns = opts[:columns]
              if columns.nil?
                @columns_updated = if opts[:changed]
                  @values.reject{|k,v| !changed_columns.include?(k)}
                else
                  _save_update_all_columns_hash
                end
                changed_columns.clear
              else # update only the specified columns
                columns = Array(columns)
                @columns_updated = @values.reject{|k, v| !columns.include?(k)}
                changed_columns.reject!{|c| columns.include?(c)}
              end
              _update_columns(@columns_updated)
              _after_update
              after_update
              true
            end
            raise_hook_failure(:around_update) unless called_cu
          end
          after_save
          true
        end
        raise_hook_failure(:around_save) unless called_save
        _after_save(pk)
        db.after_commit(sh){after_commit} if uacr
        self
      end
      
      # Refresh the object after saving it, used to get
      # default values of all columns.  Separated from _save so it
      # can be overridden to avoid the refresh.
      def _save_refresh
        _save_set_values(_refresh_get(this.server?(:default)) || raise(NoExistingObject, "Record not found"))
        changed_columns.clear
      end

      # Set values to the provided hash.  Called after a create,
      # to set the full values from the database in the model instance.
      def _save_set_values(h)
        @values = h
      end

      # Return a hash of values used when saving all columns of an
      # existing object (i.e. not passing specific columns to save
      # or using update/save_changes).  Defaults to all of the
      # object's values except unmodified primary key columns, as some
      # databases don't like you setting primary key values even
      # to their existing values.
      def _save_update_all_columns_hash
        v = Hash[@values]
        Array(primary_key).each{|x| v.delete(x) unless changed_columns.include?(x)}
        v
      end

      # Call _update with the given columns, if any are present.
      # Plugins can override this method in order to update with
      # additional columns, even when the column hash is initially empty.
      def _update_columns(columns)
        _update(columns) unless columns.empty?
      end

      # Update this instance's dataset with the supplied column hash,
      # checking that only a single row was modified.
      def _update(columns)
        n = _update_without_checking(columns)
        raise(NoExistingObject, "Attempt to update object did not result in a single row modification (SQL: #{_update_dataset.update_sql(columns)})") if require_modification && n != 1
        n
      end
      
      # The dataset to use when updating an object.  The same as the object's
      # dataset by default.
      def _update_dataset
        this
      end

      # Update this instances dataset with the supplied column hash.
      def _update_without_checking(columns)
        _update_dataset.update(columns)
      end

      # Whether to use insert_select when inserting a new row.
      def _use_insert_select?(ds)
        (!ds.opts[:select] || ds.opts[:returning]) && ds.supports_insert_select? 
      end

      # Internal validation method.  If +raise_errors+ is +true+, hook
      # failures will be raised as HookFailure exceptions.  If it is
      # +false+, +false+ will be returned instead.
      def _valid?(raise_errors, opts)
        return errors.empty? if frozen?
        errors.clear
        called = false
        error = false
        around_validation do
          called = true
          if before_validation == false
            if raise_errors
              raise_hook_failure(:before_validation)
            else
              error = true
            end
            false
          else
            validate
            after_validation
            errors.empty?
          end
        end
        error = true unless called
        if error
          if raise_errors
            raise_hook_failure(:around_validation)
          else
            false
          end
        else
          errors.empty?
        end
      end

      # If not raising on failure, check for HookFailed
      # being raised by yielding and swallow it.
      def checked_save_failure(opts)
        if raise_on_failure?(opts)
          yield
        else
          begin
            yield
          rescue HookFailed 
            nil
          end
        end
      end
      
      # If transactions should be used, wrap the yield in a transaction block.
      def checked_transaction(opts=OPTS)
        use_transaction?(opts) ? db.transaction({:server=>this_server}.merge!(opts)){yield} : yield
      end

      # Change the value of the column to given value, recording the change.
      def change_column_value(column, value)
        cc = changed_columns
        cc << column unless cc.include?(column)
        @values[column] = value
      end

      # Default error class used for errors.
      def errors_class
        Errors
      end

      if RUBY_VERSION >= '1.9'
        # Clone constructor -- freeze internal data structures if the original's
        # are frozen.
        def initialize_clone(other)
          super
          freeze if other.frozen?
          self
        end
      else
        # :nocov:
        # Ruby 1.8 doesn't support initialize_clone, so override clone to dup and freeze. 
        def clone
          o = dup
          o.freeze if frozen?
          o
        end
        public :clone
        # :nocov:
      end

      # Copy constructor -- Duplicate internal data structures.
      def initialize_copy(other)
        super
        @values = Hash[@values]
        @changed_columns = @changed_columns.dup if @changed_columns
        @errors = @errors.dup if @errors
        @this = @this.dup if @this
        self
      end

      # Set the columns with the given hash.  By default, the same as +set+, but
      # exists so it can be overridden.  This is called only for new records, before
      # changed_columns is cleared.
      def initialize_set(h)
        set(h) unless h.empty?
      end

      # Default inspection output for the values hash, overwrite to change what #inspect displays.
      def inspect_values
        @values.inspect
      end

      # Whether to raise or return false if this action fails. If the
      # :raise_on_failure option is present in the hash, use that, otherwise,
      # fallback to the object's raise_on_save_failure (if set), or
      # class's default (if not).
      def raise_on_failure?(opts)
        opts.fetch(:raise_on_failure, raise_on_save_failure)
      end

      # Raise an error appropriate to the hook type. May be swallowed by
      # checked_save_failure depending on the raise_on_failure? setting.
      def raise_hook_failure(type=nil)
        msg = case type
        when String
          type
        when Symbol
          "the #{type} hook failed"
        else
          "a hook failed"
        end

        raise HookFailed.new(msg, self)
      end
  
      # Get the ruby class or classes related to the given column's type.
      def schema_type_class(column)
        if (sch = db_schema[column]) && (type = sch[:type])
          db.schema_type_class(type)
        end
      end

      # Call setter methods based on keys in hash, with the appropriate values.
      # Restrict which methods can be called based on the provided type.
      def set_restricted(hash, type)
        return self if hash.empty?
        meths = setter_methods(type)
        strict = strict_param_setting
        hash.each do |k,v|
          m = "#{k}="
          if meths.include?(m)
            set_column_value(m, v)
          elsif strict
            # Avoid using respond_to? or creating symbols from user input
            if public_methods.map(&:to_s).include?(m)
              if Array(model.primary_key).map(&:to_s).member?(k.to_s) && model.restrict_primary_key?
                raise MassAssignmentRestriction, "#{k} is a restricted primary key"
              else
                raise MassAssignmentRestriction, "#{k} is a restricted column"
              end
            else
              raise MassAssignmentRestriction, "method #{m} doesn't exist"
            end
          end
        end
        self
      end
      
      # Returns all methods that can be used for attribute assignment (those that end with =),
      # depending on the type:
      #
      # :default :: Use the default methods allowed in the model class. 
      # :all :: Allow setting all setters, except those specifically restricted (such as ==).
      # Array :: Only allow setting of columns in the given array.
      def setter_methods(type)
        if type == :default
          if !@singleton_setter_added || model.allowed_columns
            return model.setter_methods
          end
        end

        if type.is_a?(Array)
          type.map{|x| "#{x}="}
        else
          meths = methods.collect(&:to_s).grep(SETTER_METHOD_REGEXP) - RESTRICTED_SETTER_METHODS
          meths -= Array(primary_key).map{|x| "#{x}="} if type != :all && primary_key && model.restrict_primary_key?
          meths
        end
      end

      # The server/shard that the model object's dataset uses, or :default if the
      # model object's dataset does not have an associated shard.
      def this_server
        if (s = @server)
          s
        elsif (t = @this)
          t.opts[:server] || :default
        else
          model.dataset.opts[:server] || :default
        end
      end
  
      # Typecast the value to the column's type if typecasting.  Calls the database's
      # typecast_value method, so database adapters can override/augment the handling
      # for database specific column types.
      def typecast_value(column, value)
        return value unless typecast_on_assignment && db_schema && (col_schema = db_schema[column])
        value = nil if '' == value and typecast_empty_string_to_nil and col_schema[:type] and ![:string, :blob].include?(col_schema[:type])
        raise(InvalidValue, "nil/NULL is not allowed for the #{column} column") if raise_on_typecast_failure && value.nil? && (col_schema[:allow_null] == false)
        begin
          model.db.typecast_value(col_schema[:type], value)
        rescue InvalidValue
          raise_on_typecast_failure ? raise : value
        end
      end
  
      # Set the columns, filtered by the only and except arrays.
      def update_restricted(hash, type)
        set_restricted(hash, type)
        save_changes
      end

      # Set the given dataset to use the current object's shard.
      def use_server(ds)
        @server ? ds.server(@server) : ds
      end
      
      # Whether to use a transaction for this action.  If the :transaction
      # option is present in the hash, use that, otherwise, fallback to the
      # object's default (if set), or class's default (if not).
      def use_transaction?(opts = OPTS)
        opts.fetch(:transaction, use_transactions)
      end
    end

    # Dataset methods are methods that the model class extends its dataset with in
    # the call to set_dataset.
    module DatasetMethods
      # The model class associated with this dataset
      #
      #   Artist.dataset.model # => Artist
      attr_accessor :model

      # Assume if a single integer is given that it is a lookup by primary
      # key, and call with_pk with the argument.
      #
      #   Artist.dataset[1] # SELECT * FROM artists WHERE (id = 1) LIMIT 1
      def [](*args)
        if args.length == 1 && (i = args.at(0)) && i.is_a?(Integer)
          with_pk(i)
        else
          super
        end
      end

      # Destroy each row in the dataset by instantiating it and then calling
      # destroy on the resulting model object.  This isn't as fast as deleting
      # the dataset, which does a single SQL call, but this runs any destroy
      # hooks on each object in the dataset.
      #
      #   Artist.dataset.destroy
      #   # DELETE FROM artists WHERE (id = 1)
      #   # DELETE FROM artists WHERE (id = 2)
      #   # ...
      def destroy
        pr = proc{all(&:destroy).length}
        model.use_transactions ? @db.transaction(:server=>opts[:server], &pr) : pr.call
      end

      # Allow Sequel::Model classes to be used as dataset arguments when graphing:
      #
      #   Artist.graph(Album, :artist_id=>id)
      #   # SELECT artists.id, artists.name, albums.id AS albums_id, albums.artist_id, albums.name AS albums_name
      #   # FROM artists LEFT OUTER JOIN albums ON (albums.artist_id = artists.id)
      def graph(table, *args, &block)
        if table.is_a?(Class) && table < Sequel::Model
          super(table.dataset, *args, &block)
        else
          super
        end
      end

      # Handle Sequel::Model instances when inserting, using the model instance's
      # values for the insert, unless the model instance can be used directly in
      # SQL.
      #
      #   Album.insert(Album.load(:name=>'A'))
      #   # INSERT INTO albums (name) VALUES ('A')
      def insert_sql(*values)
        if values.size == 1 && (v = values.at(0)).is_a?(Sequel::Model) && !v.respond_to?(:sql_literal_append)
          super(v.to_hash)
        else
          super
        end
      end

      # Allow Sequel::Model classes to be used as table name arguments in dataset
      # join methods:
      #
      #   Artist.join(Album, :artist_id=>id)
      #   # SELECT * FROM artists INNER JOIN albums ON (albums.artist_id = artists.id)
      def join_table(type, table, *args, &block)
        if table.is_a?(Class) && table < Sequel::Model
          if table.dataset.simple_select_all?
            super(type, table.table_name, *args, &block)
          else
            super(type, table.dataset, *args, &block)
          end
        else
          super
        end
      end

      # If there is no order already defined on this dataset, order it by
      # the primary key and call last.
      #
      #   Album.last
      #   # SELECT * FROM albums ORDER BY id DESC LIMIT 1
      def last(*a, &block)
        if opts[:order].nil? && model && (pk = model.primary_key)
          order(*pk).last(*a, &block)
        else
          super
        end
      end

      # If there is no order already defined on this dataset, order it by
      # the primary key and call paged_each.
      #
      #   Album.paged_each{|row| }
      #   # SELECT * FROM albums ORDER BY id LIMIT 1000 OFFSET 0
      #   # SELECT * FROM albums ORDER BY id LIMIT 1000 OFFSET 1000
      #   # SELECT * FROM albums ORDER BY id LIMIT 1000 OFFSET 2000
      #   # ...
      def paged_each(*a, &block)
        if opts[:order].nil? && model && (pk = model.primary_key)
          order(*pk).paged_each(*a, &block)
        else
          super
        end
      end

      # This allows you to call +to_hash+ without any arguments, which will
      # result in a hash with the primary key value being the key and the
      # model object being the value.
      #
      #   Artist.dataset.to_hash # SELECT * FROM artists
      #   # => {1=>#<Artist {:id=>1, ...}>,
      #   #     2=>#<Artist {:id=>2, ...}>,
      #   #     ...}
      def to_hash(key_column=nil, value_column=nil)
        if key_column
          super
        else
          raise(Sequel::Error, "No primary key for model") unless model && (pk = model.primary_key)
          super(pk, value_column) 
        end
      end

      # Given a primary key value, return the first record in the dataset with that primary key
      # value.  If no records matches, returns nil.
      #
      #   # Single primary key
      #   Artist.dataset.with_pk(1)
      #   # SELECT * FROM artists WHERE (artists.id = 1) LIMIT 1
      #
      #   # Composite primary key
      #   Artist.dataset.with_pk([1, 2])
      #   # SELECT * FROM artists WHERE ((artists.id1 = 1) AND (artists.id2 = 2)) LIMIT 1
      def with_pk(pk)
        first(model.qualified_primary_key_hash(pk))
      end

      # Same as with_pk, but raises NoMatchingRow instead of returning nil if no
      # row matches.
      def with_pk!(pk)
        with_pk(pk) || raise(NoMatchingRow.new(self))
      end
    end

    extend ClassMethods
    plugin self
    finder(:where, :arity=>1, :mod=>ClassMethods)
  end
end
