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
      # Which columns should be the only columns allowed in a call to a mass assignment method (e.g. set)
      # (default: not set, so all columns not otherwise restricted are allowed).
      attr_reader :allowed_columns
  
      # Array of modules that extend this model's dataset.  Stored
      # so that if the model's dataset is changed, it will be extended
      # with all of these modules.
      attr_reader :dataset_method_modules

      # Hash of dataset methods with method name keys and proc values that are
      # stored so when the dataset changes, methods defined with def_dataset_method
      # will be applied to the new dataset.
      attr_reader :dataset_methods

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
      # the given argument(s).  If no object is returned by the dataset, returns nil.
      # 
      #   Artist[1] # SELECT * FROM artists WHERE id = 1
      #   # => #<Artist {:id=>1, ...}>
      #
      #   Artist[:name=>'Bob'] # SELECT * FROM artists WHERE (name = 'Bob') LIMIT 1
      #   # => #<Artist {:name=>'Bob', ...}>
      def [](*args)
        args = args.first if (args.size == 1)
        args.is_a?(Hash) ? dataset[args] : primary_key_lookup(args)
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

      # Extend the dataset with an anonymous module, similar to adding
      # a plugin with the methods defined in DatasetMethods.  If a block
      # is given, it is module_evaled.
      #
      #   Artist.dataset_module do
      #     def foo
      #       :bar
      #     end
      #   end
      #   Artist.dataset.foo
      #   # => :bar
      #   Artist.foo
      #   # => :bar
      def dataset_module
        @dataset_module ||= Module.new
        @dataset_module.module_eval(&Proc.new) if block_given?
        dataset_extend(@dataset_module)
        @dataset_module
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
        @db = self == Model ? DATABASES.first : superclass.db
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
      def db=(db)
        @db = db
        set_dataset(db.dataset(@dataset.opts)) if @dataset
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
  
      # If a block is given, define a method on the dataset (if the model currently has an dataset)  with the given argument name using
      # the given block.  Also define a class method on the model that calls the
      # dataset method.  Stores the method name and block so that it can be reapplied if the model's
      # dataset changes.
      #
      # If a block is not given, just define a class method on the model for each argument
      # that calls the dataset method of the same argument name.
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
          meth = args.first
          @dataset_methods[meth] = block
          dataset.meta_def(meth, &block) if @dataset
        end
        args.each{|arg| instance_eval("def #{arg}(*args, &block); dataset.#{arg}(*args, &block) end", __FILE__, __LINE__) unless respond_to?(arg)}
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
        filter(*args, &block).first
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
    
      # Clear the setter_methods cache when a module is included, as it
      # may contain setter methods.
      def include(mod)
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
              n = subclass.name
              subclass.set_dataset(subclass.implicit_table_name) unless n.nil? || n.empty?
            elsif @dataset
              subclass.set_dataset(@dataset.clone, :inherited=>true)
            end
          rescue
            nil
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
  
      # Initializes a model instance as an existing record. This constructor is
      # used by Sequel to initialize model instances when fetching records.
      # +load+ requires that values be a hash where all keys are symbols. It
      # probably should not be used by external code.
      def load(values)
        new(values, true)
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
        @simple_pk = @primary_key = nil
      end
      
      # Loads a plugin for use with the model class, passing optional arguments
      # to the plugin.  If the plugin is a module, load it directly.  Otherwise,
      # require the plugin from either sequel/plugins/#{plugin} or
      # sequel_#{plugin}, and then attempt to load the module using a
      # the camelized plugin name under Sequel::Plugins.
      def plugin(plugin, *args, &blk)
        m = plugin.is_a?(Module) ? plugin : plugin_module(plugin)
        unless @plugins.include?(m)
          @plugins << m
          m.apply(self, *args, &blk) if m.respond_to?(:apply)
          include(m::InstanceMethods) if plugin_module_defined?(m, :InstanceMethods)
          extend(m::ClassMethods)if plugin_module_defined?(m, :ClassMethods)
          dataset_extend(m::DatasetMethods) if plugin_module_defined?(m, :DatasetMethods)
        end
        m.configure(self, *args, &blk) if m.respond_to?(:configure)
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

      # Return a hash where the keys are qualified column references.  Uses the given
      # qualifier if provided, or the table_name otherwise. This is useful if you
      # plan to join other tables to this table and you want the column references
      # to be qualified.
      #
      #   Artist.filter(Artist.qualified_primary_key_hash(1))
      #   # SELECT * FROM artists WHERE (artists.id = 1)
      def qualified_primary_key_hash(value, qualifier=table_name)
        h = primary_key_hash(value)
        h.to_a.each{|k,v| h[SQL::QualifiedIdentifier.new(qualifier, k)] = h.delete(k)}
        h
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
      # model objects, extends the dataset with the dataset_method_modules,
      # and defines methods on the dataset using the dataset_methods.
      # It also attempts to determine the database schema for the model,
      # based on the given dataset.
      #
      #   Artist.set_dataset(:tbl_artists)
      #   Artist.set_dataset(DB[:artists])
      def set_dataset(ds, opts={})
        inherited = opts[:inherited]
        @dataset = case ds
        when Symbol, SQL::Identifier, SQL::QualifiedIdentifier, SQL::AliasedExpression, LiteralString
          @simple_table = db.literal(ds)
          db.from(ds)
        when Dataset
          @simple_table = nil
          @db = ds.db
          ds
        else
          raise(Error, "Model.set_dataset takes one of the following classes as an argument: Symbol, LiteralString, SQL::Identifier, SQL::QualifiedIdentifier, SQL::AliasedExpression, Dataset")
        end
        @dataset.row_proc = Proc.new{|r| load(r)}
        @require_modification = Sequel::Model.require_modification.nil? ? @dataset.provides_accurate_rows_matched? : Sequel::Model.require_modification
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
      def set_primary_key(*key)
        clear_setter_methods_cache
        key = key.flatten
        @simple_pk = key.length == 1 ? db.literal(key.first) : nil 
        @primary_key = (key.length == 1) ? key[0] : key
      end
  
      # Set the columns to restrict when using mass assignment (e.g. +set+).  Using this means that
      # attempts to call setter methods for the columns listed here will cause an
      # exception or be silently skipped (based on the +strict_param_setting+ setting.
      # If you have any virtual setter methods (methods that end in =) that you
      # want not to be used during mass assignment, they need to be listed here as well (without the =).
      #
      # It's generally a bad idea to rely on a blacklist approach for security.  Using a whitelist
      # approach such as set_allowed_columns or the instance level set_only or set_fields methods
      # is usually a better choice.  So use of this method is generally a bad idea.
      #
      #   Artist.set_restricted_column(:records_sold)
      #   Artist.set(:name=>'Bob', :hometown=>'Sactown') # No Error
      #   Artist.set(:name=>'Bob', :records_sold=>30000) # Error
      def set_restricted_columns(*cols)
        clear_setter_methods_cache
        @restricted_columns = cols
      end

      # Cache of setter methods to allow by default, in order to speed up new/set/update instance methods.
      def setter_methods
        @setter_methods ||= if allowed_columns
          allowed_columns.map{|x| "#{x}="}
        else
          meths = instance_methods.collect{|x| x.to_s}.grep(SETTER_METHOD_REGEXP) - RESTRICTED_SETTER_METHODS
          meths -= Array(primary_key).map{|x| "#{x}="} if primary_key && restrict_primary_key?
          meths -= restricted_columns.map{|x| "#{x}="} if restricted_columns
          meths
        end
      end
  
      # Shortcut for +def_dataset_method+ that is restricted to modifying the
      # dataset's filter. Sometimes thought of as a scope, and like most dataset methods,
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
      # dataset methods that accept arguments, you have to use def_dataset_method.
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

      # Add the module to the class's dataset_method_modules.  Extend the dataset with the
      # module if the model has a dataset.  Add dataset methods to the class for all
      # public dataset methods.
      def dataset_extend(mod)
        dataset.extend(mod) if @dataset
        dataset_method_modules << mod
        meths = mod.public_instance_methods.reject{|x| NORMAL_METHOD_NAME_REGEXP !~ x.to_s}
        def_dataset_method(*meths) unless meths.empty?
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
        if single_table && (schema_array = (db.schema(dataset.first_source_table, :reload=>reload) rescue nil))
          schema_array.each{|k,v| schema_hash[k] = v}
          if ds_opts.include?(:select)
            # We don't remove the columns from the schema_hash,
            # as it's possible they will be used for typecasting
            # even if they are not selected.
            cols = get_columns.call
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
            Sequel.tsk_require "sequel/plugins/#{plugin}"
          rescue LoadError => e
            begin
              Sequel.tsk_require "sequel_#{plugin}"
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
          plugin.const_defined?(submod)
        end
      end
  
      # Find the row in the dataset that matches the primary key.  Uses
      # a static SQL optimization if the table and primary key are simple.
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
      #
      #   Artist.fetch("SELECT * FROM artists WHERE name LIKE 'A%'")
      #   Artist.fetch("SELECT * FROM artists WHERE id = ?", 1)
      alias fetch with_sql
    end

    # Sequel::Model instance methods that implement basic model functionality.
    #
    # * All of the methods in +HOOKS+ and +AROUND_HOOKS+ create instance methods that are called
    #   by Sequel when the appropriate action occurs.  For example, when destroying
    #   a model object, Sequel will call +around_destory+, which will call +before_destroy+, do
    #   the destroy, and then call +after_destroy+.
    # * The following instance_methods all call the class method of the same
    #   name: columns, db, primary_key, db_schema.
    # * All of the methods in +BOOLEAN_SETTINGS+ create attr_writers allowing you
    #   to set values for the attribute.  It also creates instnace getters returning
    #   the value of the setting.  If the value has not yet been set, it
    #   gets the default value from the class by calling the class method of the same name.
    module InstanceMethods
      HOOKS.each{|h| class_eval("def #{h}; end", __FILE__, __LINE__)}
      AROUND_HOOKS.each{|h| class_eval("def #{h}; yield end", __FILE__, __LINE__)}

      # Define instance method(s) that calls class method(s) of the
      # same name, caching the result in an instance variable.  Define
      # standard attr_writer method for modifying that instance variable.
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
      class_attr_overridable *BOOLEAN_SETTINGS

      # The hash of attribute values.  Keys are symbols with the names of the
      # underlying database columns.
      #
      #   Artist.new(:name=>'Bob').values # => {:name=>'Bob'}
      #   Artist[1].values # => {:id=>1, :name=>'Jim', ...}
      attr_reader :values

      # Creates new instance and passes the given values to set.
      # If a block is given, yield the instance to the block unless
      # from_db is true.
      # This method runs the after_initialize hook after
      # it has optionally yielded itself to the block.
      #
      # Arguments:
      # values :: should be a hash to pass to set. 
      # from_db :: should only be set by <tt>Model.load</tt>, forget it exists.
      #
      #   Artist.new(:name=>'Bob')
      #
      #   Artist.new do |a|
      #     a.name = 'Bob'
      #   end
      def initialize(values = {}, from_db = false)
        if from_db
          @new = false
          set_values(values)
        else
          @values = {}
          @new = true
          @modified = true
          initialize_set(values)
          changed_columns.clear 
          yield self if block_given?
        end
        after_initialize
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
        if new? || !@values.include?(column) || v != (c = @values[column]) || v.class != c.class
          changed_columns << column unless changed_columns.include?(column)
          @values[column] = v
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
        _delete
        self
      end
      
      # Like delete but runs hooks before and after delete.
      # If before_destroy returns false, returns false without
      # deleting the object the the database. Otherwise, deletes
      # the item from the database and returns self.  Uses a transaction
      # if use_transactions is true or if the :transaction option is given and
      # true.
      #
      #   Artist[1].destroy # BEGIN; DELETE FROM artists WHERE (id = 1); COMMIT;
      #   # => #<Artist {:id=>1, ...}>
      def destroy(opts = {})
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
        @errors ||= Errors.new
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
        new? ? false : !this.get(1).nil?
      end
      
      # Ignore the model's setter method cache when this instances extends a module, as the
      # module may contain setter methods.
      def extend(mod)
        @singleton_setter_added = true
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
          [model, !pk.all? ? @values.sort_by{|k,v| k.to_s} : pk].hash
        when Symbol
          [model, pk.nil? ? @values.sort_by{|k,v| k.to_s} : pk].hash
        else
          [model, @values.sort_by{|k,v| k.to_s}].hash
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
      #     a.update(...)
      #   end
      def lock!
        new? ? self : _refresh(this.for_update)
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
      def modified!
        @modified = true
      end

      # Whether this object has been modified since last saved, used by
      # save_changes to determine whether changes should be saved.  New
      # values are always considered modified.
      #
      #   a = Artist[1]
      #   a.modified? # => false
      #   a.set(:name=>'Jim')
      #   a.modified # => true
      def modified?
        @modified || !changed_columns.empty?
      end
  
      # Returns true if the current instance represents a new record.
      #
      #   Artist.new.new? # => true
      #   Artist[1].new? # => false
      def new?
        @new
      end
      
      # Returns the primary key value identifying the model instance.
      # Raises an +Error+ if this model does not have a primary key.
      # If the model has a composite primary key, returns an array of values.
      #
      #   Artist[1].pk # => 1
      #   Artist[[1, 2]].pk # => [1, 2]
      def pk
        raise(Error, "No primary key is associated with this model") unless key = primary_key
        key.is_a?(Array) ? key.map{|k| @values[k]} : @values[key]
      end
      
      # Returns a hash identifying mapping the receivers primary key column(s) to their values.
      # 
      #   Artist[1].pk_hash # => {:id=>1}
      #   Artist[[1, 2]].pk_hash # => {:id1=>1, :id2=>2}
      def pk_hash
        model.primary_key_hash(pk)
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
        _refresh(this)
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
      # :transaction :: set to true or false to override the current
      #                 +use_transactions+ setting
      # :validate :: set to false to skip validation
      # :raise_on_failure :: set to true or false to override the current
      #                      +raise_on_save_failure+ setting
      def save(*columns)
        opts = columns.last.is_a?(Hash) ? columns.pop : {}
        if opts[:validate] != false
          unless checked_save_failure(opts){_valid?(true, opts)}
            raise(ValidationFailed.new(errors)) if raise_on_failure?(opts)
            return
          end
        end
        checked_save_failure(opts){checked_transaction(opts){_save(columns, opts)}}
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
      def save_changes(opts={})
        save(opts.merge(:changed=>true)) || false if modified? 
      end
  
      # Updates the instance with the supplied values with support for virtual
      # attributes, raising an exception if a value is used that doesn't have
      # a setter method (or ignoring it if <tt>strict_param_setting = false</tt>).
      # Does not save the record.
      #
      #   artist.set(:name=>'Jim')
      #   artist.name # => 'Jim'
      def set(hash)
        set_restricted(hash, nil, nil)
      end
  
      # Set all values using the entries in the hash, ignoring any setting of
      # allowed_columns or restricted columns in the model.
      #
      #   Artist.set_restricted_columns(:name)
      #   artist.set_all(:name=>'Jim')
      #   artist.name # => 'Jim'
      def set_all(hash)
        set_restricted(hash, false, false)
      end
  
      # Set all values using the entries in the hash, except for the keys
      # given in except.  You should probably use +set_fields+ or +set_only+
      # instead of this method, as blacklist approaches to security are a bad idea.
      #
      #   artist.set_except({:name=>'Jim'}, :hometown)
      #   artist.name # => 'Jim'
      def set_except(hash, *except)
        set_restricted(hash, false, except.flatten)
      end
  
      # For each of the fields in the given array +fields+, call the setter
      # method with the value of that +hash+ entry for the field. Returns self.
      #
      #   artist.set_fields({:name=>'Jim'}, [:name])
      #   artist.name # => 'Jim'
      #
      #   artist.set_fields({:hometown=>'LA'}, [:name])
      #   artist.name # => nil
      #   artist.hometown # => 'Sac'
      def set_fields(hash, fields)
        fields.each{|f| send("#{f}=", hash[f])}
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
        set_restricted(hash, only.flatten, false)
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
        @this ||= model.dataset.filter(pk_hash).limit(1).naked
      end
      
      # Runs #set with the passed hash and then runs save_changes.
      #
      #   artist.update(:name=>'Jim') # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      def update(hash)
        update_restricted(hash, nil, nil)
      end
  
      # Update all values using the entries in the hash, ignoring any setting of
      # +allowed_columns+ or +restricted_columns+ in the model.
      #
      #   Artist.set_restricted_columns(:name)
      #   artist.update_all(:name=>'Jim') # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      def update_all(hash)
        update_restricted(hash, false, false)
      end
  
      # Update all values using the entries in the hash, except for the keys
      # given in except.  You should probably use +update_fields+ or +update_only+
      # instead of this method, as blacklist approaches to security are a bad idea.
      #
      #   artist.update_except({:name=>'Jim'}, :hometown) # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      def update_except(hash, *except)
        update_restricted(hash, false, except.flatten)
      end
  
      # Update the instances values by calling +set_fields+ with the +hash+
      # and +fields+, then save any changes to the record.  Returns self.
      #
      #   artist.update_fields({:name=>'Jim'}, [:name])
      #   # UPDATE artists SET name = 'Jim' WHERE (id = 1)
      #
      #   artist.update_fields({:hometown=>'LA'}, [:name])
      #   # UPDATE artists SET name = NULL WHERE (id = 1)
      def update_fields(hash, fields)
        set_fields(hash, fields)
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
        update_restricted(hash, only.flatten, false)
      end
      
      # Validates the object.  If the object is invalid, errors should be added
      # to the errors attribute.  By default, does nothing, as all models
      # are valid by default.  See the {"Model Validations" guide}[link:files/doc/validations_rdoc.html].
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
      def valid?(opts = {})
        _valid?(false, opts)
      end

      private
      
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
        _delete_dataset.delete
      end

      # Internal destroy method, separted from destroy to
      # allow running inside a transaction
      def _destroy(opts)
        called = false
        around_destroy do
          called = true
          raise_hook_failure(:destroy) if before_destroy == false
          _destroy_delete
          after_destroy
          true
        end
        raise_hook_failure(:destroy) unless called
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
        if !ds.opts[:select] and ds.supports_insert_select? and h = _insert_select_raw(ds)
          @values = h
          nil
        else
          iid = _insert_raw(ds)
          # if we have a regular primary key and it's not set in @values,
          # we assume it's the last inserted id
          if (pk = autoincrementing_primary_key) && pk.is_a?(Symbol) && !@values[pk]
            @values[pk] = iid
          end
          pk
        end
      end

      # The dataset to use when inserting a new object.   The same as the model's
      # dataset by default.
      def _insert_dataset
        model.dataset
      end
  
      # Insert into the given dataset and return the primary key created (if any).
      def _insert_raw(ds)
        ds.insert(@values)
      end

      # Insert into the given dataset and return the hash of column values.
      def _insert_select_raw(ds)
        ds.insert_select(@values)
      end
      
      # Refresh using a particular dataset, used inside save to make sure the same server
      # is used for reading newly inserted values from the database
      def _refresh(dataset)
        set_values(_refresh_get(dataset) || raise(Error, "Record not found"))
        changed_columns.clear
        self
      end

      # Get the row of column data from the database.
      def _refresh_get(dataset)
        dataset.first
      end
      
      # Internal version of save, split from save to allow running inside
      # it's own transaction.
      def _save(columns, opts)
        was_new = false
        pk = nil
        called_save = false
        called_cu = false
        around_save do
          called_save = true
          raise_hook_failure(:save) if before_save == false
          if new?
            was_new = true
            around_create do
              called_cu = true
              raise_hook_failure(:create) if before_create == false
              pk = _insert
              @this = nil
              @new = false
              @was_new = true
              after_create
              true
            end
            raise_hook_failure(:create) unless called_cu
          else
            around_update do
              called_cu = true
              raise_hook_failure(:update) if before_update == false
              if columns.empty?
                @columns_updated = if opts[:changed]
                  @values.reject{|k,v| !changed_columns.include?(k)}
                else
                  _save_update_all_columns_hash
                end
                changed_columns.clear
              else # update only the specified columns
                @columns_updated = @values.reject{|k, v| !columns.include?(k)}
                changed_columns.reject!{|c| columns.include?(c)}
              end
              _update_columns(@columns_updated)
              @this = nil
              after_update
              true
            end
            raise_hook_failure(:update) unless called_cu
          end
          after_save
          true
        end
        raise_hook_failure(:save) unless called_save
        if was_new
          @was_new = nil
          pk ? _save_refresh : changed_columns.clear
        else
          @columns_updated = nil
        end
        @modified = false
        self
      end

      # Refresh the object after saving it, used to get
      # default values of all columns.  Separated from _save so it
      # can be overridden to avoid the refresh.
      def _save_refresh
        _refresh(this.opts[:server] ? this : this.server(:default))
      end

      # Return a hash of values used when saving all columns of an
      # existing object (i.e. not passing specific columns to save
      # or using update/save_changes).  Defaults to all of the
      # object's values except unmodified primary key columns, as some
      # databases don't like you setting primary key values even
      # to their existing values.
      def _save_update_all_columns_hash
        v = @values.dup
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

      # Internal validation method.  If +raise_errors+ is +true+, hook
      # failures will be raised as HookFailure exceptions.  If it is
      # +false+, +false+ will be returned instead.
      def _valid?(raise_errors, opts)
        errors.clear
        called = false
        error = false
        around_validation do
          called = true
          if before_validation == false
            if raise_errors
              raise_hook_failure(:validation)
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
            raise_hook_failure(:validation)
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
      def checked_transaction(opts={})
        use_transaction?(opts) ? db.transaction(opts){yield} : yield
      end

      # Set the columns with the given hash.  By default, the same as +set+, but
      # exists so it can be overridden.  This is called only for new records, before
      # changed_columns is cleared.
      def initialize_set(h)
        set(h)
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
      def raise_hook_failure(type)
        raise HookFailed, "one of the before_#{type} hooks returned false"
      end
  
      # Set the columns, filtered by the only and except arrays.
      def set_restricted(hash, only, except)
        meths = if only.nil? && except.nil? && !@singleton_setter_added
          model.setter_methods
        else
          setter_methods(only, except)
        end
        strict = strict_param_setting
        hash.each do |k,v|
          m = "#{k}="
          if meths.include?(m)
            send(m, v)
          elsif strict
            # Avoid using respond_to? or creating symbols from user input
            if public_methods.map{|s| s.to_s}.include?(m)
              if Array(model.primary_key).map{|s| s.to_s}.member?(k.to_s) && model.restrict_primary_key?
                raise Error, "#{k} is a restricted primary key"
              else
                raise Error, "#{k} is a restricted column"
              end
            else
              raise Error, "method #{m} doesn't exist"
            end
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
        pr = proc{all{|r| r.destroy}.length}
        model.use_transactions ? @db.transaction(&pr) : pr.call
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
          raise(Sequel::Error, "No primary key for model") unless model and pk = model.primary_key
          super(pk, value_column) 
        end
      end

      # Given a primary key value, return the first record in the dataset with that primary key
      # value.
      #
      #   # Single primary key
      #   Artist.dataset.with_pk(1) # SELECT * FROM artists WHERE (id = 1) LIMIT 1
      #
      #   # Composite primary key
      #   Artist.dataset.with_pk([1, 2]) # SELECT * FROM artists
      #                                  # WHERE ((id1 = 1) AND (id2 = 2)) LIMIT 1
      def with_pk(pk)
        case primary_key = model.primary_key
        when Array
          raise(Error, "single primary key given (#{pk.inspect}) when a composite primary key is expected (#{primary_key.inspect})") unless pk.is_a?(Array)
          raise(Error, "composite primary key given (#{pk.inspect}) does not match composite primary key length (#{primary_key.inspect})") if pk.length != primary_key.length
          first(primary_key.zip(pk))
        else
          raise(Error, "composite primary key given (#{pk.inspect}) when a single primary key is expected (#{primary_key.inspect})") if pk.is_a?(Array)
          first(primary_key=>pk)
        end
      end
    end

    extend ClassMethods
    plugin self
  end
end
